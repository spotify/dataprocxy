# Copyright (c) 2015 Spotify AB
import argparse
import os
import platform
import random
import signal
import socket
import subprocess
import tempfile
import time
import httplib2
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from oauth2client.client import ApplicationDefaultCredentialsError
from oauth2client.client import GoogleCredentials


class DataProcxy():
    def __init__(self):
        self.dataproc_service = None
        self.gce_service = None
        self.proxy = None
        self.browser = None
        self.region = None
        self.job_id = None
        self.cluster_name = None
        signal.signal(signal.SIGINT, lambda s, f: self.shutdown())

    def run(self):
        retries = 5
        if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') is not None:
            print "WARNING: GOOGLE_APPLICATION_CREDENTIALS environment variable is set, using credentials from %s to access gcloud" % \
                  os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        for retry in range(0, retries):
            try:
                credentials = GoogleCredentials.get_application_default()
            except ApplicationDefaultCredentialsError as msg:
                out = subprocess.check_output("gcloud -q auth application-default login",
                                              shell=True,
                                              stderr=subprocess.STDOUT)
                success = False
                for line in out:
                    if "You are now logged in as" in out:
                        success = True
                if not success:
                    print "unable to authenticate:"
                    exit(1)
                else:
                    break

        try:
            self.dataproc_service = discovery.build('dataproc', 'v1', credentials=credentials)
            self.gce_service = discovery.build('compute', 'v1', credentials=credentials)
        except httplib2.ServerNotFoundError as error:
            print 'There was a Google API error when trying to connect to a server:\n %s' % (str(error))
            exit(1)

        self.parse_args()

        master_node, zone = self.query_cluster()

        status = self.get_master_status(master_node, zone)

        if status != 'RUNNING':
            print 'Master node not running, unable to connect'
            exit(1)

        port = random.randint(5000, 10000)

        self.proxy = SshProxy(project_id=self.project_id, zone=zone, master_node=master_node,
                              proxy_port=port)
        self.proxy.start()
        self.proxy.wait()  # blocks until ssh process becomes available
        self.browser = Browser(project_id=self.project_id, zone=zone, master_node=master_node,
                               proxy_port=port, uris=self.uris)
        self.browser.start()
        self.browser.wait()

        self.shutdown()

    def get_master_status(self, master_node, zone):
        try:
            request = self.gce_service.instances().get(project=self.project_id, zone=zone, instance=master_node)
            response = request.execute()
            return response['status'].encode('utf8')
        except HttpError as error:
            print ('There was a Compute Engine API error when trying to query a Master node:\n %s, %s' %
                   (error.resp.status, error._get_reason()))
            exit(1)

    def query_cluster(self):
        try:
            request = self.dataproc_service.projects().regions().clusters().get(projectId=self.project_id,
                                                                                region=self.region,
                                                                                clusterName=self.cluster_name)
            response = request.execute()
            master_node = response['config']['masterConfig']['instanceNames'][0].encode('utf8')
            zone = response['config']['gceClusterConfig']['zoneUri'].encode('utf8').split('/')[-1]
            return master_node, zone
        except HttpError as error:
            self.handle_dataproc_http_error(error)

    def get_cluster_from_job(self, job_id):
        try:
            request = self.dataproc_service.projects().regions().jobs().get(projectId=self.project_id,
                                                                            region=self.region,
                                                                            jobId=job_id)
            response = request.execute()
            cluster_name = response['placement']['clusterName'].encode('utf8')
            return cluster_name
        except HttpError as error:
            self.handle_dataproc_http_error(error)

    def handle_dataproc_http_error(self, error):
        print ('There was a Cloud Dataproc API call error:\n %s, %s' % (error.resp.status, error._get_reason()))
        if error.resp.status == 404:
            print 'Cluster you want to connect to does not exist, please make sure your parameters are correct and there is no typo:\n' \
                  ' * project      : %(project)s\n' \
                  ' * region       : %(region)s\n' \
                  ' * cluster name : %(cluster)s\n' \
                  ' * job ID       : %(job_id)s' % {
                      "project": self.project_id,
                      "region": self.region,
                      "cluster": self.cluster_name if self.cluster_name else "[NOT SPECIFIED]",
                      "job_id": self.job_id if self.job_id else "[NOT SPECIFIED]"}
        exit(1)

    def parse_args(self):
        parser = argparse.ArgumentParser(
            description='Opens a browser window with RM, NN and JHS of a Dataproc cluster using an ssh session to proxy')
        parser.add_argument('--job', help='Job ID to discover Dataproc cluster', nargs="?")
        parser.add_argument('--cluster', help='Name of Dataproc cluster to connect to', nargs="?")
        parser.add_argument('--project', help='Google Cloud project of Dataproc cluster', nargs="?", required=True)
        parser.add_argument('--region',
                            help='Dataproc region to query (default: %(default)s)',
                            nargs="?",
                            default='global')
        parser.add_argument('uris', nargs='*', help='URIs to be opened')
        args = parser.parse_args()
        self.project_id = args.project
        self.uris = args.uris
        self.region = args.region
        self.job_id = args.job
        if self.job_id is None and args.cluster is None:
            print 'Either Job ID or cluster Name need to be specified'
            exit(1)
        if args.cluster is None:
            self.cluster_name = self.get_cluster_from_job(job_id=self.job_id)
        else:
            self.cluster_name = args.cluster

    def shutdown(self):
        if self.proxy is not None:
            self.proxy.stop()
        if self.browser is not None:
            self.browser.stop()


class SshProxy():
    def __init__(self, project_id, zone, master_node, proxy_port):
        self.proxy_port = proxy_port
        self.master_node = master_node
        self.zone = zone
        self.project_id = project_id

    def running(self):
        return self.ssh_process.returncode is not None

    def wait(self, timeout=10):
        if self.ssh_process.returncode is not None:
            return self.ssh_process.returncode
        while 1:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.connect(('127.0.0.1', self.proxy_port))
            except socket.error as err:
                if self.ssh_process.returncode is None:
                    time.sleep(0.1)
                    continue
                else:
                    print "Unable to connect to master instance"
                    exit(1)
            s.shutdown(socket.SHUT_RDWR)
            s.close()
            break

    def start(self):
        ssh_command = 'gcloud -q compute ssh %(masterNode)s --project %(projectId)s --zone %(zone)s -- -x -o ConnectTimeout=5 -D localhost:%(port)i -n -N' % {
            "masterNode": self.master_node,
            "port": self.proxy_port,
            "projectId": self.project_id,
            "zone": self.zone}
        print "Executing: %s" % ssh_command
        self.ssh_process = subprocess.Popen(ssh_command, shell=True)

    def stop(self):
        if self.ssh_process.returncode is not None:
            try:
                self.ssh_process.terminate()
                for i in range(0, 10):
                    if self.ssh_process.returncode is None:
                        break
                    time.sleep(0.1)
                if self.ssh_process.returncode is not None:
                    self.ssh_process.kill()
            except OSError as error:
                if error.errno != 3:
                    raise error


class Browser():
    def __init__(self, project_id, zone, master_node, proxy_port, uris):
        self.proxyPort = proxy_port
        self.masterNode = master_node
        self.zone = zone
        self.projectId = project_id
        self.uris = uris
        self.browser_process = None

    def start(self):
        self.tempdir = tempfile.mkdtemp()
        more_uris = " ".join(['"' + uri + '"' for uri in self.uris])
        chrome_args = '--proxy-server="socks5://localhost:%(port)i" --host-resolver-rules="MAP * 0.0.0.0 , EXCLUDE localhost" --user-data-dir=%(tempdir)s --no-default-browser-check --no-first-run --enable-kiosk-mode --new-window "http://%(masterNode)s:8088" "http://%(masterNode)s:9870" "http://%(masterNode)s:19888/jobhistory/" %(moreURIs)s' % {
            "masterNode": self.masterNode,
            "port": self.proxyPort,
            "projectId": self.projectId,
            "zone": self.zone,
            "tempdir": self.tempdir,
            "moreURIs": more_uris}

        print "Proxy connected via ssh, starting chrome"
        if platform.system() == "Darwin":
            chrome_path = subprocess.check_output(["mdfind", "kMDItemCFBundleIdentifier", "=", "com.google.Chrome"]).split("\n")[0] + '/Contents/MacOS/Google Chrome'
            self.browser_process = subprocess.Popen('"' + chrome_path + '" ' + chrome_args, shell=True)
        else:
            self.browser_process = subprocess.Popen('google-chrome ' + chrome_args, shell=True)

    def wait(self):
        return self.browser_process.wait()

    def stop(self):
        if self.browser_process.returncode is not None:
            try:
                self.browser_process.terminate()
                for i in range(0, 10):
                    if self.browser_process.returncode is None:
                        break
                    time.sleep(0.1)
                if self.browser_process.returncode is not None:
                    self.browser_process.kill()
            except OSError as error:
                if error.errno != 3:
                    raise error
