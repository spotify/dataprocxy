# Copyright (c) 2015 Spotify AB
import argparse
import platform
import random
import signal
import socket
import subprocess
import tempfile
import time
import os

from pprint import pprint

from googleapiclient import discovery
from oauth2client.client import ApplicationDefaultCredentialsError
from oauth2client.client import GoogleCredentials


class DataProcxy():
    def __init__(self):
        self.dataproc_service = None
        self.gce_service = None
        self.proxy = None
        self.browser = None
        signal.signal(signal.SIGINT, lambda s, f: self.shutdown())

    def run(self):
        retries = 5
        if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') is not None:
            print "WARNING: GOOGLE_APPLICATION_CREDENTIALS environment variable is set, using credentials from %s to access gcloud" % os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        for retry in range (0,retries):
            try:
                credentials = GoogleCredentials.get_application_default()
            except ApplicationDefaultCredentialsError as msg:
                out = subprocess.check_output("gcloud -q auth application-default login", shell=True, stderr=subprocess.STDOUT)
                success = False
                for line in out:
                    if "You are now logged in as" in out:
                        success = True
                if not success:
                    print "unable to authenticate:"
                    exit(1)
                else:
                    break

        self.dataproc_service = discovery.build('dataproc', 'v1', credentials=credentials)
        self.gce_service = discovery.build('compute', 'v1', credentials=credentials)

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
        request = self.gce_service.instances().get(
            project=self.project_id, zone=zone, instance=master_node)
        response = request.execute()
        return response['status'].encode('utf8')

    def query_cluster(self):
        request = self.dataproc_service.projects().regions().clusters().get(
            projectId=self.project_id,
            region='global',
            clusterName=self.cluster_name)
        response = request.execute()
        master_node = response['config']['masterConfig']['instanceNames'][0].encode('utf8')
        zone = response['config']['gceClusterConfig']['zoneUri'].encode('utf8').split('/')[-1]
        return master_node, zone

    def parse_args(self):
        parser = argparse.ArgumentParser(
            description='opens a browser window to RM, NN and JHS of a dataproc cluster using an ssh session to proxy')
        parser.add_argument('--job', help='jobid to discover cluster', nargs="?")
        parser.add_argument('--cluster', help='cluster id of dataproc cluster to connect to',
                            nargs="?")
        parser.add_argument('--project', help='cloud project of the dataproc cluster', nargs="?",
                            required=True)
        parser.add_argument('uris', nargs='*', help='URIs to be opened!')
        args = parser.parse_args()
        self.project_id = args.project
        self.uris = args.uris
        if args.job is None and args.cluster is None:
            print 'Either job or cluster need to be specified'
            exit(1)
        if args.cluster is None:
            self.cluster_name = self.get_cluster_from_job(job_id=args.job)
        else:
            self.cluster_name = args.cluster

    def get_cluster_from_job(self, job_id):
        request = self.dataproc_service.projects().regions().jobs().get(projectId=self.project_id,
                                                                        region='global',
                                                                        jobId=job_id)
        response = request.execute()
        cluster_name = response['placement']['clusterName'].encode('utf8')
        return cluster_name

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
                    print "unable to connect to master instance"
                    exit(1)
            s.shutdown(socket.SHUT_RDWR)
            s.close()
            break

    def start(self):
        ssh_command = 'gcloud -q compute ssh %(masterNode)s --ssh-flag="-x" --ssh-flag="-o ConnectTimeout=5" --ssh-flag="-D localhost:%(port)i" --ssh-flag="-n" --ssh-flag="-N" --project %(projectId)s --zone %(zone)s' % {
            "masterNode": self.master_node, "port": self.proxy_port, "projectId": self.project_id,
            "zone": self.zone}
        print "executing %s" % ssh_command
        self.ssh_process = subprocess.Popen(ssh_command, shell=True)

    def stop(self):
        if self.ssh_process.returncode is not None:
            try:
                self.ssh_process.terminate()
                for i in range(0,10):
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
        chrome_args = ('--proxy-server="socks5://localhost:%(port)i" --host-resolver-rules="MAP * 0.0.0.0 , EXCLUDE localhost" --user-data-dir=%(tempdir)s --no-default-browser-check --no-first-run --enable-kiosk-mode --new-window "http://%(masterNode)s:8088" "http://%(masterNode)s:50070" "http://%(masterNode)s:19888/jobhistory/" ' + more_uris) % {
            "masterNode": self.masterNode, "port": self.proxyPort, "projectId": self.projectId,
            "zone": self.zone,
            "tempdir": self.tempdir}

        print "proxy connected via ssh, starting chrome"
        if platform.system() == "Darwin":
            chrome_path = subprocess.check_output(["mdfind","kMDItemCFBundleIdentifier","=","com.google.Chrome"]).split("\n")[0] + '/Contents/MacOS/Google Chrome'
            self.browser_process = subprocess.Popen(
                '"' + chrome_path + '" ' + chrome_args, shell=True)
        else:
            self.browser_process = subprocess.Popen('google-chrome ' + chrome_args, shell=True)

    def wait(self):
        return self.browser_process.wait()

    def stop(self):
        if self.browser_process.returncode is not None:
            try:
                self.browser_process.terminate()
                for i in range(0,10):
                    if self.browser_process.returncode is None:
                        break
                    time.sleep(0.1)
                if self.browser_process.returncode is not None:
                    self.browser_process.kill()
            except OSError as error:
                if error.errno != 3:
                    raise error

