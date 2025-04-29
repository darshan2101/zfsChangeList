from pprint import pprint
import os
import requests
import subprocess
import json
import platform
import time
import sys
import time    
import argparse
from configparser import ConfigParser
import plistlib
from filelock import Timeout, FileLock

ZFS_HOSTNAME = "ZFS_HOSTNAME"
ZFS_USERNAME = "ZFS_USERNAME"
ZFS_KEYFILE  = "ZFS_KEYFILE"

def get_zfs_config_dict():

    is_linux=0
    if platform.system() == "Linux":
        DNA_CLIENT_SERVICES = '/etc/StorageDNA/DNAClientServices.conf'
        is_linux=1
    elif platform.system() == "Darwin":
        DNA_CLIENT_SERVICES = '/Library/Preferences/com.storagedna.DNAClientServices.plist'

    config_dict = {}

    if is_linux == 1:
        config_parser = ConfigParser()
        config_parser.read(DNA_CLIENT_SERVICES)
        if config_parser.has_section('General'):
            section_info = config_parser['General']
            if config_parser.has_option('General', ZFS_HOSTNAME):
                config_dict[ZFS_HOSTNAME] = section_info[ZFS_HOSTNAME]
            if config_parser.has_option('General', ZFS_USERNAME):
                config_dict[ZFS_USERNAME] = section_info[ZFS_USERNAME]
            if config_parser.has_option('General',ZFS_KEYFILE):
                config_dict[ZFS_KEYFILE] = section_info[ZFS_KEYFILE]
 
    else:
        with open(DNA_CLIENT_SERVICES, 'rb') as fp:
            my_plist = plistlib.load(fp)
            config_dict[ZFS_HOSTNAME] = my_plist[ZFS_HOSTNAME]
            config_dict[ZFS_USERNAME] = my_plist[ZFS_USERNAME]
            config_dict[ZFS_KEYFILE] = my_plist[ZFS_KEYFILE]

    return config_dict

def run_zfs_process(path, snapname, output):

    args = ["zfs","snapshot", "-r", f"{path}@{snapname}"]

    p = subprocess.run(args, stdout=subprocess.PIPE)
    output["exitcode"]  = p.returncode
    if p.returncode != 0:
         output["result"] = p.stderr.decode()
         return False
    else:
         output["result"] = p.stdout.decode()
         return True

def run_zfs_ssh_process(config, path, snapname, output):

    args = ["ssh"]
    args.append("-i")
    args.append(config[ZFS_KEYFILE])
    username = f"{config[ZFS_USERNAME]}@{config_dict[ZFS_HOSTNAME]}"
    args.append(username)

    ssh_cmd = f"zfs snapshot -r {path}@{snapname}"
    args.append(ssh_cmd)

    p = subprocess.run(args, stdout=subprocess.PIPE)
    output["exitcode"]  = p.returncode
    if p.returncode != 0:
         output["result"] = p.stderr.decode()
         return False
    else:
         output["result"] = p.stdout.decode()
         return True

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--projectname', required = True, help = 'Project we are performing scan for.')
    parser.add_argument('-s', '--zfssourcepath', required = True, help = 'Source path to scan')
    args = parser.parse_args()

    config_dict = get_zfs_config_dict()
    output_dict = {}

    epoch_time = 0

    lock = FileLock("/tmp/zfs_create.lck")
    try:
        with lock.acquire(timeout=10):
            time.sleep(1)
            epoch_time = int(time.time())
            time.sleep(1)

    except Timeout:
         print("Unable to get lock for zfs snapshot.")
         exit(88)

    new_snapshot_name = f'{args.projectname}-{epoch_time}'

    ### Get Jobs List
    new_snapshot_path = args.zfssourcepath

    # Now create the new snapshot

    output_dict = {}

    if len(config_dict) > 0:
        if run_zfs_ssh_process(config_dict, new_snapshot_path, new_snapshot_name, output_dict) == False:
            print("Unable to create snpashot " + output_dict["result"])
            exit(output_dict["exitcode"])
    else:
       if run_zfs_process(new_snapshot_path, new_snapshot_name, output_dict) == False:
           print("Unable to create snapshot " + output_dict["result"])
           exit(output_dict["exitcode"])

    new_snapshot_id = epoch_time

    print(new_snapshot_id)
    exit(0)

