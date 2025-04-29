from pprint import pprint
import os
import subprocess
import json
import platform
import time
import sys
import stat
import time    
import argparse
from configparser import ConfigParser
import plistlib

ZFS_HOSTNAME = "ZFS_HOSTNAME"
ZFS_USERNAME = "ZFS_USERNAME"
ZFS_KEYFILE  = "ZFS_KEYFILE"

global_path_list = []
global_data_map = { }
global_file_counts = { }
global_walkhrough_error = False

global_file_counts["total_size"] = 0;
global_file_counts["total_count"] = 0;
global_file_counts["delete_count"] = 0;
global_file_counts["rename_count"] = 0;
global_file_counts["bad_dir_count"] = 0;

def escape( str ):
    str = str.replace("&", "&amp;")
    str = str.replace("<", "&lt;")
    str = str.replace(">", "&gt;")
    str = str.replace("\"", "&quot;")
    return str

def get_scan_folder_output_folder(project_name, project_guid):

    is_linux=0
    if platform.system() == "Linux":
        DNA_CLIENT_SERVICES = '/etc/StorageDNA/DNAClientServices.conf'
        is_linux=1
    elif platform.system() == "Darwin":
        DNA_CLIENT_SERVICES = '/Library/Preferences/com.storagedna.DNAClientServices.plist'

    fastScanWorkFolder = ""

    if is_linux == 1:
        config_parser = ConfigParser()
        config_parser.read(DNA_CLIENT_SERVICES)
        if config_parser.has_section('General') and config_parser.has_option('General','FastScanWorkFolder'):
            section_info = config_parser['General']
            fastScanWorkFolder = section_info['FastScanWorkFolder']
    else:
        with open(DNA_CLIENT_SERVICES, 'rb') as fp:
            my_plist = plistlib.load(fp)
            fastScanWorkFolder  = my_plist["FastScanWorkFolder"]

    if (len(fastScanWorkFolder) == 0):
        fastScanWorkFolder = "/tmp"

    fastScanWorkFile = fastScanWorkFolder + '/sdna-scan-files/' + project_guid
    return fastScanWorkFile


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

def get_scan_folder_output_folder(project_name, project_guid):

    is_linux=0
    if platform.system() == "Linux":
        DNA_CLIENT_SERVICES = '/etc/StorageDNA/DNAClientServices.conf'
        is_linux=1
    elif platform.system() == "Darwin":
        DNA_CLIENT_SERVICES = '/Library/Preferences/com.storagedna.DNAClientServices.plist'

    fastScanWorkFolder = ""

    if is_linux == 1:
        config_parser = ConfigParser()
        config_parser.read(DNA_CLIENT_SERVICES)
        if config_parser.has_section('General') and config_parser.has_option('General','FastScanWorkFolder'):
            section_info = config_parser['General']
            fastScanWorkFolder = section_info['FastScanWorkFolder']
    else:
        with open(DNA_CLIENT_SERVICES, 'rb') as fp:
            my_plist = plistlib.load(fp)
            fastScanWorkFolder  = my_plist["FastScanWorkFolder"]

    if (len(fastScanWorkFolder) == 0):
        fastScanWorkFolder = "/tmp"

    fastScanWorkFile = fastScanWorkFolder + '/sdna-scan-files/' + project_guid
    return fastScanWorkFile


def get_stat_file_obj(given_path, rel_path, action, base):

    file_map = { }

    try:
        stat_info = os.lstat(given_path)
        if stat.S_ISDIR(stat_info.st_mode):
            file_map["type"] = 'dir'
        elif stat.S_ISREG(stat_info.st_mode):
            file_map["type"] = 'file'
        else:
            return file_map

        file_map["uid"] = stat_info.st_uid
        file_map["gid"] = stat_info.st_gid
        file_map["action"] = action
        file_map["mtime"] = stat_info.st_mtime
        file_map["atime"] = stat_info.st_atime
        file_map["size"] = stat_info.st_size
        file_map["mode"] = stat_info.st_mode
        file_map["path"] = rel_path

    except (FileNotFoundError, OSError):

        file_map = {}
        file_map["type"] = 'dir'
        if os.path.isfile(given_path):
            file_map["type"] = 'file'

        epoch_time = int(time.time())

        file_map["uid"] = 0
        file_map["gid"] = 0
        file_map["action"] = action
        file_map["mtime"] = epoch_time
        file_map["atime"] = epoch_time
        file_map["size"] = 0
        file_map["mode"] = "0x777"

        if action != '-':
            file_map["type"] = 'dir'
            file_map["action"] = "BADDIR"
            global_file_counts["bad_dir_count"] = global_file_counts["bad_dir_count"] +  1
        else:
            file_map["action"] = "-"
            file_map["type"] = 'file'
            global_file_counts["delete_count"] = global_file_counts["delete_count"] +  1

        file_map["entry"] = given_path
        file_map["path"] = rel_path

    return file_map

def process_zsh_diff( base_path, mapped_path, source_path, my_list, deletes_on, out_config):
 
     for line in my_list:
        items = line.split("\t")
        action = items.pop(0)
        pathname = '\t'.join(items)

        try:
            if pathname.startswith(base_path):
                pathname = pathname.replace(mapped_path, base_path)
                if not pathname.startswith(source_path):
                    continue
            if action == 'R':
                path_parts = pathname.split(" -> ")
                rename_from_path = path_parts[0]
                rename_to_path = path_parts[1]
                rel_path = rename_to_path[len(source_path):]
                file_map = get_stat_file_obj(rename_to_path, rel_path, action, base_path)
                file_map["rename-from"] = rename_from_path
                global_data_map[rel_path] = file_map
                global_path_list.append(rel_path)
                continue

            elif action == '-':
                 if deletes_on:
                     rel_path = pathname[len(source_path):]
                     file_map = get_stat_file_obj(pathname, rel_path, action, base_path)
                     file_map["rename-from"] = ""
                     global_data_map[rel_path] = file_map
                     global_path_list.append(rel_path)
            elif action == 'M' or action =='+':
                rel_path = pathname[len(source_path):]
                file_map = get_stat_file_obj(pathname, rel_path, action, base_path)
                file_map["rename-from"] = ""
                global_data_map[rel_path] = file_map
                global_path_list.append(rel_path)
            else:
                if len(action) == 0:
                    continue

                print("Unknown action found in listing: " + action)
                sys.exit(99)

            if file_map["type"] == 'file':
                global_file_counts["total_size"] = global_file_counts["total_size"] + file_map["size"]
                global_file_counts["total_count"] =  global_file_counts["total_count"] + 1

        except OSError:
            file_map = {}
            file_map["type"] = 'dir'
            if os.path.isfile(pathname):
                file_map["type"] = 'file'
            file_map["action"] = "BADDIR"
            file_map["entry"] = path
            global_file_counts["bad_dir_count"] = global_file_counts["bad_dir_count"] + 1

def run_zfs_ssh_process(config, prev_snap, cur_snap, output):

    args = ["ssh"]
    args.append("-i")
    args.append(config[ZFS_KEYFILE])

    username = f"{config[ZFS_USERNAME]}@{config[ZFS_HOSTNAME]}"
    args.append(username)

    ssh_cmd = f"zfs diff {prev_snap} {cur_snap}"
    args.append(ssh_cmd)

    p = subprocess.run(args, stdout=subprocess.PIPE)
    output["exitcode"]  = p.returncode
    if p.returncode != 0:
         output["result"] = p.stderr.decode()
         return False

    str_result = p.stdout.decode()
    output["result"] = str_result.split('\n')
    return True

def run_zfs_process(prev_snap, cur_snap, output):

    args = ["zfs"]
    args.append("diff")
    args.append(f'{prev_snap}')
    args.append(f'{cur_snap}')

    p = subprocess.run(args, stdout=subprocess.PIPE)
    output["exitcode"]  = p.returncode
    if p.returncode != 0:
         output["result"] = p.stderr.decode()
         return False

    str_result = p.stdout.decode()
    output["result"] = str_result.split('\n')
    return True

def write_xml_result(xml_file, index):

    total_count = global_file_counts["total_count"]
    total_size = global_file_counts["total_size"]
    delete_count = global_file_counts["delete_count"]
    bad_dir_count = global_file_counts["bad_dir_count"]

    xml_file.write("<files scanned=\"" + str(total_count) + "\" selected=\"" + str(total_count) + "\" size=\"" + str(total_size) + "\" bad_dir_count=\"" + str(bad_dir_count)+ "\" delete_count=\"" + str(delete_count) + "\">\n");

    global_path_list.sort()

    for list_entry in reversed(global_path_list):

        entry = global_data_map[list_entry]
        if entry['type'] == 'file':
             if  entry['action'] == '+' or entry['action'] == 'M':
                 xml_file.write("    <file name=\"" + escape(entry['path']) + "\" size=\"" + str(entry['size'])  + "\" mode=\"0x777\"  type=\"F_REG\" mtime=\"" + str(int(entry['mtime'])) + "\" atime=\"" + str(int(entry['atime'])) + "\" owner=\"" + str(entry['uid']) + "\" group=\"" + str(entry['gid']) + "\" index=\"" + str(index) + "\"/>\n")
             elif entry['action'] == 'R' and len(entry['rename-from']) > 0:
                 xml_file.write("    <file name=\"" + escape(entry['path']) +  "\" from=\"" + escape(entry['rename-from']) + "\" size=\"" + str(entry['size'])  + "\" mode=\"0x777\"  type=\"F_REG\" mtime=\"" + str(int(entry['mtime'])) + "\" atime=\"" + str(int(entry['atime'])) + "\" owner=\"" + str(entry['uid']) + "\" group=\"" + str(entry['gid']) + "\" index=\"" + str(index) + "\"/>\n")
             elif entry['action'] == '-':
                 xml_file.write("    <deleted-file name=\"" + escape(entry['path']) +  "\" from=\"" + escape(entry['rename-from']) + "\" size=\"" + str(entry['size'])  + "\" mode=\"0x777\"  type=\"F_REG\" mtime=\"" + str(int(entry['mtime'])) + "\" atime=\"" + str(int(entry['atime'])) + "\" owner=\"" + str(entry['uid']) + "\" group=\"" + str(entry['gid']) + "\" index=\"" + str(index) + "\"/>\n")
        elif entry['action'] == 'BADDIR':
                 xml_file.write("    <bad-dir name=\"" + escape(entry['path']) +  "\" from=\"" + escape(entry['rename-from']) + "\" size=\"" + str(entry['size'])  + "\" mode=\"0x777\"  type=\"F_REG\" mtime=\"" + str(int(entry['mtime'])) + "\" atime=\"" + str(int(entry['atime'])) + "\" owner=\"" + str(entry['uid']) + "\" group=\"" + str(entry['gid']) + "\" index=\"" + str(index) + "\"/>\n")

    xml_file.write("</files>\n")
    xml_file.close()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--projectname', required = True, help = 'Project we are performing scan for.')
    parser.add_argument('-g', '--projectguid', required = True, help = 'Project guid we are performing scan for.')
    parser.add_argument('-i', '--sourceindex', required = True, help = 'Numeric index of source folders')
    parser.add_argument('-m', '--mappedpath', required = True, help = 'Path result should be mapped to.')
    parser.add_argument('-b', '--basepath', required = True, help = 'Path result should be mapped to.')
    parser.add_argument('-s', '--sourcepath', required = True, help = 'Source path to look for.')
    parser.add_argument('--prevsnapshotid', required = True, help = 'Required prev snapshot')
    parser.add_argument('--newsnapshotid', required = True, help = 'Required new snapshot')
    parser.add_argument('-d', '--deletes', help = 'Use mirror deletes', action='store_true')

    args = parser.parse_args()

    config_dict = get_zfs_config_dict()
    output_dict = {}

    # strip any /./ on the source path
    full_source_path = args.sourcepath.replace("/./","/")

    # Now create the new snapshot
    prev_snapshot_id = f'{args.mappedpath}@{args.projectname}-{args.prevsnapshotid}'
    next_snapshot_id = f'{args.mappedpath}@{args.projectname}-{args.newsnapshotid}'

    output_folder = get_scan_folder_output_folder(args.projectname, args.projectguid)
    if not os.path.isdir(output_folder):
        os.makedirs(output_folder, exist_ok=True)
    if not os.path.isdir(output_folder):
        pathlib.Path(output_folder).mkdir(parents=True, exist_ok=True)
        print("Unable to create output folder")
        exit(4)

    if len(config_dict) > 0:
        if run_zfs_ssh_process(config_dict, prev_snapshot_id, next_snapshot_id, output_dict) == False:
            print("Unable to generate snapshot difference " + output_dict["result"])
            exit(output_dict["exitcode"])
    else:
        if run_zfs_process(prev_snapshot_id, next_snapshot_id, output_dict) == False:
            print("Unable to generate snapshot difference " + output_dict["result"])
            exit(output_dict["exitcode"])    
    process_dict = {}
    if process_zsh_diff(args.basepath, args.mappedpath, full_source_path, output_dict["result"], args.deletes, process_dict) == False:
        print("Unable to process zsh diff " + output_dict["result"])
        exit(output_dir["exitcode"])

    output_file = output_folder + "/" + str(args.sourceindex) + "-files.xml"
    xml_file = open(output_file, "w")
        
    write_xml_result(xml_file, args.sourceindex)
    print(output_file)
    exit(0)

