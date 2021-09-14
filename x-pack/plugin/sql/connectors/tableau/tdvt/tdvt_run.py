#!/usr/bin/python3
#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License
# 2.0; you may not use this file except in compliance with the Elastic License
# 2.0.
#

import subprocess
import inspect
import os
import urllib3
import re
import threading
import ipaddress
import argparse
import time
import getpass


TDVT_SDK_NAME = "connector-plugin-sdk"
TDVT_SDK_REPO = "https://github.com/tableau/" + TDVT_SDK_NAME
TDVT_SDK_BRANCH = "tdvt-2.1.9"
TDVT_ES_SCHEME = "simple_lower"

TDVT_RUN_DIR = "run"

TIMEOUTS = {"_default_": 5, "checkout_tdvt_sdk": 60, "setup_workspace": 10, "add_data_source": 300, "run_tdvt": 1800}
TDVT_LAUNCHER = os.path.join(TDVT_SDK_NAME, "tdvt", "tdvt_launcher.py")

#
# configs
TDS_SRC_DIR = "tds"
TACO_SRC_DIR = "C:\\Users\\" + getpass.getuser() + "\\Documents\\My Tableau Repository\\Connectors"
TACO_SIGNED = True
ES_URL = "http://elastic-admin:elastic-password@127.0.0.1:9200"


def interact(proc, interactive):
    # no straigh forward non-blocking read on Win -> char reader in own thread
    def read_stdout(buff, condition):
        c = " "
        while c != "":
            c = proc.stdout.read(1)
            condition.acquire()
            buff.append('\0' if c == "" else c)
            condition.notify()
            condition.release()

    buff = [' ']
    condition = threading.Condition()
    reader = threading.Thread(target=read_stdout, args=(buff, condition))
    reader.start()

    interactive.reverse()
    while len(interactive) > 0 and reader.is_alive():
        token, answer = interactive.pop()
        condition.acquire()
        while buff[-1] != '\0':
            output = "".join(buff)
            if token not in output:
                condition.wait()
            else:
                condition.release()
                proc.stdin.write(answer + '\n')
                proc.stdin.flush()
                break

    reader.join()


def exe(args, interactive = None, raise_on_retcode = True):
    with subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True) as proc:
        if interactive is not None:
            interact(proc, interactive)

        try:
            caller = inspect.stack()[1][3]
            proc.wait(TIMEOUTS[caller] if caller in TIMEOUTS.keys() else TIMEOUTS["_default_"])
        except subprocess.TimeoutExpired as e:
            proc.kill()
        stdout, stderr = proc.communicate()
        if proc.returncode != 0 and raise_on_retcode:
            print("command stdout: \n" + stdout)
            print("command stderr: \n" + stderr)
            raise Exception("Command exited with code %s: '%s' !" % (proc.returncode, args))
        return (proc.returncode, stdout, stderr)

def checkout_tdvt_sdk():
    git_args = ["git", "clone", "--depth", "1", "--branch", TDVT_SDK_BRANCH, TDVT_SDK_REPO]
    exe(git_args)

def setup_workspace():

    tdvt_args = ["py", "-3", TDVT_LAUNCHER, "action", "--setup"]
    exe(tdvt_args)

def install_tds_files(tds_src_dir, elastic_url):
    TDVT_TDS_DIR = "tds"

    def dbname(es_host):
        try:
            ipaddress.ip_address(es_host)
            return es_host
        except ValueError as ve:
            pos = es_host.find(".")
            return es_host if pos <= 0 else es_host[:pos]

    es_url = urllib3.util.parse_url(elastic_url)
    if ':' in es_url.auth:
        (es_user, es_pass) = es_url.auth.split(':')
    else:
        (es_user, es_pass) = es_url.auth, ""

    for (dirpath, dirnames, filenames) in os.walk(tds_src_dir, topdown=True, followlinks=False):
        if dirpath != tds_src_dir:
            break
        for filename in filenames:
            if not (filename.endswith(".tds") or filename.endswith(".password")):
                continue
            with open(os.path.join(tds_src_dir, filename)) as src:
                content = src.read()
                if filename.endswith(".tds"):
                    content = content.replace("caption='127.0.0.1'", "caption='" + es_url.host + "'")
                    content = content.replace("dbname='elasticsearch'", "dbname='" + dbname(es_url.host) + "'")
                    content = content.replace("server='127.0.0.1'", "server='" + es_url.host + "'")
                    content = content.replace("port='9200'", "port='" + str(es_url.port) + "'")
                    if es_user != "elastic":
                        content = content.replace("username='elastic'", "username='" + es_user + "'")
                    if es_url.scheme.lower() == "https":
                        content = content.replace("sslmode=''", "sslmode='require'")
                elif filename.endswith(".password"):
                    content = content.replace("<REDACTED>", es_pass)
                else:
                    continue # shouldn't happen

                with open(os.path.join(TDVT_TDS_DIR, filename), "w") as dest:
                    dest.write(content)

def latest_tabquery():
    TABLEAU_INSTALL_FOLDER = os.path.join("C:\\", "Program Files", "Tableau")
    TABQUERY_UNDERPATH = os.path.join("bin", "tabquerytool.exe")

    latest = ""
    for (dirpath, dirnames, filenames) in os.walk(TABLEAU_INSTALL_FOLDER, topdown=True):
        if dirpath != TABLEAU_INSTALL_FOLDER:
            pass #break
        for dirname in dirnames:
            if re.match("^Tableau 202[0-9]\.[0-9]$", dirname):
                if dirname > latest:
                    latest = dirname
    tabquery_path = os.path.join(TABLEAU_INSTALL_FOLDER, latest, TABQUERY_UNDERPATH)
    os.stat(tabquery_path) # check if the executable's there
    return tabquery_path

def config_tdvt_override_ini():
    TDVT_INI_PATH = os.path.join("config", "tdvt", "tdvt_override.ini")

    tabquery_path = latest_tabquery()
    tabquery_path_line = "TAB_CLI_EXE_X64 = " + tabquery_path

    updated_lines = []
    with open(TDVT_INI_PATH) as ini:
        for line in ini.readlines():
            l = line if not line.startswith("TAB_CLI_EXE_X64") else tabquery_path_line
            l += '\n'
            updated_lines.append(l)
    if len(updated_lines) <= 0:
        print("WARNING: empty ini file under: " + TDVT_INI_PATH)
        updated_lines.append("[DEFAULT]\n")
        updated_lines.append(tabquery_path_line + '\n')
    with open(TDVT_INI_PATH, "w") as ini:
        ini.writelines(updated_lines)

def add_data_source():
    tdvt_args = ["py", "-3", TDVT_LAUNCHER, "action", "--add_ds", "elastic"]
    interactive = [("connection per tds (standard).", "n"), ("to skip selecting one now:", TDVT_ES_SCHEME),
            ("Overwrite existing ini file?(y/n)", "y")]

    exe(tdvt_args, interactive)

def config_elastic_ini():
    ELASTIC_INI = os.path.join("config", "elastic.ini")

    cmdline_override = "CommandLineOverride = -DConnectPluginsPath=%s -DDisableVerifyConnectorPluginSignature=%s" % \
            (TACO_SRC_DIR, TACO_SIGNED)

    updated_lines = []
    with open(ELASTIC_INI) as ini:
        for line in ini.readlines():
            updated_lines.append(line)
            if line.startswith("LogicalQueryFormat"):
                updated_lines.append(cmdline_override)
    with open(ELASTIC_INI, "w") as ini:
        ini.writelines(updated_lines)

def run_tdvt():
    tdvt_args = ["py", "-3", TDVT_LAUNCHER, "run", "elastic"]

    _, stdout, __ = exe(tdvt_args, raise_on_retcode = False)
    print(stdout)

def parse_args():
    parser = argparse.ArgumentParser(description="TDVT runner of the Tableau connector for Elasticsearch.",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-t", "--taco-dir", help="Directory containing the connector file.",
            default=TACO_SRC_DIR)
    parser.add_argument("-s", "--signed", help="Is the .taco signed?", action="store_true", default=TACO_SIGNED)
    parser.add_argument("-r", "--run-dir", help="Directory to run the testing under.",
            default=TDVT_RUN_DIR)
    parser.add_argument("-u", "--url", help="Elasticsearch URL.", type=str, default=ES_URL)
    parser.add_argument("-c", "--clean", help="Clean-up run directory", action="store_true", default=False)

    return parser.parse_args()

def main():
    started_at = time.time()

    args = parse_args()

    cwd = os.getcwd()
    if args.clean:
        import shutil # dependency!
        shutil.rmtree(args.run_dir)
    if not os.path.isdir(args.run_dir):
        os.makedirs(args.run_dir)
    os.chdir(args.run_dir)

    if not os.path.isdir(TDVT_SDK_REPO):
        checkout_tdvt_sdk()
    setup_workspace()

    tds_src = TDS_SRC_DIR if os.path.isabs(TDS_SRC_DIR) else os.path.join(cwd, TDS_SRC_DIR)
    install_tds_files(tds_src, args.url)

    config_tdvt_override_ini()
    add_data_source()
    if args.taco_dir != TACO_SRC_DIR and args.signed != TACO_SIGNED:
        config_elastic_ini()

    run_tdvt()

    print("Test run took %.2f seconds." % (time.time() - started_at))

if __name__ == "__main__":
    main()

# vim: set noet fenc=utf-8 ff=dos sts=0 sw=4 ts=4 tw=118 expandtab :
