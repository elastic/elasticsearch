#!/usr/bin/env python3
"""Run a local query playground, optionally accepting an IntelliJ debugger."""

import argparse
from datetime import datetime
import fcntl
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request


REPO = Path(__file__).resolve().parents[1]
KIBANA_REPO = Path('/Users/kishor/work/kibana')
KIBANA_BIN = Path('/Users/kishor/Downloads/kibana-9.4.4/bin/kibana')
EIS_URL = 'https://inference.eu-west-1.aws.svc.qa.elastic.cloud'


def say(message):
    print(message, flush=True)


def run(command, cwd, **kwargs):
    say(f'[{cwd}] {shlex.join(map(str, command))}')
    return subprocess.run(command, cwd=cwd, check=True, **kwargs)


def listening(port):
    try:
        with socket.create_connection(('localhost', port), timeout=1):
            return True
    except OSError:
        return False


def get_json(url):
    # Local requests must not go through a corporate HTTP proxy.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(url, timeout=3) as response:
        return json.load(response)


def check_children(children):
    for name, process in children:
        if process.poll() is not None:
            raise RuntimeError(f'{name} exited with code {process.returncode}; see its log.')


def wait_ready(name, url, predicate, children, timeout):
    say(f'Waiting for {name}: {url}')
    deadline = time.monotonic() + timeout
    last_update = 0
    while time.monotonic() < deadline:
        check_children(children)
        try:
            if predicate(get_json(url)):
                say(f'{name} is ready.')
                return
        except (OSError, ValueError, urllib.error.URLError):
            pass
        if time.monotonic() - last_update >= 30:
            say(f'  Still waiting for {name}; logs are available below.')
            last_update = time.monotonic()
        time.sleep(2)
    raise RuntimeError(f'{name} did not become ready within {timeout}s; see its log.')


def stop_children(children):
    # Each service gets its own process group. Never stop a reused Kibana.
    for _, process in reversed(children):
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and any(p.poll() is None for _, p in children):
        time.sleep(0.2)
    for _, process in reversed(children):
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


def gradle_command(args):
    command = [
        './gradlew', '--no-daemon', '--console=plain', ':run', '--nodes=1',
        '-Dtests.es.xpack.security.enabled=false',
        '-Dtests.es.xpack.license.self_generated.type=trial',
        '-Dtests.es.network.host=127.0.0.1',
        f'-Dtests.heap.size={args.heap}', '-Dtests.jvm.argline=-da -dsa',
        f'-Dtests.es.xpack.inference.elastic.url={EIS_URL}',
    ]
    if args.debug:
        command.extend(['--init-script', str(REPO / 'local-tools/query-stack-debug.gradle'),
                        f'-Dquery-stack.debug.port={args.debug_port}'])
    return command


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', choices=['local', 'main'],
                        help='local includes current working-tree edits; main fetches upstream/main')
    parser.add_argument('--debug', action='store_true', help='allow IntelliJ to attach after startup')
    parser.add_argument('--debug-port', type=int, default=5005)
    parser.add_argument('--heap', default='7g')
    parser.add_argument('--timeout', type=int, default=1800, help='readiness timeout per service, in seconds')
    parser.add_argument('--skip-eis', action='store_true', help='skip Vault login and EIS configuration')
    parser.add_argument('--dry-run', action='store_true', help='show commands without changing or starting anything')
    args = parser.parse_args()
    if not 1 <= args.debug_port <= 65535 or args.timeout <= 0:
        parser.error('debug port must be 1..65535 and timeout must be positive')

    state = REPO / '.gradle/query-stack'
    checkout = REPO if args.source == 'local' else state / 'upstream-main'
    command = gradle_command(args)
    kibana_command = [str(KIBANA_BIN), '--elasticsearch.hosts=http://localhost:9200']
    node_version = (KIBANA_REPO / '.nvmrc').read_text().strip().lstrip('v')
    nvm_node = Path.home() / f'.nvm/versions/node/v{node_version}/bin/node'
    node = str(nvm_node) if nvm_node.is_file() else shutil.which('node')
    vault_env = {**os.environ, 'VAULT_ADDR': 'https://secrets.elastic.co:8200'}
    if node:
        vault_env['PATH'] = str(Path(node).parent) + os.pathsep + os.environ.get('PATH', '')
    # Match the cluster just started, regardless of inherited EIS host variables.
    vault_env.update(ES_HOST='http://localhost', ES_PORT='9200')

    say(f'Elasticsearch checkout: {checkout}')
    say(f'Elasticsearch command: {shlex.join(command)}')
    say(f'Kibana command (if needed): {shlex.join(kibana_command)}')
    if args.source == 'main':
        say('Fetch upstream/main into a separate detached worktree; keep the local checkout intact.')
    if not args.skip_eis:
        say(f'EIS: Vault OIDC login, then {node or "node"} scripts/eis.js in {KIBANA_REPO}')
    if args.debug:
        say(f'IntelliJ: Remote JVM Debug / Attach to remote JVM / localhost:{args.debug_port}')
    if args.dry_run:
        return

    for tool in ['git', 'java'] + ([] if args.skip_eis else ['vault']):
        if not shutil.which(tool):
            raise RuntimeError(f'Required executable is missing from PATH: {tool}')
    if not KIBANA_BIN.is_file():
        raise RuntimeError(f'Kibana executable is missing: {KIBANA_BIN}')
    if not args.skip_eis:
        if not node:
            raise RuntimeError(f'Install Node {node_version}, as required by {KIBANA_REPO}/.nvmrc')
        actual = subprocess.check_output([node, '--version'], text=True).strip().lstrip('v')
        if actual != node_version:
            raise RuntimeError(f'EIS needs Node {node_version}; selected Node is {actual}')
    state.mkdir(parents=True, exist_ok=True)
    with (state / 'launcher.lock').open('w') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('Another query-stack launcher is already running.') from None
        for port in [9200, 9300] + ([args.debug_port] if args.debug else []):
            if listening(port):
                raise RuntimeError(f'Port {port} is in use. Stop its owner before starting this stack.')

        if args.source == 'main':
            run(['git', 'fetch', 'upstream', 'main'], REPO)
            commit = subprocess.check_output(['git', 'rev-parse', 'FETCH_HEAD'], cwd=REPO, text=True).strip()
            if checkout.exists():
                status = subprocess.check_output(['git', 'status', '--porcelain'], cwd=checkout, text=True)
                if status.strip():
                    raise RuntimeError(f'Main worktree has edits; preserve or commit them first: {checkout}')
                run(['git', 'checkout', '--detach', commit], checkout)
            else:
                run(['git', 'worktree', 'add', '--detach', str(checkout), commit], REPO)

        logs = state / 'logs' / datetime.now().strftime('%Y%m%d-%H%M%S-%f')
        logs.mkdir(parents=True)
        children = []

        def start(name, cmd, cwd):
            path = logs / f'{name}.log'
            say(f'{name} log: {path}')
            with path.open('w') as output:
                process = subprocess.Popen(cmd, cwd=cwd, stdin=subprocess.DEVNULL,
                                           stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
            children.append((name, process))

        def interrupted(signum, frame):
            raise KeyboardInterrupt

        signal.signal(signal.SIGTERM, interrupted)
        try:
            start('elasticsearch', command, checkout)
            wait_ready('Elasticsearch', 'http://localhost:9200/_cluster/health',
                       lambda result: result.get('cluster_name') == 'runTask'
                       and result.get('number_of_nodes') == 1 and result.get('status') in ('green', 'yellow'),
                       children, args.timeout)
            if listening(5601):
                say('Reusing Kibana on localhost:5601; its existing Elasticsearch configuration applies.')
            else:
                start('kibana', kibana_command, KIBANA_BIN.parent)
            wait_ready('Kibana', 'http://localhost:5601/api/status',
                       lambda result: result.get('status', {}).get('overall', {}).get('level') == 'available',
                       children, args.timeout)
            if not args.skip_eis:
                say('Complete Vault OIDC login in your browser when prompted.')
                run(['vault', 'login', '--method', 'oidc', '-no-print'], KIBANA_REPO, env=vault_env)
                check_children(children)
                run([node, 'scripts/eis.js'], KIBANA_REPO, env=vault_env)
            say('Ready: http://localhost:5601/app/dev_tools#/console')
            say('Press Ctrl+C to stop Elasticsearch and any Kibana started by this launcher.')
            while True:
                check_children(children)
                time.sleep(2)
        finally:
            # Ignore further interrupts while allowing Gradle to clean up.
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            stop_children(children)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        say('Stopped.')
    except (RuntimeError, OSError, subprocess.CalledProcessError) as error:
        print(f'Error: {error}', file=sys.stderr)
        sys.exit(1)
