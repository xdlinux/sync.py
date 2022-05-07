#!env python3
from pathlib import Path
from datetime import datetime
import subprocess
import json
import os
import fcntl

from utils import get_logger, parse_config, update_status_json, parse_state

general, mirrors = parse_config()
os.chdir(general['chdir'])
log = get_logger(general['logdir'], 'sync')


def do_sync(mirror, lastsuccess=None):
    status['state'] = f'Y{int(datetime.now().timestamp())}'
    if lastsuccess:
        status['state'] += f'O{int(lastsuccess.timestamp())}'
    log.info(f'syncing {mirror}')
    with open(path, 'w') as f:
        f.write(json.dumps(status))
    update_status_json(mirrors)

    proc_output_path = Path('logs.d', 'output', f'{mirror}-{status["state"]}')
    proc_output = open(proc_output_path, 'w')
    proc = subprocess.Popen(
        executable='/bin/sh',
        args=["sh", "-c", status['command']],
        stderr=subprocess.STDOUT,
        stdout=proc_output,
    )
    proc_output.close()

    try:
        code = proc.wait()
    except KeyboardInterrupt:
        code = -1

    if code == 0:
        log.info(f'successfully synced {mirror}')
        message = f'Successfully Synced {mirror}.'
        proc_output_path.unlink()
        status['state'] = f'S{int(datetime.now().timestamp())}'
    elif code == -1:
        message = f'Paused Syncing {mirror}.'
        status['state'] = f'P{int(datetime.now().timestamp())}'
        if lastsuccess:
            message += f' Last Successful Sync: {lastsuccess.strftime(general["timeformat"])}.'
            status['state'] += f'O{int(lastsuccess.timestamp())}'
    else:
        message = f'Error Occured Syncing {mirror}.'
        log.error(f'error syncing {mirror}')
        status['state'] = f'F{int(datetime.now().timestamp())}'
        if lastsuccess:
            message += f' Last Successful Sync: {lastsuccess.strftime(general["timeformat"])}.'
            status['state'] += f'O{int(lastsuccess.timestamp())}'

    with open(path, 'w') as f:
        f.write(json.dumps(status))
    update_status_json(mirrors)


if os.path.exists('sync.lock'):
    print('sync.py already running...')
    exit()

with open('sync.lock', 'w+') as f:
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        print('sync.py already running...')
        exit()

    for mirror in mirrors.sections():
        path = Path('status.d', mirror)
        path.touch()
        with open(path, 'r') as f:
            status = f.read()
            try:
                status = json.loads(status)
            except json.JSONDecodeError:
                status = {}
            if not status:
                log.warning(f'{mirror} never synced, syncing for the first time...')
        status['name'] = mirror
        status['command'] = mirrors[mirror]['command'].format(**general['vars'])
        lastsuccess = None
        for state, time in parse_state(status.get('state') or ''):  # follows mirrorz rules
            if state == 'S':
                if (datetime.now() - time).total_seconds() < 28800:
                    log.info(f'skipping {mirror}, less than 8 hours since last sync')
                    break
                lastsuccess = time
            elif state == 'Y':
                break
            elif state == 'O' and lastsuccess is None:
                lastsuccess = time
        else:
            do_sync(mirror, lastsuccess)

    # execute this unconditionally on exit
    # to update status.json when mirrorz.meta.json changes
    update_status_json(mirrors)
    try:
        fcntl.flock(f, fcntl.LOCK_UN)
    finally:
        os.remove('sync.lock')
