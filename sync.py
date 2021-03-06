#!env python3
import asyncio
from pathlib import Path
from datetime import datetime
import json
import os
import fcntl
import shutil

from utils import get_logger, parse_config, update_status_json, parse_state

general, mirrors = parse_config()
os.chdir(general['chdir'])
log = get_logger(general['logdir'], 'sync')
sema = asyncio.BoundedSemaphore(5)


async def do_sync(mirror):
    path = Path('status', mirror)
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
    lastsuccess = None
    for state, time in parse_state(status.get('state') or ''):  # follows mirrorz rules
        if state == 'S':
            if (datetime.now() - time).total_seconds() < 14400:
                log.info(f'skipping {mirror}, less than 4 hours since last sync')
                break
            lastsuccess = time
        elif state == 'Y':
            break
        elif state == 'O' and lastsuccess is None:
            lastsuccess = time
    else:
        log.info(f'syncing {mirror}')
        status['state'] = f'Y{int(datetime.now().timestamp())}'
        if lastsuccess:
            status['state'] += f'O{int(lastsuccess.timestamp())}'
        with open(path, 'w') as f:
            f.write(json.dumps(status))
        update_status_json(mirrors)

        proc_output_path = Path('logs', mirror)
        proc_output = open(proc_output_path, 'w')
        proc = await asyncio.create_subprocess_shell(
            cmd=mirrors[mirror]['command'].format(**general['vars']),
            stderr=asyncio.subprocess.DEVNULL,
            stdout=proc_output,
        )
        try:
            code = await proc.wait()
        except KeyboardInterrupt:
            code = -1
        proc_output.close()

        if code == 0:
            log.info(f'successfully synced {mirror}')
            message = f'Successfully Synced {mirror}.'
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
            shutil.copyfile(proc_output_path, Path('logs', f'{mirror}-{datetime.now().strftime(general["timeformat"])}'))

        proc_output_path.unlink()

        with open(path, 'w') as f:
            f.write(json.dumps(status))
        update_status_json(mirrors)


async def limited_sync(mirror):
    async with sema:
        await do_sync(mirror)

async def loop_tasks():
    tasks = []
    for mirror in mirrors.sections():
        tasks.append(asyncio.create_task(limited_sync(mirror)))

    await asyncio.wait(tasks)

    # execute this unconditionally on exit
    # to update status.json when mirrorz.meta.json changes
    update_status_json(mirrors)


if os.path.exists('sync.lock'):
    print('sync.py already running...')
    exit()

with open('sync.lock', 'w+') as fl:
    try:
        fcntl.flock(fl, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        print('sync.py already running...')
        exit()

    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(loop_tasks())
        loop.close()
        fcntl.flock(fl, fcntl.LOCK_UN)
    finally:
        os.remove('sync.lock')
