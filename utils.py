from configparser import ConfigParser
from logging import Logger, handlers, Formatter
from pathlib import Path
from datetime import datetime
import re
import json
from typing import Tuple


def parse_config(file='config.ini'):
    config = ConfigParser()
    config.read(file)

    general = {}
    for k, v in config['general'].items():
        if res := re.match(r'^(.*)\[(.*)\]$', k):
            if res[1] not in general:
                general[res[1]] = {}
            general[res[1]][res[2]] = v
        else:
            general[k] = v
    config.remove_section('general')
    return general, config


def get_logger(logdir, name):
    log = Logger(name)
    handler = handlers.RotatingFileHandler(Path(logdir, f'{name}.log'), maxBytes=1024 * 128)
    handler.setFormatter(Formatter('[%(asctime)s] - %(message)s'))
    log.addHandler(handler)
    return log


def timespan_fromtimestamp(ts):
    return (datetime.now() - datetime.fromtimestamp(ts))


def update_status_json(mirrors):
    with open('mirrorz.meta.json', 'r') as f:
        status = json.loads(f.read())
    for mirror in mirrors.sections():
        path = Path('status.d', mirror)
        path.touch()
        obj = {
            "cname": mirror,
            "desc": mirrors[mirror].get('desc') or '',
            "url": mirrors[mirror].get('url') or f'/{mirror}',
        }
        with open(path, 'r') as f:
            try:
                job = json.loads(f.read())
                obj['status'] = job['state']
            except json.JSONDecodeError:
                obj['status'] = f'N{int(datetime.now().timestamp())}'
        status['mirrors'].append(obj)

    status['mirrors'].sort(key=lambda x: x['cname'])
    with open('/srv/http/status.json.root/mirrors/status.json', 'w') as f:
        f.write(json.dumps(status))


def parse_state(state: str) -> Tuple[str, datetime]:
    def next_ts(s: str, i: int):
        for j in range(i, len(s)):
            if not s[j].isdigit():
                return s[i:j - 1]
        return s[i:]
    i = 0
    while i < len(state):
        if state[i] in 'SYFPXNO':
            ts = next_ts(state, i + 1)
            time = datetime.fromtimestamp(int(ts))
            yield state[i], time
            i += len(ts)
        else:
            yield state[i], datetime.fromtimestamp(0)
        i += 1
