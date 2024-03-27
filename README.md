# What Sticks 11 Apple Service (WS11AS)
![What Sticks Logo](/docs/images/wsLogo180.png)
## Overview

What Sticks 11 Apple Service (WS11AS) is a comprehensive data processing system designed to receive user data from the WSiOS application, store it in the What Sticks Database, and create `.json` and `.pkl` files for the What Sticks Platform. The key feature of WS11AS is its queuing mechanism, which efficiently manages data processing tasks.

This service uses two main scripts, `worker_script.py` and `send_job.py`, running on an Ubuntu server to handle and queue data processing tasks. The worker script continuously listens for job requests and processes them using the `apple_health_service.what_sticks_health_service` module.

## How it works
The key feature is the entry point to kick off a job in the queue. WSAPI uses this code:
```py
path_sub = os.path.join(current_app.config.get('APPLE_SERVICE_11_ROOT'), 'send_job.py')
process = subprocess.Popen(['python', path_sub, user_id_string, time_stamp_str_for_json_file_name, 'True', 'True'])

```

ws_api_endpoint.py is just an example of how to use WS11AS if you were going to test the process by kicking off jobs manually froma terminal. 

For testing I used multiple terminals. I used one to run worker_script.py. Then I had at least one more but sometimes two more that ran these commands to kick off multiple jobs to test the process was queueing correctly.
```
from ws_api_endpoint import call_send_job
call_send_job(1,"20240104-1225","True","False")
```


## Installation and Setup

### Requirements

- Ubuntu Server
- Python 3.x
- Access to the What Sticks Database
- Network connection between the server and WSiOS application

### Setting Up the Worker Script

1. Clone the repository to your Ubuntu server.
2. Ensure Python 3.x is installed.
3. Set up a Python virtual environment and install required dependencies.

### Configuring the Systemd Service

To ensure `worker_script.py` runs continuously in the background and starts automatically on boot, set it up as a systemd service using the provided `WhatSticks11AppleService.service` file.

Here's the service file configuration:

```bash
[Unit]
Description= Serve What Sticks 11 Apple Service (queueing) Production.
After=network.target

[Service]
User=nick
Environment=PATH=/home/nick/environments/ws11as/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin
Environment="WS_CONFIG_TYPE=prod"
ExecStart=/home/nick/environments/ws11as/bin/python /home/nick/applications/WhatSticks11AppleService/worker_script.py --serve-in-foreground

[Install]
WantedBy=multi-user.target
```

### More notes
No correlations are calculated in Apple Service. This is solely to add data to the database and create unadulterated dataframes that reflect user data so that we lessen the strain on the database by making it easier to pull raw user data from pickle files.

For correlation calculations see What Sticks 11 Analysis Package (ws_analysis).