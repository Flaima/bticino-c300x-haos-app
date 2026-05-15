# BTicino C300X App 1.0.0

Home Assistant OS add-on for a BTicino/Legrand Classe 300X/C300X firmware that publishes MQTT topics under `Bticino/#`, enables SSH as `root2`, and exposes the internal answering-machine files.

## Required add-on configuration

Set these values after installing the add-on. Empty values must be filled locally in Home Assistant.

```yaml
bticino_host: ""
mqtt_host: ""
mqtt_username: ""
mqtt_password: ""
ssh_username: root2
ssh_password: pwned123
video_mode: ssh_loopback
stream_udp_port: 5007
media_path: /media/bticino/messages
ab_remote_path: /home/bticino/cfg/extra/47/messages
ab_sync_interval: 60
```

## Firmware topics used

The add-on listens to:

```text
Bticino/tx
Bticino/LastWillT
Bticino/status/availability
Bticino/state/#
Bticino/livestream/#
Bticino/answering_machine/#
Bticino/ack/#
```

The add-on sends non-retained JSON commands to:

```text
Bticino/command/main_door/open
Bticino/command/answering_machine/set
Bticino/command/livestream/start
Bticino/command/livestream/stop
Bticino/command/answering_machine/refresh
```

## Livestream

Default mode is `ssh_loopback`. The add-on opens SSH to the C300X, runs `tcpdump` on the loopback interface, captures RTP/H264 from UDP port `5007` or `5002`, converts it with ffmpeg, and publishes MJPEG to Home Assistant.

High profile uses internal UDP port `5007`. Low profile uses `5002`.

## Answering-machine clips

The add-on copies files from the C300X:

```text
/home/bticino/cfg/extra/47/messages/message_*/msg_info.ini
/home/bticino/cfg/extra/47/messages/message_*/aswm.jpg
/home/bticino/cfg/extra/47/messages/message_*/aswm.avi
```

They are stored in Home Assistant media:

```text
/media/bticino/messages/message_*/
```

Home Assistant media paths are generated from the local media folder. The add-on web page also serves the downloaded thumbnails and AVI clips directly.
