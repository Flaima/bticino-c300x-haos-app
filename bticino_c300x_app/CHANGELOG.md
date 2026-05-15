# Changelog

## 1.0.0

- Sanitized public release without real IP addresses, personal maintainer names, e-mail addresses, confidential credentials or real passwords.
- Matched to the custom BTicino C300X firmware command topics under `Bticino/command/#`.
- Includes MQTT Discovery entities for doorbell, door opener, answering-machine switch, app-streaming sensor and live camera.
- Starts the livestream with the firmware loopback frames for high (`5007`) and low (`5002`) profiles.
- Captures BTicino loopback RTP/H264 via SSH/tcpdump and converts it to MJPEG for Home Assistant.
- Adds answering-machine clip sync via SSH/SFTP from `/home/bticino/cfg/extra/47/messages` to `/media/bticino/messages`.
