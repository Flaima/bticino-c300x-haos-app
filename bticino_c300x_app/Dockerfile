FROM alpine:3.20

ARG BUILD_VERSION
ARG BUILD_ARCH

LABEL   io.hass.version="${BUILD_VERSION}"   io.hass.type="app"   io.hass.arch="${BUILD_ARCH}"

SHELL ["/bin/sh", "-o", "pipefail", "-c"]

RUN apk add --no-cache     python3     py3-pip     py3-paramiko     ffmpeg     tcpdump

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN python3 -m pip install --no-cache-dir --break-system-packages -r /app/requirements.txt

COPY rootfs/ /

RUN chmod +x /app/run.sh

CMD ["/app/run.sh"]
