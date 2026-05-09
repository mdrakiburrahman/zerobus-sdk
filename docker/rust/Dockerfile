# syntax=docker/dockerfile:1.7
#
# Build the Rust SDK (workspace: sdk + ffi + jni + examples + tests + tools)
# and stage the FFI/JNI native libs + cbindgen header for downstream SDKs.
#
FROM rust:1-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

RUN rustup component add rustfmt clippy

WORKDIR /work
COPY rust/ ./rust/
COPY LICENSE ./LICENSE

WORKDIR /work/rust

RUN make lint
RUN make build-release
RUN make build-ffi
RUN make build-jni

# Stage the artifacts that downstream SDKs / consumers care about.
RUN mkdir -p /artifacts && \
    cp -v target/release/libzerobus_ffi.a   /artifacts/ && \
    cp -v target/release/libzerobus_ffi.so  /artifacts/ 2>/dev/null || true && \
    cp -v target/release/libzerobus_jni.so  /artifacts/ && \
    cp -v ffi/zerobus.h                     /artifacts/

# Tests run at container runtime (not build time) so compose's
# `environment:` reaches them and creds never enter image layers.
RUN cat > /entrypoint.sh <<'SH'
#!/bin/sh
set -e
cd /work/rust
if [ "${RUN_TESTS:-0}" = "1" ]; then
    echo ">> Running rust tests (mock-based, no creds needed)"
    make test
fi
rm -rf /output/* 2>/dev/null || true
mkdir -p /output
cp -rv /artifacts/. /output/
if [ -n "${HOST_UID:-}" ] && [ -n "${HOST_GID:-}" ]; then
    chown -R "${HOST_UID}:${HOST_GID}" /output
fi
ls -la /output/
echo "✓ rust artifacts copied to /output"
SH
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
