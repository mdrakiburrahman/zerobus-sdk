# syntax=docker/dockerfile:1.7
#
# Build the Go SDK. Go is source-distributed so the only "release" artifact
# is the static FFI library that downstream `go get` consumers compile
# against. We also build the example binaries to prove cgo + linking work.
#
FROM golang:1.25-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl build-essential pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"

RUN rustup component add clippy

WORKDIR /work
COPY rust/ ./rust/
COPY go/ ./go/
COPY LICENSE ./LICENSE

WORKDIR /work/go

# Builds rust FFI (via build_rust.sh) → lib/linux_amd64/libzerobus_ffi.a,
# then `go build -v` against it.
RUN make build

RUN make lint

# Examples are pure-Go binaries that link the FFI lib — proves the
# end-to-end build works.
RUN make examples

RUN mkdir -p /artifacts/lib/linux_amd64 /artifacts/examples && \
    cp -v lib/linux_amd64/libzerobus_ffi.a /artifacts/lib/linux_amd64/ && \
    find examples -maxdepth 3 -type f -executable \
        ! -name "*.go" ! -name "*.sh" \
        -exec cp -v {} /artifacts/examples/ \; 2>/dev/null || true

RUN cat > /entrypoint.sh <<'SH'
#!/bin/sh
set -e
cd /work/go
if [ "${RUN_TESTS:-0}" = "1" ]; then
    echo ">> Running go tests (mock-based, no creds needed)"
    make test
fi
rm -rf /output/* 2>/dev/null || true
mkdir -p /output
cp -rv /artifacts/. /output/
if [ -n "${HOST_UID:-}" ] && [ -n "${HOST_GID:-}" ]; then
    chown -R "${HOST_UID}:${HOST_GID}" /output
fi
ls -la /output/
echo "✓ go artifacts copied to /output"
SH
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
