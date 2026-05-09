# syntax=docker/dockerfile:1.7
#
# Build the Python SDK (PyO3 + maturin) into a release wheel.
# python/rust/Cargo.toml already pins `path = "../../rust/sdk"` via
# [patch.crates-io], so this build always uses the local Rust SDK source.
#
FROM python:3.12-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler curl build-essential pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /work
COPY rust/ ./rust/
COPY python/ ./python/
COPY LICENSE ./LICENSE

WORKDIR /work/python

# maturin requires a sibling LICENSE — the in-tree Makefile already does
# this, but COPY may not preserve a pre-existing copy.
RUN cp ../LICENSE LICENSE

# `make dev` creates .venv, installs requirements-dev.lock (incl. maturin,
# pytest, black, etc.), and runs `maturin develop` to build the Rust
# extension in-place — needed for both lint and test steps.
RUN make dev

RUN make lint

# Build release wheel (separate from `maturin develop` artifacts).
RUN make build

RUN mkdir -p /artifacts && \
    cp -v dist/*.whl /artifacts/

# Tests run at container runtime so compose's `environment:` flows through.
RUN cat > /entrypoint.sh <<'SH'
#!/bin/sh
set -e
cd /work/python
if [ "${RUN_TESTS:-0}" = "1" ]; then
    echo ">> Running python tests (mock-based, no creds needed)"
    make test
fi
rm -rf /output/* 2>/dev/null || true
mkdir -p /output
cp -rv /artifacts/. /output/
if [ -n "${HOST_UID:-}" ] && [ -n "${HOST_GID:-}" ]; then
    chown -R "${HOST_UID}:${HOST_GID}" /output
fi
ls -la /output/
echo "✓ python artifacts copied to /output"
SH
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
