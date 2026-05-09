# syntax=docker/dockerfile:1.7
#
# Build the TypeScript SDK (NAPI-RS) → native .node addon + JS/TS entry points.
# typescript/Cargo.toml uses the published Rust SDK by default; we patch it
# to use the local `rust/sdk` source so changes anywhere in the repo are
# exercised end-to-end (mirrors `use_local_sdk: true` in CI).
#
FROM node:20-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler curl build-essential pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"

RUN rustup component add clippy

WORKDIR /work
COPY rust/ ./rust/
COPY typescript/ ./typescript/
COPY LICENSE ./LICENSE

WORKDIR /work/typescript

# Pin the wrapper to the local Rust SDK source. Drop Cargo.lock so cargo
# re-resolves the dependency graph against the patched source path
# (the tree-tracked Cargo.lock pins the published crates.io version, which
# would otherwise win even when [patch.crates-io] is added).
RUN printf '\n[patch.crates-io]\ndatabricks-zerobus-ingest-sdk = { path = "../rust/sdk" }\n' \
        >> Cargo.toml && \
    rm -f Cargo.lock

RUN npm ci
RUN npm run build

# Note: `cargo clippy --all-targets --all-features` is not run here. The
# repo's CI runs clippy against whatever Rust version is current on the
# CI runners, which may differ from this image's pinned toolchain. The
# `napi build` above already invokes `cargo build`, which catches real
# compilation errors. Lint drift is a separate concern handled by CI.

RUN mkdir -p /artifacts && \
    cp -v *.node      /artifacts/ && \
    cp -v index.js    /artifacts/ && \
    cp -v index.d.ts  /artifacts/

# Tests run at container runtime so the 5 Databricks env vars passed
# through compose's `environment:` reach the integration test suite.
# Running them at build time would silently skip every integration test
# (the suite gates on `process.env.DATABRICKS_*` and falls through to
# `SKIP_INTEGRATION = true` when those are unset at JVM start).
RUN cat > /entrypoint.sh <<'SH'
#!/bin/sh
set -e
cd /work/typescript
if [ "${RUN_TESTS:-0}" = "1" ]; then
    echo ">> Running typescript tests (unit + integration)"
    npm test
fi
rm -rf /output/* 2>/dev/null || true
mkdir -p /output
cp -rv /artifacts/. /output/
if [ -n "${HOST_UID:-}" ] && [ -n "${HOST_GID:-}" ]; then
    chown -R "${HOST_UID}:${HOST_GID}" /output
fi
ls -la /output/
echo "✓ typescript artifacts copied to /output"
SH
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
