# syntax=docker/dockerfile:1.7
#
# Build the Java SDK (JNI). Build flow mirrors `.github/workflows/ci-java.yml`:
#   1. cargo build --release -p zerobus-jni --features test-helpers
#   2. stage libzerobus_jni.so → src/main/resources/native/linux-x86_64/
#   3. mvn spotless:check
#   4. mvn clean package -DskipTests
#
# Tests run at container runtime (entrypoint), not at build time. Running
# `mvn test` during `docker build` would NOT see the 5 Databricks env vars
# (they only flow through compose's runtime `environment:`), so every
# integration test in IntegrationTest / ArrowIntegrationTest would silently
# skip via the `assumeTrue(configAvailable, ...)` gates — the build would
# pass while writing no data to Databricks. Runtime execution fixes this
# AND keeps creds out of image layers.
#
FROM maven:3.9-eclipse-temurin-11

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl build-essential pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /work
COPY rust/ ./rust/
COPY java/ ./java/
COPY LICENSE ./LICENSE

# Step 1: build JNI native library (test-helpers feature is required by
# integration tests; harmless when tests are skipped).
WORKDIR /work/rust
RUN cargo build --release -p zerobus-jni --features test-helpers

# Step 2: stage the .so where Maven's resources plugin will pick it up
# and bundle it into the JAR (matches NativeLoader's lookup path).
WORKDIR /work/java
RUN mkdir -p src/main/resources/native/linux-x86_64 && \
    cp -v ../rust/target/release/libzerobus_jni.so \
          src/main/resources/native/linux-x86_64/

# Step 3: enforce formatting (Google Java Format via Spotless).
RUN mvn -B spotless:check

# Step 4: build (no tests). Produces target/zerobus-ingest-sdk-*.jar.
# Tests run at runtime in the entrypoint below.
RUN mvn -B clean package -DskipTests

RUN mkdir -p /artifacts && \
    cp -v target/zerobus-ingest-sdk-*.jar /artifacts/

RUN cat > /entrypoint.sh <<'SH'
#!/bin/sh
set -e
cd /work/java
if [ "${RUN_TESTS:-0}" = "1" ]; then
    echo ">> Running java tests (incl. live integration against Databricks)"
    # Surface which env vars actually reached the test suite — helps
    # diagnose silent skips when one of the 5 vars is missing/empty.
    for v in ZEROBUS_SERVER_ENDPOINT DATABRICKS_WORKSPACE_URL \
             ZEROBUS_TABLE_NAME DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET; do
        eval "val=\$$v"
        if [ -n "$val" ]; then
            echo "   $v: set"
        else
            echo "   $v: <unset> — integration tests will be skipped"
        fi
    done
    mvn -B test
fi
rm -rf /output/* 2>/dev/null || true
mkdir -p /output
cp -rv /artifacts/. /output/
if [ -n "${HOST_UID:-}" ] && [ -n "${HOST_GID:-}" ]; then
    chown -R "${HOST_UID}:${HOST_GID}" /output
fi
ls -la /output/
echo "✓ java artifacts copied to /output"
SH
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
