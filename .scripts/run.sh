#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# .scripts/run.sh — build every Zerobus SDK in Docker.
#
# All you need on the host is Docker. Source is COPYd into per-SDK images;
# built artifacts land in mount/<sdk>/.
#
# Env vars:
#   SDK         — comma-separated subset (rust|python|typescript|java|go|all)
#                 default: all
#   RUN_TESTS   — auto-derived from .env presence; do not set manually
# ---------------------------------------------------------------------------
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

ALL_SDKS=(rust python typescript java go)
COMPOSE_FILE="$REPO_ROOT/docker/docker-compose.build.yml"

# ---------------------------------------------------------------------------
# Resolve which SDKs to build
# ---------------------------------------------------------------------------
SDK="${SDK:-all}"
if [[ "$SDK" == "all" ]]; then
  SDKS=("${ALL_SDKS[@]}")
else
  IFS=',' read -r -a SDKS <<< "$SDK"
fi

VALID="rust python typescript java go"
for sdk in "${SDKS[@]}"; do
  if [[ ! " $VALID " == *" $sdk "* ]]; then
    echo "ERROR: unknown SDK '$sdk' (valid: $VALID, all)" >&2
    exit 1
  fi
done

# ---------------------------------------------------------------------------
# .env detection — gates whether tests run inside the containers
# ---------------------------------------------------------------------------
if [[ -f "$REPO_ROOT/.env" ]]; then
  echo "✓ .env found at repo root — tests will run inside containers"
  set -a
  # shellcheck disable=SC1091
  source "$REPO_ROOT/.env"
  set +a
  export RUN_TESTS=1
else
  cat >&2 <<'WARN'

⚠  .env not found at repo root — skipping tests.
   Build + lint will still run for every SDK.

   To enable the test suites (Java + TypeScript integration tests need
   real Databricks credentials; Rust/Python/Go are mock-based):

       cp .env.example .env
       # then edit .env with your service principal credentials

WARN
  export RUN_TESTS=0
fi

# ---------------------------------------------------------------------------
# Pre-create bind-mount targets so Docker doesn't create them as root
# ---------------------------------------------------------------------------
export HOST_UID="$(id -u)"
export HOST_GID="$(id -g)"
for sdk in "${SDKS[@]}"; do
  mkdir -p "$REPO_ROOT/mount/$sdk"
done

# ---------------------------------------------------------------------------
# Map SDK list → service list
# ---------------------------------------------------------------------------
SERVICES=()
for sdk in "${SDKS[@]}"; do
  SERVICES+=("build-$sdk")
done

cleanup() {
  docker compose -f "$COMPOSE_FILE" down --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Build images. `docker compose build` is parallel-by-default in Compose v2 —
# all services build concurrently, with BuildKit interleaving steps in its
# multi-line progress UI. `--parallel` is implicit and idempotent here.
# Any SDK build failure (lint / build / tests) propagates via `set -e`.
# ---------------------------------------------------------------------------
echo ""
echo "=== Building Docker images (parallel) for: ${SDKS[*]} ==="
docker compose -f "$COMPOSE_FILE" build "${SERVICES[@]}"

# ---------------------------------------------------------------------------
# Run each container to extract artifacts to mount/<sdk>/. We fan all of
# them out in parallel and capture per-service logs to temp files, then
# replay each contiguously (preserving readable per-SDK output even though
# execution overlapped). Total wall time = max(extract time across SDKs).
# ---------------------------------------------------------------------------
echo ""
echo "=== Extracting artifacts (parallel) ==="

# Pre-create the named network so parallel `docker compose run` invocations
# below don't all race to create it.
docker network inspect zerobus-sdk-build >/dev/null 2>&1 \
  || docker network create zerobus-sdk-build >/dev/null

declare -a JOBS=()
declare -a LOGS=()
for sdk in "${SDKS[@]}"; do
  log="$(mktemp -t "zerobus-build-$sdk.XXXXXX.log")"
  LOGS+=("$log")
  (
    docker compose -f "$COMPOSE_FILE" run --rm --no-deps "build-$sdk" \
      >"$log" 2>&1
  ) &
  JOBS+=("$!")
done

FAILED=()
for i in "${!JOBS[@]}"; do
  sdk="${SDKS[$i]}"
  pid="${JOBS[$i]}"
  log="${LOGS[$i]}"
  echo ""
  echo "--- build-$sdk ---"
  if wait "$pid"; then
    cat "$log"
  else
    cat "$log"
    FAILED+=("$sdk")
  fi
  rm -f "$log"
done

if [[ "${#FAILED[@]}" -gt 0 ]]; then
  echo ""
  echo "✗ FAILED: ${FAILED[*]}" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "=== Build summary ==="
for sdk in "${SDKS[@]}"; do
  dest="$REPO_ROOT/mount/$sdk"
  if [[ -d "$dest" ]] && [[ -n "$(ls -A "$dest" 2>/dev/null)" ]]; then
    echo "  ✓ $sdk → mount/$sdk/"
    find "$dest" -maxdepth 3 -type f -printf '      %P\n' | sort
  else
    echo "  ✗ $sdk → mount/$sdk/ (empty)"
  fi
done

if [[ "$RUN_TESTS" == "1" ]]; then
  echo ""
  echo "  Tests: ENABLED (.env was present)"
else
  echo ""
  echo "  Tests: SKIPPED (no .env)"
fi
