FROM rust:1.88-slim AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY bin/ ./bin/
COPY lib/ ./lib/

RUN cargo build --release --bin overlay-mount

FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/overlay-mount /app/overlay-mount

ENTRYPOINT ["/app/overlay-mount"]
