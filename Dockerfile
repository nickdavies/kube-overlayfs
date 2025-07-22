FROM rust:1.88-slim as builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY bin/ ./bin/
COPY lib/ ./lib/

RUN cargo build --release --bin overlay-mount

FROM alpine

FROM alpine:latest
WORKDIR /app
RUN apk update \
    && apk add openssl ca-certificates

COPY --from=builder /app/target/release/overlay-mount /app/overlay-mount

ENTRYPOINT ["/app/overlay-mount"]
