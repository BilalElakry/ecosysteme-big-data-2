FROM golang:1.25-bookworm AS build
ENV CGO_ENABLED=0
WORKDIR /src
RUN mkdir -p /out
RUN git clone --depth 1 --branch RELEASE.2025-10-15T17-29-55Z https://github.com/minio/minio.git server \
    && cd server \
    && go build -p 2 -trimpath -o /out/minio .
RUN git clone --depth 1 --branch RELEASE.2025-08-13T08-35-41Z https://github.com/minio/mc.git client \
    && cd client \
    && go build -p 2 -trimpath -o /out/mc .

FROM debian:bookworm-slim
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/minio /out/mc /usr/local/bin/
RUN minio --version && mc --version
ENTRYPOINT ["minio"]
