FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY go.mod ./

# Download all dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o proxy-service .

# Use a smaller image for the final application
FROM alpine:latest  

WORKDIR /root/

# Copy the binary from builder
COPY --from=builder /app/proxy-service .

RUN apk --no-cache add ca-certificates
RUN apk --no-cache add tzdata
RUN apk add --no-cache jq

# Expose the application port
EXPOSE 8000

# Command to run the executable
CMD ["./proxy-service"]