FROM golang:1.16 as builder

RUN apt-get -y update && \
    apt-get -y install zip jq libxml2

 # Copy local code to the container image.
WORKDIR /app

 # Retrieve application dependencies using go modules.
 # Allows container builds to reuse downloaded dependencies.
COPY go.* ./
RUN go mod download

 # Copy local code to the container image.
COPY . ./
 # current working dir is /app 

#  RUN cd $GOPATH/pkg/mod/github.com/ibmdb/go_ibm_db@v0.4.1/installer && go run setup.go
#  ENV DB2HOME=$GOPATH/pkg/mod/github.com/ibmdb/clidriver 
#  ENV CGO_CFLAGS=-I$DB2HOME/include CGO_LDFLAGS=-L$DB2HOME/lib LD_LIBRARY_PATH=$GOPATH/pkg/mod/github.com/ibmdb/clidriver/lib
#  RUN cd /app

RUN CGO_ENABLED=0 GOOS=linux go build -mod=readonly  -v -o scraper

 # Use a Docker multi-stage build to create a lean production image.
 # https://docs.docker.com/develop/develop-images/multistage-build/#use-multi-stage-builds
FROM alpine:3
RUN apk add --no-cache ca-certificates

 # Copy the binary to the production image from the builder stage.
COPY --from=builder /app/scraper /scraper

 # Run the web service on container startup.
CMD ["/scraper"]