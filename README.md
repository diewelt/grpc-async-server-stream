# grpc-async-server-stream

protoc -I .   --cpp_out=.   --grpc_out=.   --plugin=protoc-gen-grpc=`which grpc_cpp_plugin`   helloworld.proto
build/bin/greeter_async_server --thread=1 --cq=2 --pool=1 --port=50051
