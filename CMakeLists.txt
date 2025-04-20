cmake_minimum_required(VERSION 3.15)

project(grpc_async_helloworld C CXX)

# Use C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)

# gRPC and protobuf
find_package(gRPC REQUIRED)
find_package(Protobuf REQUIRED)

include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Source files
set(PROTO_SRC
    helloworld.pb.cc
    helloworld.grpc.pb.cc
)

set(CLIENT_SRC
    greeter_async_client.cc
)

set(SERVER_SRC
    greeter_async_server.cc
)

# Add the client executable
add_executable(greeter_async_client ${CLIENT_SRC} ${PROTO_SRC})
target_link_libraries(greeter_async_client
    gRPC::grpc++
    ${Protobuf_LIBRARIES}
)

# Add the server executable
add_executable(greeter_async_server ${SERVER_SRC} ${PROTO_SRC})
target_link_libraries(greeter_async_server
    gRPC::grpc++
    ${Protobuf_LIBRARIES}
    absl_strings
)

# Optional: organize build output
set_target_properties(greeter_async_client PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set_target_properties(greeter_async_server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

