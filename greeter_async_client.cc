/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <grpcpp/grpcpp.h>

#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include "helloworld.grpc.pb.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientAsyncReaderInterface;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using helloworld::Greeter;
using helloworld::HelloReply;
using helloworld::HelloRequest;

class GreeterClient {
public:
    explicit GreeterClient(std::shared_ptr<Channel> channel)
        : stub_(Greeter::NewStub(channel)) {}

    // Loop while listening for completed responses.
    // Prints out the response from the server.
    void AsyncCompleteRpc() {
        void* got_tag;
        bool ok = false;

        while (cq_.Next(&got_tag, &ok)) {
            std::cout << "cq_.Next() returned " << ok << std::endl;

            // The tag in this example is the memory location of the call object
            ResponseHandler* responseHandler = static_cast<ResponseHandler*>(got_tag);
            std::cout << "Tag received: " << responseHandler << std::endl;

            // Verify that the request was completed successfully. Note that "ok"
            // corresponds solely to the request for updates introduced by Finish().
            std::cout << "Next returned: " << ok << std::endl;
            if (responseHandler != nullptr)
            {
                responseHandler->HandleResponse(ok);
            }
        }
    }

    void createStreamRequest() {
        new AsyncClientCall(*this);
        new AsyncStreamCall(*this);
    }

    void createRequest() {
        new UnarySayHello(*this);
    }
private:

    class ResponseHandler {
    public:
        virtual bool HandleResponse(bool eventStatus) = 0;
    };

    // struct for keeping state and data information
    class UnarySayHello: public ResponseHandler {
    public:

        UnarySayHello(GreeterClient& parent): parent_(parent) {
            HelloRequest request;

            std::string user = "kyle";
            request.set_name(user);

            // Initiate the async request.
            rpc_ = parent_.stub_->AsyncSayHello(&context_, request, &parent.cq_);
            // Add the operation to the queue, so we get notified when
            // the request is completed.
            // Note that we use the instance address as tag. We don't really
            // need the handle in this unary call, but the server implementation
            // need's to iterate over a Handle to deal with the other request
            // classes
            rpc_->Finish(&reply_, &status_, this);
        }

        bool HandleResponse(bool responseStatus) override {
            std::cout << "start of HandleResponse()" << std::endl;
            if (status_.ok()) {
                std::cout << "response:" << reply_.message() << std::endl;
            } else {
                std::cout << "gRPC request failed" << std::endl;
            }

            delete this;

            return true;
        }

        // Container for the data we expect from the server.
        HelloReply reply_;
        // the server and/or tweak certain RPC behaviors.
        ClientContext context_;
        std::unique_ptr<ClientAsyncResponseReader<HelloReply>> rpc_;
        GreeterClient& parent_;
        // Storage for the status of the RPC upon completion.
        Status status_;
    };

    // struct for keeping state and data information
    class AsyncClientCall: public ResponseHandler {
        enum CallStatus {CREATE, PROCESS, FINISH};
        CallStatus callStatus_;
    public:

        AsyncClientCall(GreeterClient& parent): parent_(parent), callStatus_(CREATE) {
            std::cout << "AsyncClientCall constructor!!!" << std::endl;
            HelloRequest request;
            request.set_name("kyle");
            // Initiate the async request.
            // Note that this time, we have to supply the tag to the gRPC
            // initiation method. That's because we will get an event that the
            // request is in progress before we should (can?) start reading
            // the replies.
            response_reader = parent_.stub_->AsyncSayHelloSvrStreamReply(&context_, request, &parent_.cq_, (void*)this);
        }

        bool HandleResponse(bool responseStatus) override {
            if (responseStatus == true) {
                switch (callStatus_) {
                case CREATE:
                    std::cout << "async CREATE in HandleResponse()!!!" << std::endl;
                    if (responseStatus) {
                        response_reader->Read(&reply_, (void*)this);
                        std::cout << "async Greeter received: " << this << " : " << reply_.message() << std::endl;
                        callStatus_ = PROCESS;
                    } else {
                        response_reader->Finish(&status, (void*)this);
                        callStatus_ = FINISH;
                    }
                    break;
                case PROCESS:
                std::cout << "async PROCESS in HandleResponse()!!!" << std::endl;
                    if (responseStatus) {
                        std::cout << "async Greeter received: " << this << " : " << reply_.message() << std::endl;
                        response_reader->Read(&reply_, (void*)this);
                    } else {
                        response_reader->Finish(&status, (void*)this);
                        callStatus_ = FINISH;
                    }
                    break;
                case FINISH:
                    if (status.ok()) {
                        std::cout << "Server Response Completed: " << this << " CallData: " << this << std::endl;
                    }
                    else {
                        std::cout << "RPC failed" << std::endl;
                    }
                    delete this;
                }
            }
            else
            {
                std::cout << "async connection lost!!!" << std::endl;
            }

            return true;
        }

        GreeterClient& parent_;
        // Context for the client. It could be used to convey extra information to
        // the server and/or tweak certain RPC behaviors.
        ClientContext context_;

        // Container for the data we expect from the server.
        HelloReply reply_;

        // Storage for the status of the RPC upon completion.
        Status status;

        //std::unique_ptr<ClientAsyncResponseReader<HelloReply>> response_reader;
        std::unique_ptr<ClientAsyncReaderInterface<HelloReply>> response_reader;
    };

    class AsyncStreamCall: public ResponseHandler {
        enum CallStatus {CREATE, PROCESS, FINISH};
        CallStatus callStatus_;
        public:
    
            AsyncStreamCall(GreeterClient& parent): parent_(parent), callStatus_(CREATE) {
                std::cout << "AsyncStreamCall constructor!!!" << std::endl;
                HelloRequest request;
                request.set_name("kyle");
                // Initiate the async request.
                // Note that this time, we have to supply the tag to the gRPC
                // initiation method. That's because we will get an event that the
                // request is in progress before we should (can?) start reading
                // the replies.
                // Note: In bidirectional streaming, you don’t pass the request when initiating the call — the client sends requests later using Write() or WriteAndFinish().
                readerwriter_ = parent_.stub_->AsyncSayHelloBidiStreamReply(&context_, &parent_.cq_, (void*)this);
            }

            ~AsyncStreamCall() {
                if (writerThread_.joinable()) {
                    isWriting_ = false;
                    writerThread_.join();
                }
            }
        
            bool HandleResponse(bool responseStatus) override {
                if (responseStatus == true) {
                    switch (callStatus_) {
                    case CREATE:
                        std::cout << "async CREATE in HandleResponse()!!!" << std::endl;
                        if (responseStatus) {
                            readerwriter_->Read(&reply_, (void*)this);
                            std::cout << "async Greeter received: " << this << " : " << reply_.message() << std::endl;
                            callStatus_ = PROCESS;
                        } else {
                            readerwriter_->Finish(&status, (void*)this);
                            callStatus_ = FINISH;
                        }

                        // Start writing thread after stream creation - 클라이언트 스트림 테스트용
                        writerThread_ = std::thread([this]() {
                            int count = 0;
                            while (isWriting_) {
                                HelloRequest req;
                                req.Clear();
                                req.set_name("client message #" + std::to_string(count++));
                                std::this_thread::sleep_for(std::chrono::seconds(3));
                                std::cout << "<<<<<<<<<<<<<<<<<<<< Client sending message: " << req.name() << std::endl;
                                readerwriter_->Write(req, (void*)nullptr);  // Send message
                            }
                        });

                        break;
                    case PROCESS:
                    std::cout << "async PROCESS in HandleResponse()!!!" << std::endl;
                        if (responseStatus) {
                            readerwriter_->Read(&reply_, (void*)this);
                            std::cout << "async Greeter received: " << this << " : " << reply_.message() << std::endl;
                        } else {
                            readerwriter_->Finish(&status, (void*)this);
                            callStatus_ = FINISH;
                        }
                        break;
                    case FINISH:
                        if (status.ok()) {
                            std::cout << "Server Response Completed: " << this << " CallData: " << this << std::endl;
                        }
                        else {
                            std::cout << "RPC failed" << std::endl;
                        }
                        delete this;
                    }
                }
                else
                {
                    std::cout << "async connection lost!!!" << std::endl;
                }
    
                return true;
            }
    
            GreeterClient& parent_;
            // Context for the client. It could be used to convey extra information to
            // the server and/or tweak certain RPC behaviors.
            ClientContext context_;
    
            // Container for the data we expect from the server.
            HelloReply reply_;
    
            // Storage for the status of the RPC upon completion.
            Status status;
    
            //std::unique_ptr<ClientAsyncResponseReader<HelloReply>> response_reader;
            std::unique_ptr<grpc::ClientAsyncReaderWriter<HelloRequest, HelloReply>> readerwriter_;

            std::thread writerThread_;
            bool isWriting_ = true;
    };

    // Out of the passed in Channel comes the stub, stored here, our view of the
    // server's exposed services.
    std::unique_ptr<Greeter::Stub> stub_;

    // The producer-consumer queue we use to communicate asynchronously with the
    // gRPC runtime.
    CompletionQueue cq_;
};

int main(int argc, char** argv) {
    // Instantiate the client. It requires a channel, out of which the actual RPCs
    // are created. This channel models a connection to an endpoint (in this case,
    // localhost at port 50051). We indicate that the channel isn't authenticated
    // (use of InsecureChannelCredentials()).
    GreeterClient greeter(grpc::CreateChannel(
                              "localhost:50051", grpc::InsecureChannelCredentials()));

    greeter.createStreamRequest();

    // Spawn reader thread that loops indefinitely
    std::thread thread_ = std::thread(&GreeterClient::AsyncCompleteRpc, &greeter);
    while(true)
    {
        greeter.createRequest();
        sleep(1);
    }

    std::cout << "Press control-c to quit" << std::endl << std::endl;
    thread_.join();  //blocks forever

    return 0;
}
