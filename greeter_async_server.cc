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

#include <memory>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <random>
#include <atomic>
#include <condition_variable>
#include <queue>

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>

#include "helloworld.grpc.pb.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerAsyncWriter;
using grpc::ServerCompletionQueue;
using grpc::Status;
using grpc::StatusCode;
using helloworld::HelloRequest;
using helloworld::HelloReply;
using helloworld::Greeter;

int g_thread_num = 1;
int g_cq_num = 1;
int g_pool = 1;
int g_port = 50051;

std::atomic<void*>**    g_instance_pool = nullptr;

std::mutex g_event_mutex;
std::condition_variable g_event_cv;
std::queue<std::string> g_event_queue;  // this can hold the event data

class CallDataBase {
    public:
        CallDataBase(Greeter::AsyncService* service, ServerCompletionQueue* cq) : service_(service), cq_(cq){
        }

        virtual void Proceed(bool ok) = 0;

    protected:
        // The means of communication with the gRPC runtime for an asynchronous
        // server.
        Greeter::AsyncService* service_;
        // Producer-consumer queue where for asynchronous server notifications.
        ServerCompletionQueue* cq_;

        // Context for the rpc, allowing to tweak aspects of it such as the use
        // of compression, authentication, as well as to send metadata back to
        // the client.
        ServerContext ctx_;

        // What we get from the client.
        HelloRequest request_;
        // What we send back to the client.
        HelloReply reply_;
};

class CallDataUnary : public CallDataBase {
    public:
        // Take in the "service" instance (in this case representing an asynchronous
        // server) and the completion queue "cq" used for asynchronous communication
        // with the gRPC runtime.
        CallDataUnary(Greeter::AsyncService* service, ServerCompletionQueue* cq) : CallDataBase(service,cq),responder_(&ctx_), status_(CREATE) {
            // Invoke the serving logic right away.

            // As part of the initial CREATE state, we *request* that the system
            // start processing SayHello requests. In this request, "this" acts are
            // the tag uniquely identifying the request (so that different CallDataUnary
            // instances can serve different requests concurrently), in this case
            // the memory address of this CallDataUnary instance.

            status_ = PROCESS;
            //service_->RequestSayHello(&ctx_, &request_, &responder_, cq_, cq_, (void*)this);
        }

        virtual void SpawnInstance(Greeter::AsyncService* service, ServerCompletionQueue* cq) = 0;

        void Proceed(bool ok) {
            //std::cout << "ok value:" << ok << std::endl;
            static int counter = 0;

            if (status_ == PROCESS) {
                // Spawn a new CallDataUnary instance to serve new clients while we process
                // the one for this CallDataUnary. The instance will deallocate itself as
                // part of its FINISH state.

                //new CallDataUnary(service_, cq_);
                this->SpawnInstance(service_, cq_);

                // The actual processing.
                std::string prefix("Hello ");
                reply_.set_message(prefix + request_.name());

                std::cout << "req name:" << request_.name() << std::endl;

                //std::this_thread::sleep_for(std::chrono::seconds(3));

                // And we are done! Let the gRPC runtime know we've finished, using the
                // memory address of this instance as the uniquely identifying tag for
                // the event.
                status_ = FINISH;
                responder_.Finish(reply_, Status::OK, (void*)this);
                //std::cout << "finish called," << counter++ << std::endl;
            } else {
                if (status_ != FINISH) {
                std::cout << "wrong status:" << status_ << std::endl;
                assert(false);
            }
            // Once in the FINISH state, deallocate ourselves (CallDataUnary).
            delete this;
            //std::cout << "delete called," << counter++ << std::endl;
        }
    }

    protected:
        // The means to get back to the client.
        ServerAsyncResponseWriter<HelloReply> responder_;

        // Let's implement a tiny state machine with the following states.
        enum CallStatus { CREATE, PROCESS, FINISH };
        CallStatus status_;  // The current serving state.
};


class CallDataSayHello : public CallDataUnary {
    public:

    CallDataSayHello(Greeter::AsyncService* service, ServerCompletionQueue* cq) : CallDataUnary(service, cq) {
        service_->RequestSayHello(&ctx_, &request_, &responder_, cq_, cq_, (void*)this);
    }

    virtual void SpawnInstance(Greeter::AsyncService* service, ServerCompletionQueue* cq) override {
        new CallDataSayHello(service, cq);
    }

};

class CallDataSvrStream : CallDataBase {
    public:

        // Take in the "service" instance (in this case representing an asynchronous
        // server) and the completion queue "cq" used for asynchronous communication
        // with the gRPC runtime.
        CallDataSvrStream(Greeter::AsyncService* service, ServerCompletionQueue* cq) : CallDataBase(service,cq), writer_(&ctx_), status_(SvrStreamStatus::START){
            // Invoke the serving logic right away.

            ctx_.AsyncNotifyWhenDone((void*)this);
            service_->RequestSayHelloStreamReply(&ctx_, &request_, &writer_, cq_, cq_, (void*)this);

            //std::thread* _t(new std::thread(&CallDataSvrStream::threadFunc,this));
            //_t->detach();
        }

        void Proceed(bool ok) override {
            std::unique_lock<std::mutex> lock(m_mutex);

            switch (status_) {
                case SvrStreamStatus::START:
                    new CallDataSvrStream(service_, cq_);
                    StartMessageThread();
                    status_ = SvrStreamStatus::READY;
                    break;
                case SvrStreamStatus::READY:
                    if (!pending_messages_.empty()) {
                        std::string msg = pending_messages_.front();
                        pending_messages_.pop();

                        HelloReply reply;
                        reply.set_message("[Event Driven] " + msg);
                        status_ = SvrStreamStatus::WRITING;
                        writer_.Write(reply, (void*)this); // Async write
                    }
                    break;
                case SvrStreamStatus::WRITING:
                    // Previous write completed, go back to READY
                    status_ = SvrStreamStatus::READY;
                    // Check if more messages are pending
                    if (!pending_messages_.empty()) {
                        Proceed(true);  // Recurse to send next
                    }
                    break;
                case SvrStreamStatus::FINISH:
                    writer_.Finish(Status::OK, (void*)this);
                    lock.unlock();
                    delete this;
                    break;
            }
        }
    // public:

    private:
        // The means to get back to the client.
        ServerAsyncWriter<HelloReply>    writer_;

        // Let's implement a tiny state machine with the following states.
        enum class SvrStreamStatus { START, READY, WRITING, FINISH };
        SvrStreamStatus status_;
        std::queue<std::string> pending_messages_;
        std::mutex    m_mutex;

        std::thread msg_thread_;
        std::atomic<bool> stopped_;

        void StartMessageThread() {
            msg_thread_ = std::thread([this]() {
                int wait_count = 0;
                while (!stopped_) {
                    std::this_thread::sleep_for(std::chrono::seconds(5));
                    std::string msg = "External Event #" + std::to_string(wait_count++);

                    {
                        std::lock_guard<std::mutex> lock(m_mutex);
                        pending_messages_.push(msg);
                    }

                    g_event_cv.notify_all();

                    // Notify the gRPC loop via the CQ tag
                    this->Proceed(true); // Wake up CQ thread to handle write
                }
            });
        }
    // private:
};

class ServerImpl final {
    public:
        ~ServerImpl() {
        server_->Shutdown();
        // Always shutdown the completion queue after the server.
        for (const auto& _cq : m_cq)
            _cq->Shutdown();
        }

        // There is no shutdown handling in this code.
        void Run() {
            std::string server_address("0.0.0.0:" + std::to_string(g_port));

            ServerBuilder builder;
            // Listen on the given address without any authentication mechanism.
            builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
            // Register "service_" as the instance through which we'll communicate with
            // clients. In this case it corresponds to an *asynchronous* service.
            builder.RegisterService(&service_);
            // Get hold of the completion queue used for the asynchronous communication
            // with the gRPC runtime.

            for (int i = 0; i < g_cq_num; ++i) {
                //cq_ = builder.AddCompletionQueue();
                m_cq.emplace_back(builder.AddCompletionQueue());
            }

            // Finally assemble the server.
            server_ = builder.BuildAndStart();
            std::cout << "Server listening on " << server_address << std::endl;

            // Proceed to the server's main loop.
            std::vector<std::thread*> _vec_threads;

            for (int i = 0; i < g_thread_num; ++i) {
                int _cq_idx = i % g_cq_num;
                for (int j = 0; j < g_pool; ++j) {
                    new CallDataSayHello(&service_, m_cq[_cq_idx].get());
                    new CallDataSvrStream(&service_, m_cq[_cq_idx].get());
                }

                _vec_threads.emplace_back(new std::thread(&ServerImpl::HandleRpcs, this, _cq_idx));
            }

            std::cout << g_thread_num << " working aysnc threads spawned" << std::endl;

            for (const auto& _t : _vec_threads)
                _t->join();
        }
    // public:

    private:
        // Class encompasing the state and logic needed to serve a request.

        // This can be run in multiple threads if needed.
        void HandleRpcs(int cq_idx) {
            // Spawn a new CallDataUnary instance to serve new clients.
            void* tag;  // uniquely identifies a request.
            bool ok;
            while (true) {
                // Block waiting to read the next event from the completion queue. The
                // event is uniquely identified by its tag, which in this case is the
                // memory address of a CallDataUnary instance.
                // The return value of Next should always be checked. This return value
                // tells us whether there is any kind of event or cq_ is shutting down.
                //GPR_ASSERT(cq_->Next(&tag, &ok));
                GPR_ASSERT(m_cq[cq_idx]->Next(&tag, &ok));

                CallDataBase* _p_ins = (CallDataBase*)tag;
                _p_ins->Proceed(ok);
            }
        }

        std::vector<std::unique_ptr<ServerCompletionQueue>>  m_cq;

        Greeter::AsyncService service_;
        std::unique_ptr<Server> server_;
    // private:
};

const char* ParseCmdPara( char* argv,const char* para) {
    auto p_target = std::strstr(argv,para);
    if (p_target == nullptr) {
        printf("para error argv[%s] should be %s \n",argv,para);
        return nullptr;
    }
    p_target += std::strlen(para);
    return p_target;
}

void SimulateExternalEvent() {
    std::thread([]() {
        int count = 0;
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(5));
            std::string event_data = "External Event #" + std::to_string(count++);

            {
                std::lock_guard<std::mutex> lock(g_event_mutex);
                g_event_queue.push(event_data);
            }

            g_event_cv.notify_all();  // Notify all waiting threads
        }
    }).detach();
}

int main(int argc, char** argv) {
    if (argc != 5) {
        std::cout << "Usage:./program --thread=xx --cq=xx --pool=xx --port=xx";
        return 0;
    }

    g_thread_num = std::atoi(ParseCmdPara(argv[1],"--thread="));
    g_cq_num = std::atoi(ParseCmdPara(argv[2],"--cq="));
    g_pool = std::atoi(ParseCmdPara(argv[3],"--pool="));
    g_port = std::atoi(ParseCmdPara(argv[4],"--port="));

    ServerImpl server;
    SimulateExternalEvent();
    server.Run();

    return 0;
}
