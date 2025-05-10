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

#include <grpcpp/server.h>
#include <grpcpp/server_context.h>
// #include <grpcpp/server_async_reader_writer.h> 1.38 이상
#include <grpcpp/support/async_stream.h>

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

class GrpcService
{
    public:
        class CallDataBase
        {
            public:
                CallDataBase(Greeter::AsyncService* service, ServerCompletionQueue* cq) : service_(service), cq_(cq) {}
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
        
        class CallDataUnary : public CallDataBase
        {
            public:
                // Take in the "service" instance (in this case representing an asynchronous
                // server) and the completion queue "cq" used for asynchronous communication
                // with the gRPC runtime.
                CallDataUnary(Greeter::AsyncService* service, ServerCompletionQueue* cq) : CallDataBase(service,cq),responder_(&ctx_), status_(CREATE)
                {
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
        
                void Proceed(bool ok) override
                {
                    //std::cout << "ok value:" << ok << std::endl;
                    if (status_ == PROCESS)
                    {
                        this->SpawnInstance(service_, cq_);
                        std::string prefix("Hello ");
                        reply_.set_message(prefix + request_.name());
                        std::cout << "req name:" << request_.name() << std::endl;
                        status_ = FINISH;
                        responder_.Finish(reply_, Status::OK, (void*)this);
                    }
                    else
                    {
                        delete this;
                    }
                }
        
            protected:
                ServerAsyncResponseWriter<HelloReply> responder_;
                enum CallStatus { CREATE, PROCESS, FINISH };
                CallStatus status_;
        };
        
        class CallDataSayHello : public CallDataUnary
        {
            public:
                CallDataSayHello(Greeter::AsyncService* service, ServerCompletionQueue* cq)
                    : CallDataUnary(service, cq)
                {
                    service_->RequestSayHello(&ctx_, &request_, &responder_, cq_, cq_, (void*)this);
                }
        
                void SpawnInstance(Greeter::AsyncService* service, ServerCompletionQueue* cq) override
                {
                    new CallDataSayHello(service, cq);
                }
        };
        
        class CallDataSvrStream : public CallDataBase
        {
            public:
                // Take in the "service" instance (in this case representing an asynchronous
                // server) and the completion queue "cq" used for asynchronous communication
                // with the gRPC runtime.
                CallDataSvrStream(Greeter::AsyncService* service, ServerCompletionQueue* cq)
                    : CallDataBase(service, cq), writer_(&ctx_), status_(SvrStreamStatus::START)
                {
                    // Invoke the serving logic right away.
                    ctx_.AsyncNotifyWhenDone((void*)this);
                    service_->RequestSayHelloSvrStreamReply(&ctx_, &request_, &writer_, cq_, cq_, (void*)this);
        
                    std::lock_guard<std::mutex> lock(GrpcService::g_svr_stream_mutex);
                    GrpcService::g_svr_stream_instances.push_back(this);
                }
        
                ~CallDataSvrStream()
                {
                    std::lock_guard<std::mutex> lock(GrpcService::g_svr_stream_mutex);
                    GrpcService::g_svr_stream_instances.erase(std::remove(GrpcService::g_svr_stream_instances.begin(), GrpcService::g_svr_stream_instances.end(), this), GrpcService::g_svr_stream_instances.end());
                }
            
                void sendSvrEvent(const std::string& msg)
                {
                    std::lock_guard<std::mutex> lock(m_mutex);
                    if (status_ == SvrStreamStatus::READY)
                    {
                        HelloReply reply;
                        reply.set_message(msg);
                        std::cout << "=========================== server stream message to be sent" << std::endl;
                        writer_.Write(reply, (void*)this); // Async write
                    }
                }
            
                void Proceed(bool ok) override
                {
                    std::unique_lock<std::mutex> lock(m_mutex);

                    if (!ok) {
                        status_ = SvrStreamStatus::FINISH;
                        return;
                    }

                    switch (status_)
                    {
                        case SvrStreamStatus::START:
                            new CallDataSvrStream(service_, cq_);
                            status_ = SvrStreamStatus::READY;
                            break;
                        case SvrStreamStatus::READY:
                            std::cout << "=========================== server stream message sent" << std::endl;
                            break;
                        case SvrStreamStatus::FINISH:
                            writer_.Finish(Status::OK, (void*)this);
                            lock.unlock();
                            delete this;
                            status_ = SvrStreamStatus::START;
                            break;
                    }
                }
        
            private:
                // The means to get back to the client.
                ServerAsyncWriter<HelloReply> writer_;
                // Let's implement a tiny state machine with the following states.
                enum class SvrStreamStatus { START, READY, FINISH };
                SvrStreamStatus status_;
                std::mutex m_mutex;
        };
        
        class CallDataBidiStream : public CallDataBase
        {
            public:
                // Take in the "service" instance (in this case representing an asynchronous
                // server) and the completion queue "cq" used for asynchronous communication
                // with the gRPC runtime.
                CallDataBidiStream(Greeter::AsyncService* service, ServerCompletionQueue* cq)
                    : CallDataBase(service, cq), stream_(&ctx_), status_(BidiStreamStatus::START)
                {
                    // Invoke the serving logic right away.
                    ctx_.AsyncNotifyWhenDone((void*)this);
                    service_->RequestSayHelloBidiStreamReply(&ctx_, &stream_, cq_, cq_, (void*)this);
        
                    std::lock_guard<std::mutex> lock(g_bidi_stream_mutex);
                    g_bidi_stream_instances.push_back(this);
                }
        
                ~CallDataBidiStream()
                {
                    std::lock_guard<std::mutex> lock(g_bidi_stream_mutex);
                    g_bidi_stream_instances.erase(std::remove(g_bidi_stream_instances.begin(), g_bidi_stream_instances.end(), this), g_bidi_stream_instances.end());
                }
            
                void sendBidiEvent(const std::string& msg)
                {
                    std::lock_guard<std::mutex> lock(m_mutex);
                    if (status_ == BidiStreamStatus::READY)
                    {
                        HelloReply reply;
                        reply.set_message(msg);
                        std::cout << "=========================== bidi stream message to be sent" << std::endl;
                        // don't register call handler, leave it as null so that Proceed() will not be executed
                        stream_.Write(reply, nullptr); // Async write; Next() return nullptr for CallData tag
                    }
                }
            
                void Proceed(bool ok) override
                {
                    std::unique_lock<std::mutex> lock(m_mutex);

                    if (!ok) {
                        status_ = BidiStreamStatus::FINISH;
                        return;
                    }

                    switch (status_)
                    {
                        case BidiStreamStatus::START:
                            std::cout << "=========================== bidi stream req rcvd" << std::endl;
                            stream_.Read(&request_, (void*)this); // 이것이 반드시 필요
                            new CallDataBidiStream(service_, cq_);
                            status_ = BidiStreamStatus::READY;
                            break;
                        case BidiStreamStatus::READY:
                            stream_.Read(&request_, (void*)this);
                            std::cout << "=========================== bidi stream message rcvd: " << request_.name() << std::endl;
                            break;
                        case BidiStreamStatus::FINISH:
                            std::cout << "=========================== bidi stream closing" << std::endl;
                            stream_.Finish(Status::OK, this);
                            status_ = BidiStreamStatus::DESTROY;
                            break;
                        case BidiStreamStatus::DESTROY:
                            std::cout << "=========================== bidi stream destroying" << std::endl;
                            delete this;
                            break;
                    }
                }

        private:
            // The means to get back to the client.
            grpc::ServerAsyncReaderWriter<HelloReply, HelloRequest> stream_;
            // Let's implement a tiny state machine with the following states.
            enum class BidiStreamStatus { START, READY, FINISH, DESTROY };
            BidiStreamStatus status_;
            std::mutex m_mutex;
    };

    // 전역 리스트로 활성 CallDataSvrStream 인스턴스를 추적
    static std::mutex g_svr_stream_mutex;
    static std::vector<class CallDataSvrStream*> g_svr_stream_instances;

    static std::mutex g_bidi_stream_mutex;
    static std::vector<class CallDataBidiStream*> g_bidi_stream_instances;
};

// 정적 멤버 정의 및 초기화
std::mutex GrpcService::g_svr_stream_mutex;
std::vector<GrpcService::CallDataSvrStream*> GrpcService::g_svr_stream_instances;
std::mutex GrpcService::g_bidi_stream_mutex;
std::vector<class GrpcService::CallDataBidiStream*> GrpcService::g_bidi_stream_instances;

class ServerImpl final
{
    public:
        enum class SvrStreamStatus { TEST_SVR_STREAM, TEST_BIDI_STREAM };

        static int g_thread_num;
        static int g_cq_num;
        static int g_pool;
        static int g_port;
        static SvrStreamStatus g_testStream; // test server stream;

        ServerImpl()
        {
        }

        ~ServerImpl()
        {
            server_->Shutdown();
            for (const auto& _cq : m_cq)
                _cq->Shutdown();
        }
    
        void Run()
        {
            std::string server_address("0.0.0.0:" + std::to_string(g_port));
            ServerBuilder builder;

            // Listen on the given address without any authentication mechanism.
            builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

            // Register "service_" as the instance through which we'll communicate with
            // clients. In this case it corresponds to an *asynchronous* service.
            builder.RegisterService(&asyncService_);

            // Get hold of the completion queue used for the asynchronous communication
            // with the gRPC runtime.
            for (int i = 0; i < g_cq_num; ++i)
            {
                m_cq.emplace_back(builder.AddCompletionQueue());
            }

            // Finally assemble the server.
            server_ = builder.BuildAndStart();
            std::cout << "Server listening on " << server_address << std::endl;
    
            // Proceed to the server's main loop.
            std::vector<std::thread*> _vec_threads;
            for (int i = 0; i < g_thread_num; ++i)
            {
                int _cq_idx = i % g_cq_num;
                for (int j = 0; j < g_pool; ++j)
                {
                    new GrpcService::CallDataSayHello(&asyncService_, m_cq[_cq_idx].get());
                    if (g_testStream == SvrStreamStatus::TEST_SVR_STREAM)
                    {
                        new GrpcService::CallDataSvrStream(&asyncService_, m_cq[_cq_idx].get());
                    }
                    else
                    {
                        new GrpcService::CallDataBidiStream(&asyncService_, m_cq[_cq_idx].get());
                    }
                }
                _vec_threads.emplace_back(new std::thread(&ServerImpl::HandleRpcs, this, _cq_idx));
            }
    
            std::cout << g_thread_num << " working async threads spawned" << std::endl;
    
            for (const auto& _t : _vec_threads)
            {
                _t->join();
            }
        }
    
    private:
        // Class encompasing the state and logic needed to serve a request.

        // This can be run in multiple threads if needed.
        void HandleRpcs(int cq_idx)
        {
            void* tag;
            bool ok;
            while (m_cq[cq_idx]->Next(&tag, &ok))
            {
                if (ok == true)
                {
                    std::cout << "=========================== cq Next called" << std::endl;
                    // Block waiting to read the next event from the completion queue. The
                    // event is uniquely identified by its tag, which in this case is the
                    // memory address of a CallDataUnary instance.
                    // The return value of Next should always be checked. This return value
                    // tells us whether there is any kind of event or cq_ is shutting down.
                    //GPR_ASSERT(cq_->Next(&tag, &ok));
                    GrpcService::CallDataBase* _p_ins = (GrpcService::CallDataBase*)tag;

                    if (_p_ins != nullptr)
                    {
                        std::cout << "=========================== proceed()" << std::endl;
                        _p_ins->Proceed(ok);
                    }
                }
                else
                {
                    GrpcService::CallDataBase* _p_ins = (GrpcService::CallDataBase*)tag;

                    if (_p_ins != nullptr)
                    {
                        _p_ins->Proceed(ok);
                    }
                }
            }
        }
    
        std::vector<std::unique_ptr<ServerCompletionQueue>> m_cq;
        Greeter::AsyncService asyncService_;
        std::unique_ptr<Server> server_;
};

const char* ParseCmdPara(char* argv, const char* para)
{
    if (argv == nullptr)
    {
        return nullptr;
    }

    auto p_target = std::strstr(argv, para);
    if (p_target == nullptr)
    {
        printf("para error argv[%s] should be %s \n", argv, para);
        return nullptr;
    }

    return p_target + std::strlen(para);
}

void SimulateExternalSvrEvent()
{
    std::thread([]()
    {
        int count = 0;
        while (true)
        {
            std::this_thread::sleep_for(std::chrono::seconds(5));
            std::string event_data = "[Push Event] External Svr Event #" + std::to_string(count++);

            std::lock_guard<std::mutex> lock(GrpcService::g_svr_stream_mutex);
            for (GrpcService::CallDataSvrStream* stream : GrpcService::g_svr_stream_instances)
            {
                stream->sendSvrEvent(event_data);
            }
        }
    }).detach();
}

void SimulateExternalBidiEvent()
{
    std::thread([]()
    {
        int count = 0;
        while (true)
        {
            std::this_thread::sleep_for(std::chrono::seconds(5));
            std::string event_data = "[Push Event] External Bidi Event #" + std::to_string(count++);

            std::lock_guard<std::mutex> lock(GrpcService::g_bidi_stream_mutex);
            for (GrpcService::CallDataBidiStream* stream : GrpcService::g_bidi_stream_instances)
            {
                stream->sendBidiEvent(event_data);
            }
        }
    }).detach();
}

ServerImpl::SvrStreamStatus ServerImpl::g_testStream = ServerImpl::SvrStreamStatus::TEST_BIDI_STREAM; // test bidi stream by default
int ServerImpl::g_thread_num = 1;
int ServerImpl::g_cq_num = 1;
int ServerImpl::g_pool = 1;
int ServerImpl::g_port = 50051;

int main(int argc, char** argv)
{
    if (argc != 5)
    {
        std::cout << "Usage: ./program --test=[svr/bidi] --thread=xx --cq=xx --pool=xx --port=xx";
    }

    const char *szTestStream = ParseCmdPara(argv[1], "--test=");
    const char *szThreadNum = ParseCmdPara(argv[2], "--thread=");
    const char *szCqNum = ParseCmdPara(argv[3], "--cq=");
    const char *szPoolNum =ParseCmdPara(argv[4], "--pool=");
    const char *szPort = ParseCmdPara(argv[5], "--port=");

    if (szTestStream != nullptr)
    {
        if (std::strncmp(szTestStream, "svr", 3) == 0)
        {
            ServerImpl::g_testStream = ServerImpl::SvrStreamStatus::TEST_SVR_STREAM;
        }
        else
        {
            ServerImpl::g_testStream = ServerImpl::SvrStreamStatus::TEST_BIDI_STREAM;
        }
    }

    if (szThreadNum != nullptr)
    {
        ServerImpl::g_thread_num = std::atoi(szThreadNum);
    }

    if (szCqNum != nullptr)
    {
        ServerImpl::g_cq_num = std::atoi(szCqNum);
    }

    if (szPoolNum != nullptr)
    {
        ServerImpl::g_pool = std::atoi(szPoolNum);
    }

    if (szPort != nullptr)
    {
        ServerImpl::g_port = std::atoi(szPort);
    }

    ServerImpl server;

    if (ServerImpl::g_testStream == ServerImpl::SvrStreamStatus::TEST_SVR_STREAM)
    {
        std::cout << "SimulateExternalSvrEvent() is running" << std::endl;
        SimulateExternalSvrEvent(); // to test server stream
    }
    else
    {
        std::cout << "SimulateExternalBidiEvent() is running" << std::endl;
        SimulateExternalBidiEvent();  // to test bidi stream
    }

    server.Run();

    return 0;
}
