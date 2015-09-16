/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.transport.TransportRequestOptions.options;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public abstract class AbstractSimpleTransportTestCase extends ESTestCase {

    protected ThreadPool threadPool;

    protected static final Version version0 = Version.fromId(/*0*/99);
    protected DiscoveryNode nodeA;
    protected MockTransportService serviceA;

    protected static final Version version1 = Version.fromId(199);
    protected DiscoveryNode nodeB;
    protected MockTransportService serviceB;

    protected abstract MockTransportService build(Settings settings, Version version, NamedWriteableRegistry namedWriteableRegistry);

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new ThreadPool(getClass().getName());
        serviceA = build(
                Settings.builder().put("name", "TS_A", TransportService.SETTING_TRACE_LOG_INCLUDE, "", TransportService.SETTING_TRACE_LOG_EXCLUDE, "NOTHING").build(),
                version0, new NamedWriteableRegistry()
        );
        nodeA = new DiscoveryNode("TS_A", "TS_A", serviceA.boundAddress().publishAddress(), ImmutableMap.<String, String>of(), version0);
        serviceB = build(
                Settings.builder().put("name", "TS_B", TransportService.SETTING_TRACE_LOG_INCLUDE, "", TransportService.SETTING_TRACE_LOG_EXCLUDE, "NOTHING").build(),
                version1, new NamedWriteableRegistry()
        );
        nodeB = new DiscoveryNode("TS_B", "TS_B", serviceB.boundAddress().publishAddress(), ImmutableMap.<String, String>of(), version1);

        // wait till all nodes are properly connected and the event has been sent, so tests in this class
        // will not get this callback called on the connections done in this setup
        final boolean useLocalNode = randomBoolean();
        final CountDownLatch latch = new CountDownLatch(useLocalNode ? 2 : 4);
        TransportConnectionListener waitForConnection = new TransportConnectionListener() {
            @Override
            public void onNodeConnected(DiscoveryNode node) {
                latch.countDown();
            }

            @Override
            public void onNodeDisconnected(DiscoveryNode node) {
                fail("disconnect should not be called " + node);
            }
        };
        serviceA.addConnectionListener(waitForConnection);
        serviceB.addConnectionListener(waitForConnection);

        if (useLocalNode) {
            logger.info("--> using local node optimization");
            serviceA.setLocalNode(nodeA);
            serviceB.setLocalNode(nodeB);
        } else {
            logger.info("--> actively connecting to local node");
            serviceA.connectToNode(nodeA);
            serviceB.connectToNode(nodeB);
        }

        serviceA.connectToNode(nodeB);
        serviceB.connectToNode(nodeA);

        assertThat("failed to wait for all nodes to connect", latch.await(5, TimeUnit.SECONDS), equalTo(true));
        serviceA.removeConnectionListener(waitForConnection);
        serviceB.removeConnectionListener(waitForConnection);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        serviceA.close();
        serviceB.close();
        terminate(threadPool);
    }

    @Test
    public void testHelloWorld() {
        serviceA.registerRequestHandler("sayHello", StringMessageRequest::new, ThreadPool.Names.GENERIC, new TransportRequestHandler<StringMessageRequest>() {
            @Override
            public void messageReceived(StringMessageRequest request, TransportChannel channel) {
                assertThat("moshe", equalTo(request.message));
                try {
                    channel.sendResponse(new StringMessageResponse("hello " + request.message));
                } catch (IOException e) {
                    e.printStackTrace();
                    assertThat(e.getMessage(), false, equalTo(true));
                }
            }
        });

        TransportFuture<StringMessageResponse> res = serviceB.submitRequest(nodeA, "sayHello",
                new StringMessageRequest("moshe"), new BaseTransportResponseHandler<StringMessageResponse>() {
                    @Override
                    public StringMessageResponse newInstance() {
                        return new StringMessageResponse();
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }

                    @Override
                    public void handleResponse(StringMessageResponse response) {
                        assertThat("hello moshe", equalTo(response.message));
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        exp.printStackTrace();
                        assertThat("got exception instead of a response: " + exp.getMessage(), false, equalTo(true));
                    }
                });

        try {
            StringMessageResponse message = res.get();
            assertThat("hello moshe", equalTo(message.message));
        } catch (Exception e) {
            assertThat(e.getMessage(), false, equalTo(true));
        }

        res = serviceB.submitRequest(nodeA, "sayHello",
                new StringMessageRequest("moshe"), TransportRequestOptions.options().withCompress(true), new BaseTransportResponseHandler<StringMessageResponse>() {
                    @Override
                    public StringMessageResponse newInstance() {
                        return new StringMessageResponse();
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }

                    @Override
                    public void handleResponse(StringMessageResponse response) {
                        assertThat("hello moshe", equalTo(response.message));
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        exp.printStackTrace();
                        assertThat("got exception instead of a response: " + exp.getMessage(), false, equalTo(true));
                    }
                });

        try {
            StringMessageResponse message = res.get();
            assertThat("hello moshe", equalTo(message.message));
        } catch (Exception e) {
            assertThat(e.getMessage(), false, equalTo(true));
        }

        serviceA.removeHandler("sayHello");
    }

    @Test
    public void testLocalNodeConnection() throws InterruptedException {
        assertTrue("serviceA is not connected to nodeA", serviceA.nodeConnected(nodeA));
        if (((TransportService) serviceA).getLocalNode() != null) {
            // this should be a noop
            serviceA.disconnectFromNode(nodeA);
        }
        final AtomicReference<Exception> exception = new AtomicReference<>();
        serviceA.registerRequestHandler("localNode", StringMessageRequest::new, ThreadPool.Names.GENERIC, new TransportRequestHandler<StringMessageRequest>() {
            @Override
            public void messageReceived(StringMessageRequest request, TransportChannel channel) {
                try {
                    channel.sendResponse(new StringMessageResponse(request.message));
                } catch (IOException e) {
                    exception.set(e);
                }
            }
        });
        final AtomicReference<String> responseString = new AtomicReference<>();
        final CountDownLatch responseLatch = new CountDownLatch(1);
        serviceA.sendRequest(nodeA, "localNode", new StringMessageRequest("test"), new TransportResponseHandler<StringMessageResponse>() {
            @Override
            public StringMessageResponse newInstance() {
                return new StringMessageResponse();
            }

            @Override
            public void handleResponse(StringMessageResponse response) {
                responseString.set(response.message);
                responseLatch.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                exception.set(exp);
                responseLatch.countDown();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }
        });
        responseLatch.await();
        assertNull(exception.get());
        assertThat(responseString.get(), equalTo("test"));
    }

    @Test
    public void testVoidMessageCompressed() {
        serviceA.registerRequestHandler("sayHello", TransportRequest.Empty::new, ThreadPool.Names.GENERIC, new TransportRequestHandler<TransportRequest.Empty>() {
            @Override
            public void messageReceived(TransportRequest.Empty request, TransportChannel channel) {
                try {
                    channel.sendResponse(TransportResponse.Empty.INSTANCE, TransportResponseOptions.options().withCompress(true));
                } catch (IOException e) {
                    e.printStackTrace();
                    assertThat(e.getMessage(), false, equalTo(true));
                }
            }
        });

        TransportFuture<TransportResponse.Empty> res = serviceB.submitRequest(nodeA, "sayHello",
                TransportRequest.Empty.INSTANCE, TransportRequestOptions.options().withCompress(true), new BaseTransportResponseHandler<TransportResponse.Empty>() {
                    @Override
                    public TransportResponse.Empty newInstance() {
                        return TransportResponse.Empty.INSTANCE;
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }

                    @Override
                    public void handleResponse(TransportResponse.Empty response) {
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        exp.printStackTrace();
                        assertThat("got exception instead of a response: " + exp.getMessage(), false, equalTo(true));
                    }
                });

        try {
            TransportResponse.Empty message = res.get();
            assertThat(message, notNullValue());
        } catch (Exception e) {
            assertThat(e.getMessage(), false, equalTo(true));
        }

        serviceA.removeHandler("sayHello");
    }

    @Test
    public void testHelloWorldCompressed() {
        serviceA.registerRequestHandler("sayHello", StringMessageRequest::new, ThreadPool.Names.GENERIC, new TransportRequestHandler<StringMessageRequest>() {
            @Override
            public void messageReceived(StringMessageRequest request, TransportChannel channel) {
                assertThat("moshe", equalTo(request.message));
                try {
                    channel.sendResponse(new StringMessageResponse("hello " + request.message), TransportResponseOptions.options().withCompress(true));
                } catch (IOException e) {
                    e.printStackTrace();
                    assertThat(e.getMessage(), false, equalTo(true));
                }
            }
        });

        TransportFuture<StringMessageResponse> res = serviceB.submitRequest(nodeA, "sayHello",
                new StringMessageRequest("moshe"), TransportRequestOptions.options().withCompress(true), new BaseTransportResponseHandler<StringMessageResponse>() {
                    @Override
                    public StringMessageResponse newInstance() {
                        return new StringMessageResponse();
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }

                    @Override
                    public void handleResponse(StringMessageResponse response) {
                        assertThat("hello moshe", equalTo(response.message));
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        exp.printStackTrace();
                        assertThat("got exception instead of a response: " + exp.getMessage(), false, equalTo(true));
                    }
                });

        try {
            StringMessageResponse message = res.get();
            assertThat("hello moshe", equalTo(message.message));
        } catch (Exception e) {
            assertThat(e.getMessage(), false, equalTo(true));
        }

        serviceA.removeHandler("sayHello");
    }

    @Test
    public void testErrorMessage() {
        serviceA.registerRequestHandler("sayHelloException", StringMessageRequest::new, ThreadPool.Names.GENERIC, new TransportRequestHandler<StringMessageRequest>() {
            @Override
            public void messageReceived(StringMessageRequest request, TransportChannel channel) throws Exception {
                assertThat("moshe", equalTo(request.message));
                throw new RuntimeException("bad message !!!");
            }
        });

        TransportFuture<StringMessageResponse> res = serviceB.submitRequest(nodeA, "sayHelloException",
                new StringMessageRequest("moshe"), new BaseTransportResponseHandler<StringMessageResponse>() {
                    @Override
                    public StringMessageResponse newInstance() {
                        return new StringMessageResponse();
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }

                    @Override
                    public void handleResponse(StringMessageResponse response) {
                        fail("got response instead of exception");
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        assertThat("bad message !!!", equalTo(exp.getCause().getMessage()));
                    }
                });

        try {
            res.txGet();
            fail("exception should be thrown");
        } catch (Exception e) {
            assertThat(e.getCause().getMessage(), equalTo("bad message !!!"));
        }

        serviceA.removeHandler("sayHelloException");
    }

    @Test
    public void testDisconnectListener() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        TransportConnectionListener disconnectListener = new TransportConnectionListener() {
            @Override
            public void onNodeConnected(DiscoveryNode node) {
                fail("node connected should not be called, all connection have been done previously, node: " + node);
            }

            @Override
            public void onNodeDisconnected(DiscoveryNode node) {
                latch.countDown();
            }
        };
        serviceA.addConnectionListener(disconnectListener);
        serviceB.close();
        assertThat(latch.await(5, TimeUnit.SECONDS), equalTo(true));
    }

    @Test
    public void testNotifyOnShutdown() throws Exception {
        final CountDownLatch latch2 = new CountDownLatch(1);

        serviceA.registerRequestHandler("foobar", StringMessageRequest::new, ThreadPool.Names.GENERIC, new TransportRequestHandler<StringMessageRequest>() {
            @Override
            public void messageReceived(StringMessageRequest request, TransportChannel channel) {
                try {
                    latch2.await();
                    logger.info("Stop ServiceB now");
                    serviceB.stop();
                } catch (Exception e) {
                    fail(e.getMessage());
                }
            }
        });
        TransportFuture<TransportResponse.Empty> foobar = serviceB.submitRequest(nodeA, "foobar",
                new StringMessageRequest(""), options(), EmptyTransportResponseHandler.INSTANCE_SAME);
        latch2.countDown();
        try {
            foobar.txGet();
            fail("TransportException expected");
        } catch (TransportException ex) {

        }
        serviceA.removeHandler("sayHelloTimeoutDelayedResponse");
    }

    @Test
    public void testTimeoutSendExceptionWithNeverSendingBackResponse() throws Exception {
        serviceA.registerRequestHandler("sayHelloTimeoutNoResponse", StringMessageRequest::new, ThreadPool.Names.GENERIC, new TransportRequestHandler<StringMessageRequest>() {
            @Override
            public void messageReceived(StringMessageRequest request, TransportChannel channel) {
                assertThat("moshe", equalTo(request.message));
                // don't send back a response
//                try {
//                    channel.sendResponse(new StringMessage("hello " + request.message));
//                } catch (IOException e) {
//                    e.printStackTrace();
//                    assertThat(e.getMessage(), false, equalTo(true));
//                }
            }
        });

        TransportFuture<StringMessageResponse> res = serviceB.submitRequest(nodeA, "sayHelloTimeoutNoResponse",
                new StringMessageRequest("moshe"), options().withTimeout(100), new BaseTransportResponseHandler<StringMessageResponse>() {
                    @Override
                    public StringMessageResponse newInstance() {
                        return new StringMessageResponse();
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }

                    @Override
                    public void handleResponse(StringMessageResponse response) {
                        fail("got response instead of exception");
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        assertThat(exp, instanceOf(ReceiveTimeoutTransportException.class));
                    }
                });

        try {
            StringMessageResponse message = res.txGet();
            fail("exception should be thrown");
        } catch (Exception e) {
            assertThat(e, instanceOf(ReceiveTimeoutTransportException.class));
        }

        serviceA.removeHandler("sayHelloTimeoutNoResponse");
    }

    @Test
    public void testTimeoutSendExceptionWithDelayedResponse() throws Exception {
        serviceA.registerRequestHandler("sayHelloTimeoutDelayedResponse", StringMessageRequest::new, ThreadPool.Names.GENERIC, new TransportRequestHandler<StringMessageRequest>() {
            @Override
            public void messageReceived(StringMessageRequest request, TransportChannel channel) {
                TimeValue sleep = TimeValue.parseTimeValue(request.message, null, "sleep");
                try {
                    Thread.sleep(sleep.millis());
                } catch (InterruptedException e) {
                    // ignore
                }
                try {
                    channel.sendResponse(new StringMessageResponse("hello " + request.message));
                } catch (IOException e) {
                    e.printStackTrace();
                    assertThat(e.getMessage(), false, equalTo(true));
                }
            }
        });
        final CountDownLatch latch = new CountDownLatch(1);
        TransportFuture<StringMessageResponse> res = serviceB.submitRequest(nodeA, "sayHelloTimeoutDelayedResponse",
                new StringMessageRequest("300ms"), options().withTimeout(100), new BaseTransportResponseHandler<StringMessageResponse>() {
                    @Override
                    public StringMessageResponse newInstance() {
                        return new StringMessageResponse();
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }

                    @Override
                    public void handleResponse(StringMessageResponse response) {
                        latch.countDown();
                        fail("got response instead of exception");
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        latch.countDown();
                        assertThat(exp, instanceOf(ReceiveTimeoutTransportException.class));
                    }
                });

        try {
            StringMessageResponse message = res.txGet();
            fail("exception should be thrown");
        } catch (Exception e) {
            assertThat(e, instanceOf(ReceiveTimeoutTransportException.class));
        }
        latch.await();

        for (int i = 0; i < 10; i++) {
            final int counter = i;
            // now, try and send another request, this times, with a short timeout
            res = serviceB.submitRequest(nodeA, "sayHelloTimeoutDelayedResponse",
                    new StringMessageRequest(counter + "ms"), options().withTimeout(3000), new BaseTransportResponseHandler<StringMessageResponse>() {
                        @Override
                        public StringMessageResponse newInstance() {
                            return new StringMessageResponse();
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.GENERIC;
                        }

                        @Override
                        public void handleResponse(StringMessageResponse response) {
                            assertThat("hello " + counter + "ms", equalTo(response.message));
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            exp.printStackTrace();
                            fail("got exception instead of a response for " + counter + ": " + exp.getDetailedMessage());
                        }
                    });

            StringMessageResponse message = res.txGet();
            assertThat(message.message, equalTo("hello " + counter + "ms"));
        }

        serviceA.removeHandler("sayHelloTimeoutDelayedResponse");
    }


    @Test
    @TestLogging(value = "test. transport.tracer:TRACE")
    public void testTracerLog() throws InterruptedException {
        TransportRequestHandler handler = new TransportRequestHandler<StringMessageRequest>() {
            @Override
            public void messageReceived(StringMessageRequest request, TransportChannel channel) throws Exception {
                channel.sendResponse(new StringMessageResponse(""));
            }
        };

        TransportRequestHandler handlerWithError = new TransportRequestHandler<StringMessageRequest>() {
            @Override
            public void messageReceived(StringMessageRequest request, TransportChannel channel) throws Exception {
                if (request.timeout() > 0) {
                    Thread.sleep(request.timeout);
                }
                channel.sendResponse(new RuntimeException(""));

            }
        };

        final Semaphore requestCompleted = new Semaphore(0);
        TransportResponseHandler noopResponseHandler = new BaseTransportResponseHandler<StringMessageResponse>() {

            @Override
            public StringMessageResponse newInstance() {
                return new StringMessageResponse();
            }

            @Override
            public void handleResponse(StringMessageResponse response) {
                requestCompleted.release();
            }

            @Override
            public void handleException(TransportException exp) {
                requestCompleted.release();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        };

        serviceA.registerRequestHandler("test", StringMessageRequest::new, ThreadPool.Names.SAME, handler);
        serviceA.registerRequestHandler("testError", StringMessageRequest::new, ThreadPool.Names.SAME, handlerWithError);
        serviceB.registerRequestHandler("test", StringMessageRequest::new, ThreadPool.Names.SAME, handler);
        serviceB.registerRequestHandler("testError", StringMessageRequest::new, ThreadPool.Names.SAME, handlerWithError);

        final Tracer tracer = new Tracer();
        serviceA.addTracer(tracer);
        serviceB.addTracer(tracer);

        tracer.reset(4);
        boolean timeout = randomBoolean();
        TransportRequestOptions options = timeout ? new TransportRequestOptions().withTimeout(1) : TransportRequestOptions.EMPTY;
        serviceA.sendRequest(nodeB, "test", new StringMessageRequest("", 10), options, noopResponseHandler);
        requestCompleted.acquire();
        tracer.expectedEvents.get().await();
        assertThat("didn't see request sent", tracer.sawRequestSent, equalTo(true));
        assertThat("didn't see request received", tracer.sawRequestReceived, equalTo(true));
        assertThat("didn't see response sent", tracer.sawResponseSent, equalTo(true));
        assertThat("didn't see response received", tracer.sawResponseReceived, equalTo(true));
        assertThat("saw error sent", tracer.sawErrorSent, equalTo(false));

        tracer.reset(4);
        serviceA.sendRequest(nodeB, "testError", new StringMessageRequest(""), noopResponseHandler);
        requestCompleted.acquire();
        tracer.expectedEvents.get().await();
        assertThat("didn't see request sent", tracer.sawRequestSent, equalTo(true));
        assertThat("didn't see request received", tracer.sawRequestReceived, equalTo(true));
        assertThat("saw response sent", tracer.sawResponseSent, equalTo(false));
        assertThat("didn't see response received", tracer.sawResponseReceived, equalTo(true));
        assertThat("didn't see error sent", tracer.sawErrorSent, equalTo(true));

        String includeSettings;
        String excludeSettings;
        if (randomBoolean()) {
            // sometimes leave include empty (default)
            includeSettings = randomBoolean() ? "*" : "";
            excludeSettings = "*Error";
        } else {
            includeSettings = "test";
            excludeSettings = "DOESN'T_MATCH";
        }

        serviceA.applySettings(Settings.builder()
                .put(TransportService.SETTING_TRACE_LOG_INCLUDE, includeSettings, TransportService.SETTING_TRACE_LOG_EXCLUDE, excludeSettings)
                .build());

        tracer.reset(4);
        serviceA.sendRequest(nodeB, "test", new StringMessageRequest(""), noopResponseHandler);
        requestCompleted.acquire();
        tracer.expectedEvents.get().await();
        assertThat("didn't see request sent", tracer.sawRequestSent, equalTo(true));
        assertThat("didn't see request received", tracer.sawRequestReceived, equalTo(true));
        assertThat("didn't see response sent", tracer.sawResponseSent, equalTo(true));
        assertThat("didn't see response received", tracer.sawResponseReceived, equalTo(true));
        assertThat("saw error sent", tracer.sawErrorSent, equalTo(false));

        tracer.reset(2);
        serviceA.sendRequest(nodeB, "testError", new StringMessageRequest(""), noopResponseHandler);
        requestCompleted.acquire();
        tracer.expectedEvents.get().await();
        assertThat("saw request sent", tracer.sawRequestSent, equalTo(false));
        assertThat("didn't see request received", tracer.sawRequestReceived, equalTo(true));
        assertThat("saw response sent", tracer.sawResponseSent, equalTo(false));
        assertThat("saw response received", tracer.sawResponseReceived, equalTo(false));
        assertThat("didn't see error sent", tracer.sawErrorSent, equalTo(true));
    }

    private static class Tracer extends MockTransportService.Tracer {
        public volatile boolean sawRequestSent;
        public volatile boolean sawRequestReceived;
        public volatile boolean sawResponseSent;
        public volatile boolean sawErrorSent;
        public volatile boolean sawResponseReceived;

        public AtomicReference<CountDownLatch> expectedEvents = new AtomicReference<>();


        @Override
        public void receivedRequest(long requestId, String action) {
            super.receivedRequest(requestId, action);
            sawRequestReceived = true;
            expectedEvents.get().countDown();
        }

        @Override
        public void requestSent(DiscoveryNode node, long requestId, String action, TransportRequestOptions options) {
            super.requestSent(node, requestId, action, options);
            sawRequestSent = true;
            expectedEvents.get().countDown();
        }

        @Override
        public void responseSent(long requestId, String action) {
            super.responseSent(requestId, action);
            sawResponseSent = true;
            expectedEvents.get().countDown();
        }

        @Override
        public void responseSent(long requestId, String action, Throwable t) {
            super.responseSent(requestId, action, t);
            sawErrorSent = true;
            expectedEvents.get().countDown();
        }

        @Override
        public void receivedResponse(long requestId, DiscoveryNode sourceNode, String action) {
            super.receivedResponse(requestId, sourceNode, action);
            sawResponseReceived = true;
            expectedEvents.get().countDown();
        }

        public void reset(int expectedCount) {
            sawRequestSent = false;
            sawRequestReceived = false;
            sawResponseSent = false;
            sawErrorSent = false;
            sawResponseReceived = false;
            expectedEvents.set(new CountDownLatch(expectedCount));
        }
    }


    public static class StringMessageRequest extends TransportRequest {

        private String message;
        private long timeout;

        StringMessageRequest(String message, long timeout) {
            this.message = message;
            this.timeout = timeout;
        }

        public StringMessageRequest() {
        }

        public StringMessageRequest(String message) {
            this(message, -1);
        }

        public long timeout() {
            return timeout;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            message = in.readString();
            timeout = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(message);
            out.writeLong(timeout);
        }
    }

    static class StringMessageResponse extends TransportResponse {

        private String message;

        StringMessageResponse(String message) {
            this.message = message;
        }

        StringMessageResponse() {
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            message = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(message);
        }
    }


    public static class Version0Request extends TransportRequest {

        int value1;


        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            value1 = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(value1);
        }
    }

    public static class Version1Request extends Version0Request {

        int value2;

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            if (in.getVersion().onOrAfter(version1)) {
                value2 = in.readInt();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getVersion().onOrAfter(version1)) {
                out.writeInt(value2);
            }
        }
    }

    static class Version0Response extends TransportResponse {

        int value1;

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            value1 = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(value1);
        }
    }

    static class Version1Response extends Version0Response {

        int value2;

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            if (in.getVersion().onOrAfter(version1)) {
                value2 = in.readInt();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getVersion().onOrAfter(version1)) {
                out.writeInt(value2);
            }
        }
    }

    @Test
    public void testVersion_from0to1() throws Exception {
        serviceB.registerRequestHandler("/version", Version1Request::new, ThreadPool.Names.SAME, new TransportRequestHandler<Version1Request>() {
            @Override
            public void messageReceived(Version1Request request, TransportChannel channel) throws Exception {
                assertThat(request.value1, equalTo(1));
                assertThat(request.value2, equalTo(0)); // not set, coming from service A
                Version1Response response = new Version1Response();
                response.value1 = 1;
                response.value2 = 2;
                channel.sendResponse(response);
            }
        });

        Version0Request version0Request = new Version0Request();
        version0Request.value1 = 1;
        Version0Response version0Response = serviceA.submitRequest(nodeB, "/version", version0Request, new BaseTransportResponseHandler<Version0Response>() {
            @Override
            public Version0Response newInstance() {
                return new Version0Response();
            }

            @Override
            public void handleResponse(Version0Response response) {
                assertThat(response.value1, equalTo(1));
            }

            @Override
            public void handleException(TransportException exp) {
                exp.printStackTrace();
                fail();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        }).txGet();

        assertThat(version0Response.value1, equalTo(1));
    }

    @Test
    public void testVersion_from1to0() throws Exception {
        serviceA.registerRequestHandler("/version", Version0Request::new, ThreadPool.Names.SAME, new TransportRequestHandler<Version0Request>() {
            @Override
            public void messageReceived(Version0Request request, TransportChannel channel) throws Exception {
                assertThat(request.value1, equalTo(1));
                Version0Response response = new Version0Response();
                response.value1 = 1;
                channel.sendResponse(response);
            }
        });

        Version1Request version1Request = new Version1Request();
        version1Request.value1 = 1;
        version1Request.value2 = 2;
        Version1Response version1Response = serviceB.submitRequest(nodeA, "/version", version1Request, new BaseTransportResponseHandler<Version1Response>() {
            @Override
            public Version1Response newInstance() {
                return new Version1Response();
            }

            @Override
            public void handleResponse(Version1Response response) {
                assertThat(response.value1, equalTo(1));
                assertThat(response.value2, equalTo(0)); // initial values, cause its serialized from version 0
            }

            @Override
            public void handleException(TransportException exp) {
                exp.printStackTrace();
                fail();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        }).txGet();

        assertThat(version1Response.value1, equalTo(1));
        assertThat(version1Response.value2, equalTo(0));
    }

    @Test
    public void testVersion_from1to1() throws Exception {
        serviceB.registerRequestHandler("/version", Version1Request::new, ThreadPool.Names.SAME, new TransportRequestHandler<Version1Request>() {
            @Override
            public void messageReceived(Version1Request request, TransportChannel channel) throws Exception {
                assertThat(request.value1, equalTo(1));
                assertThat(request.value2, equalTo(2));
                Version1Response response = new Version1Response();
                response.value1 = 1;
                response.value2 = 2;
                channel.sendResponse(response);
            }
        });

        Version1Request version1Request = new Version1Request();
        version1Request.value1 = 1;
        version1Request.value2 = 2;
        Version1Response version1Response = serviceB.submitRequest(nodeB, "/version", version1Request, new BaseTransportResponseHandler<Version1Response>() {
            @Override
            public Version1Response newInstance() {
                return new Version1Response();
            }

            @Override
            public void handleResponse(Version1Response response) {
                assertThat(response.value1, equalTo(1));
                assertThat(response.value2, equalTo(2));
            }

            @Override
            public void handleException(TransportException exp) {
                exp.printStackTrace();
                fail();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        }).txGet();

        assertThat(version1Response.value1, equalTo(1));
        assertThat(version1Response.value2, equalTo(2));
    }

    @Test
    public void testVersion_from0to0() throws Exception {
        serviceA.registerRequestHandler("/version", Version0Request::new, ThreadPool.Names.SAME, new TransportRequestHandler<Version0Request>() {
            @Override
            public void messageReceived(Version0Request request, TransportChannel channel) throws Exception {
                assertThat(request.value1, equalTo(1));
                Version0Response response = new Version0Response();
                response.value1 = 1;
                channel.sendResponse(response);
            }
        });

        Version0Request version0Request = new Version0Request();
        version0Request.value1 = 1;
        Version0Response version0Response = serviceA.submitRequest(nodeA, "/version", version0Request, new BaseTransportResponseHandler<Version0Response>() {
            @Override
            public Version0Response newInstance() {
                return new Version0Response();
            }

            @Override
            public void handleResponse(Version0Response response) {
                assertThat(response.value1, equalTo(1));
            }

            @Override
            public void handleException(TransportException exp) {
                exp.printStackTrace();
                fail();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        }).txGet();

        assertThat(version0Response.value1, equalTo(1));
    }

    @Test
    public void testMockFailToSendNoConnectRule() {
        serviceA.registerRequestHandler("sayHello", StringMessageRequest::new, ThreadPool.Names.GENERIC, new TransportRequestHandler<StringMessageRequest>() {
            @Override
            public void messageReceived(StringMessageRequest request, TransportChannel channel) throws Exception {
                assertThat("moshe", equalTo(request.message));
                throw new RuntimeException("bad message !!!");
            }
        });

        serviceB.addFailToSendNoConnectRule(nodeA);

        TransportFuture<StringMessageResponse> res = serviceB.submitRequest(nodeA, "sayHello",
                new StringMessageRequest("moshe"), new BaseTransportResponseHandler<StringMessageResponse>() {
                    @Override
                    public StringMessageResponse newInstance() {
                        return new StringMessageResponse();
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }

                    @Override
                    public void handleResponse(StringMessageResponse response) {
                        fail("got response instead of exception");
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        assertThat(exp.getCause().getMessage(), endsWith("DISCONNECT: simulated"));
                    }
                });

        try {
            res.txGet();
            fail("exception should be thrown");
        } catch (Exception e) {
            assertThat(e.getCause().getMessage(), endsWith("DISCONNECT: simulated"));
        }

        try {
            serviceB.connectToNode(nodeA);
            fail("exception should be thrown");
        } catch (ConnectTransportException e) {
            // all is well
        }

        try {
            serviceB.connectToNodeLight(nodeA);
            fail("exception should be thrown");
        } catch (ConnectTransportException e) {
            // all is well
        }

        serviceA.removeHandler("sayHello");
    }

    @Test
    public void testMockUnresponsiveRule() {
        serviceA.registerRequestHandler("sayHello", StringMessageRequest::new, ThreadPool.Names.GENERIC, new TransportRequestHandler<StringMessageRequest>() {
            @Override
            public void messageReceived(StringMessageRequest request, TransportChannel channel) throws Exception {
                assertThat("moshe", equalTo(request.message));
                throw new RuntimeException("bad message !!!");
            }
        });

        serviceB.addUnresponsiveRule(nodeA);

        TransportFuture<StringMessageResponse> res = serviceB.submitRequest(nodeA, "sayHello",
                new StringMessageRequest("moshe"), TransportRequestOptions.options().withTimeout(100), new BaseTransportResponseHandler<StringMessageResponse>() {
                    @Override
                    public StringMessageResponse newInstance() {
                        return new StringMessageResponse();
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }

                    @Override
                    public void handleResponse(StringMessageResponse response) {
                        fail("got response instead of exception");
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        assertThat(exp, instanceOf(ReceiveTimeoutTransportException.class));
                    }
                });

        try {
            res.txGet();
            fail("exception should be thrown");
        } catch (Exception e) {
            assertThat(e, instanceOf(ReceiveTimeoutTransportException.class));
        }

        try {
            serviceB.connectToNode(nodeA);
            fail("exception should be thrown");
        } catch (ConnectTransportException e) {
            // all is well
        }

        try {
            serviceB.connectToNodeLight(nodeA);
            fail("exception should be thrown");
        } catch (ConnectTransportException e) {
            // all is well
        }

        serviceA.removeHandler("sayHello");
    }


    @Test
    public void testHostOnMessages() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        final AtomicReference<TransportAddress> addressA = new AtomicReference<>();
        final AtomicReference<TransportAddress> addressB = new AtomicReference<>();
        serviceB.registerRequestHandler("action1", TestRequest::new, ThreadPool.Names.SAME, new TransportRequestHandler<TestRequest>() {
            @Override
            public void messageReceived(TestRequest request, TransportChannel channel) throws Exception {
                addressA.set(request.remoteAddress());
                channel.sendResponse(new TestResponse());
                latch.countDown();
            }
        });
        serviceA.sendRequest(nodeB, "action1", new TestRequest(), new TransportResponseHandler<TestResponse>() {
            @Override
            public TestResponse newInstance() {
                return new TestResponse();
            }

            @Override
            public void handleResponse(TestResponse response) {
                addressB.set(response.remoteAddress());
                latch.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                latch.countDown();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        });

        if (!latch.await(10, TimeUnit.SECONDS)) {
            fail("message round trip did not complete within a sensible time frame");
        }

        assertTrue(nodeA.address().sameHost(addressA.get()));
        assertTrue(nodeB.address().sameHost(addressB.get()));
    }

    public static class TestRequest extends TransportRequest {
    }

    private static class TestResponse extends TransportResponse {
    }
}
