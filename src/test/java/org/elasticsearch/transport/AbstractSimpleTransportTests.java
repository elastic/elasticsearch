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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.transport.TransportRequestOptions.options;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public abstract class AbstractSimpleTransportTests extends ElasticsearchTestCase {

    protected ThreadPool threadPool;

    protected static final Version version0 = Version.fromId(/*0*/99);
    protected DiscoveryNode nodeA;
    protected MockTransportService serviceA;

    protected static final Version version1 = Version.fromId(199);
    protected DiscoveryNode nodeB;
    protected MockTransportService serviceB;

    protected abstract MockTransportService build(Settings settings, Version version);

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new ThreadPool(getClass().getName());
        serviceA = build(ImmutableSettings.builder().put("name", "TS_A").build(), version0);
        nodeA = new DiscoveryNode("TS_A", "TS_A", serviceA.boundAddress().publishAddress(), ImmutableMap.<String, String>of(), version0);
        serviceB = build(ImmutableSettings.builder().put("name", "TS_B").build(), version1);
        nodeB = new DiscoveryNode("TS_B", "TS_B", serviceB.boundAddress().publishAddress(), ImmutableMap.<String, String>of(), version1);

        // wait till all nodes are properly connected and the event has been sent, so tests in this class
        // will not get this callback called on the connections done in this setup
        final CountDownLatch latch = new CountDownLatch(4);
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

        serviceA.connectToNode(nodeB);
        serviceA.connectToNode(nodeA);
        serviceB.connectToNode(nodeA);
        serviceB.connectToNode(nodeB);

        assertThat("failed to wait for all nodes to connect", latch.await(5, TimeUnit.SECONDS), equalTo(true));
        serviceA.removeConnectionListener(waitForConnection);
        serviceB.removeConnectionListener(waitForConnection);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        serviceA.close();
        serviceB.close();
        terminate(threadPool);
    }

    @Test
    public void testHelloWorld() {
        serviceA.registerHandler("sayHello", new BaseTransportRequestHandler<StringMessageRequest>() {
            @Override
            public StringMessageRequest newInstance() {
                return new StringMessageRequest();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }

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
    public void testVoidMessageCompressed() {
        serviceA.registerHandler("sayHello", new BaseTransportRequestHandler<TransportRequest.Empty>() {
            @Override
            public TransportRequest.Empty newInstance() {
                return TransportRequest.Empty.INSTANCE;
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }

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
        serviceA.registerHandler("sayHello", new BaseTransportRequestHandler<StringMessageRequest>() {
            @Override
            public StringMessageRequest newInstance() {
                return new StringMessageRequest();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }

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
        serviceA.registerHandler("sayHelloException", new BaseTransportRequestHandler<StringMessageRequest>() {
            @Override
            public StringMessageRequest newInstance() {
                return new StringMessageRequest();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }

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
                assertThat("got response instead of exception", false, equalTo(true));
            }

            @Override
            public void handleException(TransportException exp) {
                assertThat("bad message !!!", equalTo(exp.getCause().getMessage()));
            }
        });

        try {
            res.txGet();
            assertThat("exception should be thrown", false, equalTo(true));
        } catch (Exception e) {
            assertThat("bad message !!!", equalTo(e.getCause().getMessage()));
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

        serviceA.registerHandler("foobar", new BaseTransportRequestHandler<StringMessageRequest>() {
            @Override
            public StringMessageRequest newInstance() {
                return new StringMessageRequest();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }

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
        serviceA.registerHandler("sayHelloTimeoutNoResponse", new BaseTransportRequestHandler<StringMessageRequest>() {
            @Override
            public StringMessageRequest newInstance() {
                return new StringMessageRequest();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }

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
                assertThat("got response instead of exception", false, equalTo(true));
            }

            @Override
            public void handleException(TransportException exp) {
                assertThat(exp, instanceOf(ReceiveTimeoutTransportException.class));
            }
        });

        try {
            StringMessageResponse message = res.txGet();
            assertThat("exception should be thrown", false, equalTo(true));
        } catch (Exception e) {
            assertThat(e, instanceOf(ReceiveTimeoutTransportException.class));
        }

        serviceA.removeHandler("sayHelloTimeoutNoResponse");
    }

    @Test
    public void testTimeoutSendExceptionWithDelayedResponse() throws Exception {
        serviceA.registerHandler("sayHelloTimeoutDelayedResponse", new BaseTransportRequestHandler<StringMessageRequest>() {
            @Override
            public StringMessageRequest newInstance() {
                return new StringMessageRequest();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }

            @Override
            public void messageReceived(StringMessageRequest request, TransportChannel channel) {
                TimeValue sleep = TimeValue.parseTimeValue(request.message, null);
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
                assertThat("got response instead of exception", false, equalTo(true));
            }

            @Override
            public void handleException(TransportException exp) {
                latch.countDown();
                assertThat(exp, instanceOf(ReceiveTimeoutTransportException.class));
            }
        });

        try {
            StringMessageResponse message = res.txGet();
            assertThat("exception should be thrown", false, equalTo(true));
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
                    assertThat("got exception instead of a response for " + counter + ": " + exp.getDetailedMessage(), false, equalTo(true));
                }
            });

            StringMessageResponse message = res.txGet();
            assertThat(message.message, equalTo("hello " + counter + "ms"));
        }

        serviceA.removeHandler("sayHelloTimeoutDelayedResponse");
    }

    static class StringMessageRequest extends TransportRequest {

        private String message;

        StringMessageRequest(String message) {
            this.message = message;
        }

        StringMessageRequest() {
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


    static class Version0Request extends TransportRequest {

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

    static class Version1Request extends Version0Request {

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
        serviceB.registerHandler("/version", new BaseTransportRequestHandler<Version1Request>() {
            @Override
            public Version1Request newInstance() {
                return new Version1Request();
            }

            @Override
            public void messageReceived(Version1Request request, TransportChannel channel) throws Exception {
                assertThat(request.value1, equalTo(1));
                assertThat(request.value2, equalTo(0)); // not set, coming from service A
                Version1Response response = new Version1Response();
                response.value1 = 1;
                response.value2 = 2;
                channel.sendResponse(response);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
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
        serviceA.registerHandler("/version", new BaseTransportRequestHandler<Version0Request>() {
            @Override
            public Version0Request newInstance() {
                return new Version0Request();
            }

            @Override
            public void messageReceived(Version0Request request, TransportChannel channel) throws Exception {
                assertThat(request.value1, equalTo(1));
                Version0Response response = new Version0Response();
                response.value1 = 1;
                channel.sendResponse(response);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
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
        serviceB.registerHandler("/version", new BaseTransportRequestHandler<Version1Request>() {
            @Override
            public Version1Request newInstance() {
                return new Version1Request();
            }

            @Override
            public void messageReceived(Version1Request request, TransportChannel channel) throws Exception {
                assertThat(request.value1, equalTo(1));
                assertThat(request.value2, equalTo(2));
                Version1Response response = new Version1Response();
                response.value1 = 1;
                response.value2 = 2;
                channel.sendResponse(response);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
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
        serviceA.registerHandler("/version", new BaseTransportRequestHandler<Version0Request>() {
            @Override
            public Version0Request newInstance() {
                return new Version0Request();
            }

            @Override
            public void messageReceived(Version0Request request, TransportChannel channel) throws Exception {
                assertThat(request.value1, equalTo(1));
                Version0Response response = new Version0Response();
                response.value1 = 1;
                channel.sendResponse(response);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
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
        serviceA.registerHandler("sayHello", new BaseTransportRequestHandler<StringMessageRequest>() {
            @Override
            public StringMessageRequest newInstance() {
                return new StringMessageRequest();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }

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
                        assertThat("got response instead of exception", false, equalTo(true));
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        assertThat(exp.getCause().getMessage(), endsWith("DISCONNECT: simulated"));
                    }
                });

        try {
            res.txGet();
            assertThat("exception should be thrown", false, equalTo(true));
        } catch (Exception e) {
            assertThat(e.getCause().getMessage(), endsWith("DISCONNECT: simulated"));
        }

        try {
            serviceB.connectToNode(nodeA);
            assertThat("exception should be thrown", false, equalTo(true));
        } catch (ConnectTransportException e) {
            // all is well
        }

        try {
            serviceB.connectToNodeLight(nodeA);
            assertThat("exception should be thrown", false, equalTo(true));
        } catch (ConnectTransportException e) {
            // all is well
        }

        serviceA.removeHandler("sayHello");
    }

    @Test
    public void testMockUnresponsiveRule() {
        serviceA.registerHandler("sayHello", new BaseTransportRequestHandler<StringMessageRequest>() {
            @Override
            public StringMessageRequest newInstance() {
                return new StringMessageRequest();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }

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
                        assertThat("got response instead of exception", false, equalTo(true));
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        assertThat(exp, instanceOf(ReceiveTimeoutTransportException.class));
                    }
                });

        try {
            res.txGet();
            assertThat("exception should be thrown", false, equalTo(true));
        } catch (Exception e) {
            assertThat(e, instanceOf(ReceiveTimeoutTransportException.class));
        }

        try {
            serviceB.connectToNode(nodeA);
            assertThat("exception should be thrown", false, equalTo(true));
        } catch (ConnectTransportException e) {
            // all is well
        }

        try {
            serviceB.connectToNodeLight(nodeA);
            assertThat("exception should be thrown", false, equalTo(true));
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
        serviceB.registerHandler("action1", new TransportRequestHandler<TestRequest>() {
            @Override
            public TestRequest newInstance() {
                return new TestRequest();
            }

            @Override
            public void messageReceived(TestRequest request, TransportChannel channel) throws Exception {
                addressA.set(request.remoteAddress());
                channel.sendResponse(new TestResponse());
                latch.countDown();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public boolean isForceExecution() {
                return false;
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

    private static class TestRequest extends TransportRequest {
    }

    private static class TestResponse extends TransportResponse {
    }
}
