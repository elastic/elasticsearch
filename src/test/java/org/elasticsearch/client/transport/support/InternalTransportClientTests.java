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

package org.elasticsearch.client.transport.support;

import org.elasticsearch.Version;
import org.elasticsearch.action.*;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.FailAndRetryMockTransport;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClientNodesService;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public class InternalTransportClientTests extends ElasticsearchTestCase {

    private static class TestIteration implements Closeable {
        private final ThreadPool threadPool;
        private final FailAndRetryMockTransport<TestResponse> transport;
        private final TransportService transportService;
        private final TransportClientNodesService transportClientNodesService;
        private final InternalTransportClient internalTransportClient;
        private final int nodesCount;

        TestIteration() {
            threadPool = new ThreadPool("internal-transport-client-tests");
            transport = new FailAndRetryMockTransport<TestResponse>(getRandom()) {
                @Override
                protected TestResponse newResponse() {
                    return new TestResponse();
                }
            };
            transportService = new TransportService(ImmutableSettings.EMPTY, transport, threadPool);
            transportService.start();
            transportClientNodesService = new TransportClientNodesService(ImmutableSettings.EMPTY, ClusterName.DEFAULT, transportService, threadPool, Version.CURRENT);
            Map<String, GenericAction> actions = new HashMap<>();
            actions.put(TestAction.NAME, TestAction.INSTANCE);
            actions.put(NodesInfoAction.NAME, NodesInfoAction.INSTANCE);
            internalTransportClient = new InternalTransportClient(ImmutableSettings.EMPTY, threadPool, transportService,
                    transportClientNodesService, null, actions);

            nodesCount = randomIntBetween(1, 10);
            for (int i = 0; i < nodesCount; i++) {
                transportClientNodesService.addTransportAddresses(new LocalTransportAddress("node" + i));
            }
            transport.endConnectMode();
        }

        public void close() {
            threadPool.shutdown();
            try {
                threadPool.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().isInterrupted();
            }
            transportService.stop();
            transportClientNodesService.close();
            internalTransportClient.close();
        }
    }

    @Test
    public void testListenerFailures() throws InterruptedException {

        int iters = iterations(10, 100);
        for (int i = 0; i < iters; i++) {
            try(final TestIteration iteration = new TestIteration()) {
                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger finalFailures = new AtomicInteger();
                final AtomicReference<Throwable> finalFailure = new AtomicReference<>();
                final AtomicReference<TestResponse> response = new AtomicReference<>();
                ActionListener<TestResponse> actionListener = new ActionListener<TestResponse>() {
                    @Override
                    public void onResponse(TestResponse testResponse) {
                        response.set(testResponse);
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        finalFailures.incrementAndGet();
                        finalFailure.set(e);
                        latch.countDown();
                    }
                };

                final AtomicInteger preSendFailures = new AtomicInteger();

                iteration.internalTransportClient.execute(TestAction.INSTANCE, new TestRequest(), actionListener);

                assertThat(latch.await(1, TimeUnit.SECONDS), equalTo(true));

                //there can be only either one failure that causes the request to fail straightaway or success
                assertThat(preSendFailures.get() + iteration.transport.failures() + iteration.transport.successes(), lessThanOrEqualTo(1));

                if (iteration.transport.successes() == 1) {
                    assertThat(finalFailures.get(), equalTo(0));
                    assertThat(finalFailure.get(), nullValue());
                    assertThat(response.get(), notNullValue());
                } else {
                    assertThat(finalFailures.get(), equalTo(1));
                    assertThat(finalFailure.get(), notNullValue());
                    assertThat(response.get(), nullValue());
                    if (preSendFailures.get() == 0 && iteration.transport.failures() == 0) {
                        assertThat(finalFailure.get(), instanceOf(NoNodeAvailableException.class));
                    }
                }

                assertThat(iteration.transport.triedNodes().size(), lessThanOrEqualTo(iteration.nodesCount));
                assertThat(iteration.transport.triedNodes().size(), equalTo(iteration.transport.connectTransportExceptions() + iteration.transport.failures() + iteration.transport.successes()));
            }
        }
    }

    @Test
    public void testSyncFailures() throws InterruptedException {

        int iters = iterations(10, 100);
        for (int i = 0; i < iters; i++) {
            try(final TestIteration iteration = new TestIteration()) {
                TestResponse testResponse = null;
                Throwable finalFailure = null;

                try {
                    ActionFuture<TestResponse> future = iteration.internalTransportClient.execute(TestAction.INSTANCE, new TestRequest());
                    testResponse = future.get();
                } catch (Throwable t) {
                    finalFailure = t;
                }

                //there can be only either one failure that causes the request to fail straightaway or success
                assertThat(iteration.transport.failures() + iteration.transport.successes(), lessThanOrEqualTo(1));

                if (iteration.transport.successes() == 1) {
                    assertThat(finalFailure, nullValue());
                    assertThat(testResponse, notNullValue());
                } else {
                    assertThat(testResponse, nullValue());
                    assertThat(finalFailure, notNullValue());
                    if (iteration.transport.failures() == 0) {
                        assertThat(finalFailure, instanceOf(NoNodeAvailableException.class));
                    }
                }

                assertThat(iteration.transport.triedNodes().size(), lessThanOrEqualTo(iteration.nodesCount));
                assertThat(iteration.transport.triedNodes().size(), equalTo(iteration.transport.connectTransportExceptions() + iteration.transport.failures() + iteration.transport.successes()));
            }
        }
    }

    private static class TestRequest extends ActionRequest {
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    private static class TestResponse extends ActionResponse {

    }

    private static class TestAction extends ClientAction<TestRequest, TestResponse, TestRequestBuilder> {
        static final String NAME = "test-action";
        static final TestAction INSTANCE = new TestAction(NAME);

        private TestAction(String name) {
            super(name);
        }

        @Override
        public TestRequestBuilder newRequestBuilder(Client client) {
            //not needed
            return null;
        }

        @Override
        public TestResponse newResponse() {
            return new TestResponse();
        }
    }

    private static class TestRequestBuilder extends ActionRequestBuilder<TestRequest, TestResponse, TestRequestBuilder, Client> {

        protected TestRequestBuilder(Client client, TestRequest request) {
            super(client, request);
        }

        @Override
        protected void doExecute(ActionListener<TestResponse> listener) {
            //not needed
        }
    }
}
