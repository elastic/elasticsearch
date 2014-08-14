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

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.Matchers.instanceOf;

/**
 *
 */
@ClusterScope(scope= Scope.SUITE, numDataNodes =0)
public class TransportServiceExtensibilityTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put("transport.service.type", AnotherTransportService.class.getName())
                .build();
    }

    @Test
    public void test() throws Exception {
        String target = internalCluster().startNode();
        String source = internalCluster().startNode();

        TransportService service = internalCluster().getInstance(TransportService.class, target);
        assertThat(service, instanceOf(AnotherTransportService.class));
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, target);
        DiscoveryNode targetNode = clusterService.localNode();
        service.registerHandler("_action", new TransportRequestHandler<Request>() {
            @Override
            public Request newInstance() {
                return new Request();
            }

            @Override
            public void messageReceived(Request request, TransportChannel channel) throws Exception {
                channel.sendResponse(new Response());
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

        final AtomicReference<Response> responseHolder = new AtomicReference<>(null);
        final AtomicReference<TransportException> errorHolder = new AtomicReference<>(null);
        final CountDownLatch latch = new CountDownLatch(1);

        service = internalCluster().getInstance(TransportService.class, source);
        assertThat(service, instanceOf(AnotherTransportService.class));
        service.sendRequest(targetNode, "_action", new Request(), new TransportResponseHandler<Response>() {
            @Override
            public Response newInstance() {
                return new Response();
            }

            @Override
            public void handleResponse(Response response) {
                responseHolder.set(response);
                latch.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                errorHolder.set(exp);
                latch.countDown();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        });

        if (!latch.await(5, TimeUnit.MINUTES)) {
            fail("waiting too much time for response");
        }

    }

    public static class AnotherTransportService extends TransportService {

        @Inject
        public AnotherTransportService(Settings settings, Transport transport, ThreadPool threadPool) {
            super(settings, transport, threadPool);
        }

        @Override
        protected Adapter<AnotherTransportService> createAdapter() {
            return new AnotherAdapter(this);
        }

        protected static class AnotherAdapter extends Adapter<AnotherTransportService> {

            public AnotherAdapter(AnotherTransportService service) {
                super(service);
            }


        }
    }

    static class Request extends TransportRequest {
    }

    static class Response extends TransportResponse {
    }

}
