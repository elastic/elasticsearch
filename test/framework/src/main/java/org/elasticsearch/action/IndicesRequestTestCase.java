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

package org.elasticsearch.action;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Collections.singletonList;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;

public class IndicesRequestTestCase extends ESIntegTestCase {
    private final List<String> indices = new ArrayList<>();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return singletonList(InterceptingTransportService.TestPlugin.class);
    }

    @Before
    public void setup() {
        int numIndices = iterations(1, 5);
        for (int i = 0; i < numIndices; i++) {
            indices.add("test" + i);
        }
        for (String index : indices) {
            assertAcked(prepareCreate(index).addAlias(new Alias(index + "-alias")));
        }
        ensureGreen();
    }

    @After
    public void cleanUp() {
        assertAllRequestsHaveBeenConsumed();
        indices.clear();
    }

    protected String randomIndexOrAlias() {
        String index = randomFrom(indices);
        if (randomBoolean()) {
            return index + "-alias";
        } else {
            return index;
        }
    }

    protected String[] randomIndicesOrAliases() {
        int count = randomIntBetween(1, indices.size() * 2); //every index has an alias
        String[] indices = new String[count];
        for (int i = 0; i < count; i++) {
            indices[i] = randomIndexOrAlias();
        }
        return indices;
    }

    protected String[] randomUniqueIndicesOrAliases() {
        Set<String> uniqueIndices = new HashSet<>();
        int count = randomIntBetween(1, this.indices.size());
        while (uniqueIndices.size() < count) {
            uniqueIndices.add(randomFrom(this.indices));
        }
        String[] indices = new String[count];
        int i = 0;
        for (String index : uniqueIndices) {
            indices[i++] = randomBoolean() ? index + "-alias" : index;
        }
        return indices;
    }

    protected static void assertSameIndices(IndicesRequest originalRequest, String... actions) {
        assertSameIndices(originalRequest, false, actions);
    }

    protected static void assertSameIndicesOptionalRequests(IndicesRequest originalRequest,
            String... actions) {
        assertSameIndices(originalRequest, true, actions);
    }

    protected static void assertSameIndices(IndicesRequest originalRequest, boolean optional,
            String... actions) {
        for (String action : actions) {
            List<TransportRequest> requests = consumeTransportRequests(action);
            if (!optional) {
                assertThat("no internal requests intercepted for action [" + action + "]",
                        requests.size(), greaterThan(0));
            }
            for (TransportRequest internalRequest : requests) {
                IndicesRequest indicesRequest = convertRequest(internalRequest);
                assertThat(internalRequest.getClass().getName(), indicesRequest.indices(),
                        equalTo(originalRequest.indices()));
                assertThat(indicesRequest.indicesOptions(),
                        equalTo(originalRequest.indicesOptions()));
            }
        }
    }
    protected static void assertIndicesSubset(List<String> indices, String... actions) {
        //indices returned by each bulk shard request need to be a subset of the original indices
        for (String action : actions) {
            List<TransportRequest> requests = consumeTransportRequests(action);
            assertThat("no internal requests intercepted for action [" + action + "]",
                    requests.size(), greaterThan(0));
            for (TransportRequest internalRequest : requests) {
                IndicesRequest indicesRequest = convertRequest(internalRequest);
                for (String index : indicesRequest.indices()) {
                    assertThat(indices, hasItem(index));
                }
            }
        }
    }
    private static IndicesRequest convertRequest(TransportRequest request) {
        final IndicesRequest indicesRequest;
        if (request instanceof IndicesRequest) {
            indicesRequest = (IndicesRequest) request;
        } else {
            indicesRequest = resolveRequest(request);
        }
        return indicesRequest;
    }
    private static <R extends ReplicationRequest<R>> R resolveRequest(
            TransportRequest requestOrWrappedRequest) {
        if (requestOrWrappedRequest instanceof TransportReplicationAction.ConcreteShardRequest) {
            requestOrWrappedRequest =
                    ((TransportReplicationAction.ConcreteShardRequest<?>) requestOrWrappedRequest)
                    .getRequest();
        }
        return (R) requestOrWrappedRequest;
    }

    protected static void assertAllRequestsHaveBeenConsumed() {
        withInterceptingTransportService(its ->
                assertThat(its.requests.entrySet(), emptyIterable()));
    }

    protected static void clearInterceptedActions() {
        withInterceptingTransportService(InterceptingTransportService::clearInterceptedActions);
    }

    protected static void interceptTransportActions(String... actions) {
        withInterceptingTransportService(its -> its.interceptTransportActions(actions));
    }

    protected static List<TransportRequest> consumeTransportRequests(String action) {
        List<TransportRequest> requests = new ArrayList<>();
        withInterceptingTransportService(its -> {
            List<TransportRequest> transportRequests = its.consumeRequests(action);
            if (transportRequests != null) {
                requests.addAll(transportRequests);
            }
        });
        return requests;
    }

    private static void withInterceptingTransportService(
            Consumer<InterceptingTransportService> callback) {
        for (PluginsService pluginsService : internalCluster().getInstances(PluginsService.class)) {
            InterceptingTransportService its = pluginsService
                    .filterPlugins(InterceptingTransportService.TestPlugin.class)
                    .stream().findFirst().get().instance;
            callback.accept(its);
        }

    }

    private static class InterceptingTransportService implements TransportInterceptor {
        public static class TestPlugin extends Plugin implements NetworkPlugin {
            public final InterceptingTransportService instance = new InterceptingTransportService();
            @Override
            public List<TransportInterceptor> getTransportInterceptors(
                    NamedWriteableRegistry namedWriteableRegistry, ThreadContext threadContext) {
                return Collections.singletonList(instance);
            }
        }

        private final Set<String> actions = new HashSet<>();

        private final Map<String, List<TransportRequest>> requests = new HashMap<>();

        @Override
        public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
                String action, String executor, boolean forceExecution,
                TransportRequestHandler<T> actualHandler) {
            return new InterceptingRequestHandler<>(action, actualHandler);
        }

        synchronized List<TransportRequest> consumeRequests(String action) {
            return requests.remove(action);
        }

        synchronized void interceptTransportActions(String... actions) {
            Collections.addAll(this.actions, actions);
        }

        synchronized void clearInterceptedActions() {
            actions.clear();
        }


        private class InterceptingRequestHandler<T extends TransportRequest>
                implements TransportRequestHandler<T> {
            private final TransportRequestHandler<T> requestHandler;
            private final String action;

            InterceptingRequestHandler(String action, TransportRequestHandler<T> requestHandler) {
                this.requestHandler = requestHandler;
                this.action = action;
            }

            @Override
            public void messageReceived(T request, TransportChannel channel, Task task)
                    throws Exception {
                synchronized (InterceptingTransportService.this) {
                    if (actions.contains(action)) {
                        List<TransportRequest> requestList = requests.get(action);
                        if (requestList == null) {
                            requestList = new ArrayList<>();
                            requestList.add(request);
                            requests.put(action, requestList);
                        } else {
                            requestList.add(request);
                        }
                    }
                }
                requestHandler.messageReceived(request, channel, task);
            }

            @Override
            public void messageReceived(T request, TransportChannel channel) throws Exception {
                messageReceived(request, channel, null);
            }
        }
    }
}
