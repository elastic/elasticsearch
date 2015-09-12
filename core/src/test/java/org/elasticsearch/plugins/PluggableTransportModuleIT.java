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
package org.elasticsearch.plugins;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.AssertingLocalTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import static org.elasticsearch.test.ESIntegTestCase.Scope;
import static org.hamcrest.Matchers.*;

/**
 *
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 2)
public class PluggableTransportModuleIT extends ESIntegTestCase {

    public static final AtomicInteger SENT_REQUEST_COUNTER = new AtomicInteger(0);

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(DiscoveryModule.DISCOVERY_TYPE_KEY, "local")
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(CountingSentRequestsPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return pluginList(CountingSentRequestsPlugin.class);
    }

    @Test
    public void testThatPluginFunctionalityIsLoadedWithoutConfiguration() throws Exception {
        for (Transport transport : internalCluster().getInstances(Transport.class)) {
            assertThat(transport, instanceOf(CountingAssertingLocalTransport.class));
        }

        int countBeforeRequest = SENT_REQUEST_COUNTER.get();
        internalCluster().clientNodeClient().admin().cluster().prepareHealth().get();
        int countAfterRequest = SENT_REQUEST_COUNTER.get();
        assertThat("Expected send request counter to be greather than zero", countAfterRequest, is(greaterThan(countBeforeRequest)));
    }

    public static class CountingSentRequestsPlugin extends Plugin {
        @Override
        public String name() {
            return "counting-pipelines-plugin";
        }

        @Override
        public String description() {
            return "counting-pipelines-plugin";
        }

        public void onModule(TransportModule transportModule) {
            transportModule.setTransport(CountingAssertingLocalTransport.class, this.name());
        }
    }

    public static final class CountingAssertingLocalTransport extends AssertingLocalTransport {

        @Inject
        public CountingAssertingLocalTransport(Settings settings, ThreadPool threadPool, Version version, NamedWriteableRegistry namedWriteableRegistry) {
            super(settings, threadPool, version, namedWriteableRegistry);
        }

        @Override
        public void sendRequest(final DiscoveryNode node, final long requestId, final String action, final TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
            SENT_REQUEST_COUNTER.incrementAndGet();
            super.sendRequest(node, requestId, action, request, options);
        }
    }
}
