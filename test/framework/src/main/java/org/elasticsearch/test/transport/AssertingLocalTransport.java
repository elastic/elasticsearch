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

package org.elasticsearch.test.transport;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.local.LocalTransport;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class AssertingLocalTransport extends LocalTransport {

    public static class TestPlugin extends Plugin {
        public void onModule(NetworkModule module) {
            module.registerTransport("mock", AssertingLocalTransport.class);
        }
        @Override
        public Settings additionalSettings() {
            return Settings.builder().put(NetworkModule.TRANSPORT_TYPE_KEY, "mock").build();
        }

        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(ASSERTING_TRANSPORT_MIN_VERSION_KEY, ASSERTING_TRANSPORT_MAX_VERSION_KEY);
        }
    }

    public static final Setting<Version> ASSERTING_TRANSPORT_MIN_VERSION_KEY =
        new Setting<>("transport.asserting.version.min", Integer.toString(Version.CURRENT.minimumCompatibilityVersion().id),
            (s) -> Version.fromId(Integer.parseInt(s)), Property.NodeScope);
    public static final Setting<Version> ASSERTING_TRANSPORT_MAX_VERSION_KEY =
        new Setting<>("transport.asserting.version.max", Integer.toString(Version.CURRENT.id),
            (s) -> Version.fromId(Integer.parseInt(s)), Property.NodeScope);
    private final Random random;
    private final Version minVersion;
    private final Version maxVersion;

    @Inject
    public AssertingLocalTransport(Settings settings, CircuitBreakerService circuitBreakerService, ThreadPool threadPool,
                                   Version version, NamedWriteableRegistry namedWriteableRegistry) {
        super(settings, threadPool, version, namedWriteableRegistry, circuitBreakerService);
        final long seed = ESIntegTestCase.INDEX_TEST_SEED_SETTING.get(settings);
        random = new Random(seed);
        minVersion = ASSERTING_TRANSPORT_MIN_VERSION_KEY.get(settings);
        maxVersion = ASSERTING_TRANSPORT_MAX_VERSION_KEY.get(settings);
    }

    @Override
    protected void handleParsedResponse(final TransportResponse response, final TransportResponseHandler handler) {
        ElasticsearchAssertions.assertVersionSerializable(VersionUtils.randomVersionBetween(random, minVersion, maxVersion), response,
                namedWriteableRegistry);
        super.handleParsedResponse(response, handler);
    }

    @Override
    public void sendRequest(final DiscoveryNode node, final long requestId, final String action, final TransportRequest request,
                            TransportRequestOptions options) throws IOException, TransportException {
        ElasticsearchAssertions.assertVersionSerializable(VersionUtils.randomVersionBetween(random, minVersion, maxVersion), request,
                namedWriteableRegistry);
        super.sendRequest(node, requestId, action, request, options);
    }
}
