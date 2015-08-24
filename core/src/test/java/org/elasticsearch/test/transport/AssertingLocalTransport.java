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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.local.LocalTransport;

import java.io.IOException;
import java.util.Random;

public class AssertingLocalTransport extends LocalTransport {

    public static class TestPlugin extends Plugin {
        @Override
        public String name() {
            return "asserting-local-transport";
        }
        @Override
        public String description() {
            return "an asserting transport for testing";
        }
        public void onModule(TransportModule transportModule) {
            transportModule.addTransport("mock", AssertingLocalTransport.class);
        }
        @Override
        public Settings additionalSettings() {
            return Settings.builder().put(TransportModule.TRANSPORT_TYPE_KEY, "mock").build();
        }
    }

    public static final String ASSERTING_TRANSPORT_MIN_VERSION_KEY = "transport.asserting.version.min";
    public static final String ASSERTING_TRANSPORT_MAX_VERSION_KEY = "transport.asserting.version.max";
    private final Random random;
    private final Version minVersion;
    private final Version maxVersion;

    @Inject
    public AssertingLocalTransport(Settings settings, ThreadPool threadPool, Version version, NamedWriteableRegistry namedWriteableRegistry) {
        super(settings, threadPool, version, namedWriteableRegistry);
        final long seed = settings.getAsLong(ESIntegTestCase.SETTING_INDEX_SEED, 0l);
        random = new Random(seed);
        minVersion = settings.getAsVersion(ASSERTING_TRANSPORT_MIN_VERSION_KEY, Version.V_0_18_0);
        maxVersion = settings.getAsVersion(ASSERTING_TRANSPORT_MAX_VERSION_KEY, Version.CURRENT);
    }

    @Override
    protected void handleParsedResponse(final TransportResponse response, final TransportResponseHandler handler) {
        ElasticsearchAssertions.assertVersionSerializable(VersionUtils.randomVersionBetween(random, minVersion, maxVersion), response);
        super.handleParsedResponse(response, handler);
    }
    
    @Override
    public void sendRequest(final DiscoveryNode node, final long requestId, final String action, final TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
        ElasticsearchAssertions.assertVersionSerializable(VersionUtils.randomVersionBetween(random, minVersion, maxVersion), request);
        super.sendRequest(node, requestId, action, request, options);
    }
}
