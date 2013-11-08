/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.transport.local.LocalTransport;

import java.io.IOException;
import java.util.Random;

/**
 *
 */
public class AssertingLocalTransport extends LocalTransport {
    private final Random random;

    @Inject
    public AssertingLocalTransport(Settings settings, ThreadPool threadPool, Version version) {
        super(settings, threadPool, version);
        final long seed = settings.getAsLong(ElasticsearchIntegrationTest.INDEX_SEED_SETTING, 0l);
        random = new Random(seed);
    }

    @Override
    protected void handleParsedRespone(final TransportResponse response, final TransportResponseHandler handler) {
        ElasticsearchAssertions.assertVersionSerializable(ElasticsearchTestCase.randomVersion(random), response);
        super.handleParsedRespone(response, handler);
    }
    
    @Override
    public void sendRequest(final DiscoveryNode node, final long requestId, final String action, final TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
        ElasticsearchAssertions.assertVersionSerializable(ElasticsearchTestCase.randomVersion(random), request);
        super.sendRequest(node, requestId, action, request, options);
    }
}
