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
package org.elasticsearch.http.netty4;

import io.netty.handler.codec.http.FullHttpResponse;
import org.elasticsearch.ESNetty4IntegTestCase;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

@ClusterScope(scope = Scope.TEST, supportsDedicatedMasters = false, numDataNodes = 1)
public class Netty4PipeliningDisabledIT extends ESNetty4IntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(NetworkModule.HTTP_ENABLED.getKey(), true)
            .put("http.pipelining", false)
            .build();
    }

    public void testThatNettyHttpServerDoesNotSupportPipelining() throws Exception {
        ensureGreen();
        String[] requests = new String[] {"/", "/_nodes/stats", "/", "/_cluster/state", "/", "/_nodes", "/"};

        HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        TransportAddress[] boundAddresses = httpServerTransport.boundAddress().boundAddresses();
        TransportAddress transportAddress = (TransportAddress) randomFrom(boundAddresses);

        try (Netty4HttpClient nettyHttpClient = new Netty4HttpClient()) {
            Collection<FullHttpResponse> responses = nettyHttpClient.get(transportAddress.address(), requests);
            assertThat(responses, hasSize(requests.length));

            List<String> opaqueIds = new ArrayList<>(Netty4HttpClient.returnOpaqueIds(responses));

            assertResponsesOutOfOrder(opaqueIds);
        }
    }

    /**
     * checks if all responses are there, but also tests that they are out of order because pipelining is disabled
     */
    private void assertResponsesOutOfOrder(List<String> opaqueIds) {
        String message = String.format(Locale.ROOT, "Expected returned http message ids to be in any order of: %s", opaqueIds);
        assertThat(message, opaqueIds, containsInAnyOrder("0", "1", "2", "3", "4", "5", "6"));
    }

}
