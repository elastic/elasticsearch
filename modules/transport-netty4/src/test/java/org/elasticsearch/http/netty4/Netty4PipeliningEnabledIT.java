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
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.util.Collection;
import java.util.Locale;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = Scope.TEST, supportsDedicatedMasters = false, numDataNodes = 1)
public class Netty4PipeliningEnabledIT extends ESNetty4IntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(NetworkModule.HTTP_ENABLED.getKey(), true)
            .put("http.pipelining", true)
            .build();
    }

    public void testThatNettyHttpServerSupportsPipelining() throws Exception {
        String[] requests = new String[]{"/", "/_nodes/stats", "/", "/_cluster/state", "/"};

        HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        TransportAddress[] boundAddresses = httpServerTransport.boundAddress().boundAddresses();
        InetSocketTransportAddress inetSocketTransportAddress = (InetSocketTransportAddress) randomFrom(boundAddresses);

        try (Netty4HttpClient nettyHttpClient = new Netty4HttpClient()) {
            Collection<FullHttpResponse> responses = nettyHttpClient.get(inetSocketTransportAddress.address(), requests);
            assertThat(responses, hasSize(5));

            Collection<String> opaqueIds = Netty4HttpClient.returnOpaqueIds(responses);
            assertOpaqueIdsInOrder(opaqueIds);
        }
    }

    private void assertOpaqueIdsInOrder(Collection<String> opaqueIds) {
        // check if opaque ids are monotonically increasing
        int i = 0;
        String msg = String.format(Locale.ROOT, "Expected list of opaque ids to be monotonically increasing, got [%s]", opaqueIds);
        for (String opaqueId : opaqueIds) {
            assertThat(msg, opaqueId, is(String.valueOf(i++)));
        }
    }

}
