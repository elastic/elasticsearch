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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 * This test checks that in-flight requests are limited on HTTP level and that requests that are excluded from limiting can pass.
 *
 * As the same setting is also used to limit in-flight requests on transport level, we avoid transport messages by forcing
 * a single node "cluster". We also force test infrastructure to use the node client instead of the transport client for the same reason.
 */
@ClusterScope(scope = Scope.TEST, supportsDedicatedMasters = false, numClientNodes = 0, numDataNodes = 1, transportClientRatio = 0)
public class Netty4HttpRequestSizeLimitIT extends ESNetty4IntegTestCase {

    private static final ByteSizeValue LIMIT = new ByteSizeValue(2, ByteSizeUnit.KB);

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(NetworkModule.HTTP_ENABLED.getKey(), true)
            .put(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), LIMIT)
            .build();
    }

    public void testLimitsInFlightRequests() throws Exception {
        ensureGreen();

        // we use the limit size as a (very) rough indication on how many requests we should sent to hit the limit
        int numRequests = LIMIT.bytesAsInt() / 100;

        StringBuilder bulkRequest = new StringBuilder();
        for (int i = 0; i < numRequests; i++) {
            bulkRequest.append("{\"index\": {}}");
            bulkRequest.append(System.lineSeparator());
            bulkRequest.append("{ \"field\" : \"value\" }");
            bulkRequest.append(System.lineSeparator());
        }

        @SuppressWarnings("unchecked")
        Tuple<String, CharSequence>[] requests = new Tuple[150];
        for (int i = 0; i < requests.length; i++) {
            requests[i] = Tuple.tuple("/index/type/_bulk", bulkRequest);
        }

        HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        TransportAddress transportAddress = (TransportAddress) randomFrom(httpServerTransport.boundAddress
            ().boundAddresses());

        try (Netty4HttpClient nettyHttpClient = new Netty4HttpClient()) {
            Collection<FullHttpResponse> singleResponse = nettyHttpClient.post(transportAddress.address(), requests[0]);
            assertThat(singleResponse, hasSize(1));
            assertAtLeastOnceExpectedStatus(singleResponse, HttpResponseStatus.OK);

            Collection<FullHttpResponse> multipleResponses = nettyHttpClient.post(transportAddress.address(), requests);
            assertThat(multipleResponses, hasSize(requests.length));
            assertAtLeastOnceExpectedStatus(multipleResponses, HttpResponseStatus.SERVICE_UNAVAILABLE);
        }
    }

    public void testDoesNotLimitExcludedRequests() throws Exception {
        ensureGreen();

        @SuppressWarnings("unchecked")
        Tuple<String, CharSequence>[] requestUris = new Tuple[1500];
        for (int i = 0; i < requestUris.length; i++) {
            requestUris[i] = Tuple.tuple("/_cluster/settings",
                "{ \"transient\": {\"search.default_search_timeout\": \"40s\" } }");
        }

        HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        TransportAddress transportAddress = (TransportAddress) randomFrom(httpServerTransport.boundAddress
            ().boundAddresses());

        try (Netty4HttpClient nettyHttpClient = new Netty4HttpClient()) {
            Collection<FullHttpResponse> responses = nettyHttpClient.put(transportAddress.address(), requestUris);
            assertThat(responses, hasSize(requestUris.length));
            assertAllInExpectedStatus(responses, HttpResponseStatus.OK);
        }
    }

    private void assertAtLeastOnceExpectedStatus(Collection<FullHttpResponse> responses, HttpResponseStatus expectedStatus) {
        long countExpectedStatus = responses.stream().filter(r -> r.status().equals(expectedStatus)).count();
        assertThat("Expected at least one request with status [" + expectedStatus + "]", countExpectedStatus, greaterThan(0L));
    }

    private void assertAllInExpectedStatus(Collection<FullHttpResponse> responses, HttpResponseStatus expectedStatus) {
        long countUnexpectedStatus = responses.stream().filter(r -> r.status().equals(expectedStatus) == false).count();
        assertThat("Expected all requests with status [" + expectedStatus + "] but [" + countUnexpectedStatus +
            "] requests had a different one", countUnexpectedStatus, equalTo(0L));
    }

}
