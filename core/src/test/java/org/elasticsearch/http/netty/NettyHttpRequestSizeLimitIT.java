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
package org.elasticsearch.http.netty;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 *
 */
@ClusterScope(scope = Scope.TEST, numDedicatedMasterNodes = 0, numDataNodes = 1)
public class NettyHttpRequestSizeLimitIT extends ESIntegTestCase {
    private static final ByteSizeValue LIMIT = new ByteSizeValue(2, ByteSizeUnit.KB);

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(NetworkModule.HTTP_ENABLED.getKey(), true)
            .put(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), LIMIT)
            .build();
    }

    @TestLogging("_root:DEBUG,org.elasticsearch.common.breaker:TRACE,org.elasticsearch.test:TRACE,org.elasticsearch.transport:TRACE")
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
        InetSocketTransportAddress inetSocketTransportAddress = (InetSocketTransportAddress) randomFrom(httpServerTransport.boundAddress
            ().boundAddresses());

        try (NettyHttpClient nettyHttpClient = new NettyHttpClient()) {
            Collection<HttpResponse> singleResponse = nettyHttpClient.post(inetSocketTransportAddress.address(), requests[0]);
            assertThat(singleResponse, hasSize(1));
            assertAtLeastOnceExpectedStatus(singleResponse, HttpResponseStatus.OK);

            Collection<HttpResponse> multipleResponses = nettyHttpClient.post(inetSocketTransportAddress.address(), requests);
            assertThat(multipleResponses, hasSize(requests.length));
            assertAtLeastOnceExpectedStatus(multipleResponses, HttpResponseStatus.SERVICE_UNAVAILABLE);
        }
    }

    private void assertAtLeastOnceExpectedStatus(Collection<HttpResponse> responses, HttpResponseStatus expectedStatus) {
        long countResponseErrors = responses.stream().filter(r -> r.getStatus().equals(expectedStatus)).count();
        assertThat(countResponseErrors, greaterThan(0L));

    }
}
