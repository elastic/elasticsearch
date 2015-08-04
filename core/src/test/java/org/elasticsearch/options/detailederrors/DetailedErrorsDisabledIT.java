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

package org.elasticsearch.options.detailederrors;

import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.netty.NettyHttpServerTransport;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.rest.client.http.HttpDeleteWithEntity;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

/**
 * Tests that when disabling detailed errors, a request with the error_trace parameter returns a HTTP 400
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 1)
public class DetailedErrorsDisabledIT extends ESIntegTestCase {

    // Build our cluster settings
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(Node.HTTP_ENABLED, true)
                .put(NettyHttpServerTransport.SETTING_HTTP_DETAILED_ERRORS_ENABLED, false)
                .build();
    }

    @Test
    public void testThatErrorTraceParamReturns400() throws Exception {
        // Make the HTTP request
        HttpResponse response = new HttpRequestBuilder(HttpClients.createDefault())
                .httpTransport(internalCluster().getDataNodeInstance(HttpServerTransport.class))
                .addParam("error_trace", "true")
                .method(HttpDeleteWithEntity.METHOD_NAME)
                .execute();

        assertThat(response.getHeaders().get("Content-Type"), is("application/json"));
        assertThat(response.getBody(), is("{\"error\":\"error traces in responses are disabled.\"}"));
        assertThat(response.getStatusCode(), is(400));
    }
}
