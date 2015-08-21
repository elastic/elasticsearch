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

package org.elasticsearch.example;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ExternalTestCluster;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.test.rest.client.RestResponse;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * verifies content is actually served for the site plugin
 */
public class SiteContentsIT extends ESIntegTestCase {

    // TODO: probably a better way to test, but we don't want to really
    // define a fake rest spec or anything?
    public void test() throws Exception {
        TestCluster cluster = cluster();
        assumeTrue("this test will not work from an IDE unless you pass tests.cluster pointing to a running instance", cluster instanceof ExternalTestCluster);
        ExternalTestCluster externalCluster = (ExternalTestCluster) cluster;
        try (CloseableHttpClient httpClient = HttpClients.createMinimal(new PoolingHttpClientConnectionManager(15, TimeUnit.SECONDS))) {
            for (InetSocketAddress address :  externalCluster.httpAddresses()) {
                RestResponse restResponse = new RestResponse(
                        new HttpRequestBuilder(httpClient)
                        .host(NetworkAddress.formatAddress(address.getAddress())).port(address.getPort())
                        .path("/_plugin/site-example/")
                        .method("GET").execute());
                assertEquals(200, restResponse.getStatusCode());
                String body = restResponse.getBodyAsString();
                assertTrue("unexpected body contents: " + body, body.contains("<body>Page body</body>"));
            }
        }
    }
}
