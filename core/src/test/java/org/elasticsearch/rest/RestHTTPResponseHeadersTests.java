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

package org.elasticsearch.rest;

import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

/**
 * Refer to
 * <a href="https://github.com/elastic/elasticsearch/issues/15335">Unsupported
 * methods on REST endpoints should respond with status code 405</a> for more
 * information.
 */
public class RestHTTPResponseHeadersTests extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(NetworkModule.HTTP_ENABLED.getKey(), true).put(super.nodeSettings()).build();
    }

    /**
     * For an OPTIONS request to a valid REST endpoint, verify that a 200 HTTP
     * response code is returned, and that the response 'Allow' header includes
     * a list of valid HTTP methods for the endpoint (see
     * <a href="https://tools.ietf.org/html/rfc2616#section-9.2">HTTP/1.1 - 9.2
     * - Options</a>).
     */
    public void testValidEndpointOptionsResponseHTTPHeader() throws Exception {
        createIndex("test");
        HttpResponse httpResponse = httpClient().method("OPTIONS").path("/test").execute();
        assertThat(httpResponse.getStatusCode(), is(200));
        assertThat(httpResponse.getHeaders().get("Allow"), notNullValue());
        List<String> allowHeader = Arrays.asList(httpResponse.getHeaders().get("Allow").split(","));
        assertThat(allowHeader, containsInAnyOrder("HEAD", "GET", "PUT", "POST", "DELETE"));
    }

    /**
     * For requests to a valid REST endpoint using an unsupported HTTP method,
     * verify that a 405 HTTP response code is returned, and that the response
     * 'Allow' header includes a list of valid HTTP methods for the endpoint
     * (see
     * <a href="https://tools.ietf.org/html/rfc2616#section-10.4.6">HTTP/1.1 -
     * 10.4.6 - 405 Method Not Allowed</a>).
     */
    public void testUnsupportedMethodResponseHTTPHeader() throws Exception {
        createIndex("test");
        HttpResponse httpResponse = httpClient().method("DELETE").path("/test/_analyze").execute();
        assertThat(httpResponse.getStatusCode(), is(405));
        assertThat(httpResponse.getHeaders().get("Allow"), notNullValue());
        List<String> allowHeader = Arrays.asList(httpResponse.getHeaders().get("Allow").split(","));
        assertThat(allowHeader, containsInAnyOrder("HEAD", "GET", "POST"));
    }

    private HttpRequestBuilder httpClient() {
        final NodesInfoResponse nodeInfos = client().admin().cluster().prepareNodesInfo().get();
        final NodeInfo[] nodes = nodeInfos.getNodes();
        assertTrue(nodes.length > 0);
        TransportAddress publishAddress = nodes[0].getHttp().address().publishAddress();
        assertEquals(1, publishAddress.uniqueAddressTypeId());
        InetSocketAddress address = ((InetSocketTransportAddress) publishAddress).address();
        return new HttpRequestBuilder(HttpClients.createDefault()).host(NetworkAddress.format(address.getAddress()))
                .port(address.getPort());
    }

}
