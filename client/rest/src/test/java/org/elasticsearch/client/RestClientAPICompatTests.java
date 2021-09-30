/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

public class RestClientAPICompatTests extends RestClientTestCase {

    private static HttpServer httpServer;

    @BeforeClass
    public static void startHttpServer() throws Exception {
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.createContext("/", new APIHeaderHandler());
        httpServer.start();
    }

    @AfterClass
    public static void stopHttpServers() {
        httpServer.stop(0);
        httpServer = null;
    }

    private static class APIHeaderHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {

            // Decode body (if any)
            String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
            String accept = exchange.getRequestHeaders().getFirst("Accept");

            ByteArrayOutputStream bao = new ByteArrayOutputStream();

            // Outputs <content-type|null>#<accept|null>
            bao.write(String.valueOf(contentType).getBytes(StandardCharsets.UTF_8));
            bao.write('#');
            bao.write(String.valueOf(accept).getBytes(StandardCharsets.UTF_8));
            bao.close();

            byte[] bytes = bao.toByteArray();

            exchange.sendResponseHeaders(200, bytes.length);

            exchange.getResponseBody().write(bytes);
            exchange.close();
        }
    }

    /** Read all bytes of an input stream and close it. */
    private static byte[] readAll(InputStream in) throws IOException {
        byte[] buffer = new byte[1024];
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int len = 0;
        while ((len = in.read(buffer)) > 0) {
            bos.write(buffer, 0, len);
        }
        in.close();
        return bos.toByteArray();
    }

    private RestClient createClient(boolean apiCompat) {
        InetSocketAddress address = httpServer.getAddress();
        return RestClient.builder(new HttpHost(address.getHostString(), address.getPort(), "http"))
            .setAPICompatibilityMode(apiCompat)
            .build();
    }

    public void testAPICompatOff() throws Exception {
        RestClient restClient = createClient(false);

        Request request = new Request("GET", "/");
        request.setEntity(new StringEntity("{}", ContentType.APPLICATION_JSON));

        Response response = restClient.performRequest(request);

        Assert.assertTrue(response.getEntity().getContentLength() > 0);
        checkResponse("application/json; charset=UTF-8#null", response);

        request = new Request("GET", "/");
        request.setEntity(new StringEntity("aoeu", ContentType.TEXT_PLAIN));

        response = restClient.performRequest(request);

        Assert.assertTrue(response.getEntity().getContentLength() > 0);
        checkResponse("text/plain; charset=ISO-8859-1#null", response);

        restClient.close();
    }

    public void testAPICompatOn() throws Exception {
        RestClient restClient = createClient(true);

        Request request = new Request("POST", "/");
        request.setEntity(new StringEntity("{}", ContentType.APPLICATION_JSON));

        Response response = restClient.performRequest(request);

        Assert.assertTrue(response.getEntity().getContentLength() > 0);
        checkResponse("application/vnd.elasticsearch+json; compatible-with=7; charset=UTF-8" +
                "#application/vnd.elasticsearch+json; compatible-with=7",
            response);

        // Test with no entity, the default header should still be added
        request = new Request("GET", "/");
        response = restClient.performRequest(request);
        Assert.assertTrue(response.getEntity().getContentLength() > 0);
        checkResponse("null#application/vnd.elasticsearch+json; compatible-with=7",
            response);

        restClient.close();
    }

    private static void checkResponse(String expected, Response response) throws Exception {
        HttpEntity entity = response.getEntity();
        Assert.assertNotNull(entity);

        String content = new String(readAll(entity.getContent()), StandardCharsets.UTF_8);
        assertThat(content, containsString(expected));
    }
}
