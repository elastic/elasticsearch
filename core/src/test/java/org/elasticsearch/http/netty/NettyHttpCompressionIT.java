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

import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.client.http.HttpResponse;

import java.io.IOException;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 1)
public class NettyHttpCompressionIT extends ESIntegTestCase {
    private static final String GZIP_ENCODING = "gzip";

    private static final String SAMPLE_DOCUMENT = "{\n" +
        "   \"name\": {\n" +
        "      \"first name\": \"Steve\",\n" +
        "      \"last name\": \"Jobs\"\n" +
        "   }\n" +
        "}";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(NetworkModule.HTTP_ENABLED.getKey(), true)
            .put(HttpTransportSettings.SETTING_HTTP_COMPRESSION.getKey(), true)
            .build();
    }

    public void testCompressesResponseIfRequested() throws Exception {
        ensureGreen();

        // we need to intercept early, otherwise internal logic in HttpClient will just remove the header and we cannot verify it
        ContentEncodingHeaderExtractor headerExtractor = new ContentEncodingHeaderExtractor();
        CloseableHttpClient internalClient = HttpClients.custom().addInterceptorFirst(headerExtractor).build();

        HttpResponse response = httpClient(internalClient).path("/").addHeader(HttpHeaders.ACCEPT_ENCODING, GZIP_ENCODING).execute();
        assertEquals(200, response.getStatusCode());
        assertTrue(headerExtractor.hasContentEncodingHeader());
        assertEquals(GZIP_ENCODING, headerExtractor.getContentEncodingHeader().getValue());
    }

    public void testUncompressedResponseByDefault() throws Exception {
        ensureGreen();

        ContentEncodingHeaderExtractor headerExtractor = new ContentEncodingHeaderExtractor();
        CloseableHttpClient internalClient = HttpClients
            .custom()
            .disableContentCompression()
            .addInterceptorFirst(headerExtractor)
            .build();

        HttpResponse response = httpClient(internalClient).path("/").execute();
        assertEquals(200, response.getStatusCode());
        assertFalse(headerExtractor.hasContentEncodingHeader());
    }

    public void testCanInterpretUncompressedRequest() throws Exception {
        ensureGreen();

        ContentEncodingHeaderExtractor headerExtractor = new ContentEncodingHeaderExtractor();
        CloseableHttpClient internalClient = HttpClients
            .custom()
            // this disable content compression in both directions (request and response)
            .disableContentCompression()
            .addInterceptorFirst(headerExtractor)
            .build();

        HttpResponse response = httpClient(internalClient)
            .path("/company/employees/1")
            .method("POST")
            .body(SAMPLE_DOCUMENT)
            .execute();

        assertEquals(201, response.getStatusCode());
        assertFalse(headerExtractor.hasContentEncodingHeader());
    }

    public void testCanInterpretCompressedRequest() throws Exception {
        ensureGreen();

        ContentEncodingHeaderExtractor headerExtractor = new ContentEncodingHeaderExtractor();
        // we don't call #disableContentCompression() hence the client will send the content compressed
        CloseableHttpClient internalClient = HttpClients.custom().addInterceptorFirst(headerExtractor).build();

        HttpResponse response = httpClient(internalClient)
            .path("/company/employees/2")
            .method("POST")
            .body(SAMPLE_DOCUMENT)
            .execute();

        assertEquals(201, response.getStatusCode());
        assertTrue(headerExtractor.hasContentEncodingHeader());
        assertEquals(GZIP_ENCODING, headerExtractor.getContentEncodingHeader().getValue());
    }

    private static class ContentEncodingHeaderExtractor implements HttpResponseInterceptor {
        private Header contentEncodingHeader;

        @Override
        public void process(org.apache.http.HttpResponse response, HttpContext context) throws HttpException, IOException {
            final Header[] headers = response.getHeaders(HttpHeaders.CONTENT_ENCODING);
            if (headers.length == 1) {
                this.contentEncodingHeader = headers[0];
            } else if (headers.length > 1) {
                throw new AssertionError("Expected none or one content encoding header but got " + headers.length + " headers.");
            }
        }

        public boolean hasContentEncodingHeader() {
            return contentEncodingHeader != null;
        }

        public Header getContentEncodingHeader() {
            return contentEncodingHeader;
        }
    }


}
