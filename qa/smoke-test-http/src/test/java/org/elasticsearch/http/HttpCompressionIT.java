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
package org.elasticsearch.http;

import org.apache.http.HttpHeaders;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;

public class HttpCompressionIT extends ESRestTestCase {
    private static final String GZIP_ENCODING = "gzip";

    private static final String SAMPLE_DOCUMENT = "{\n" +
        "   \"name\": {\n" +
        "      \"first name\": \"Steve\",\n" +
        "      \"last name\": \"Jobs\"\n" +
        "   }\n" +
        "}";


    public void testCompressesResponseIfRequested() throws IOException {
        Request request = new Request("GET", "/");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader(HttpHeaders.ACCEPT_ENCODING, GZIP_ENCODING);
        request.setOptions(options);
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals(GZIP_ENCODING, response.getHeader(HttpHeaders.CONTENT_ENCODING));
    }

    public void testUncompressedResponseByDefault() throws IOException {
        Response response = client().performRequest(new Request("GET", "/"));
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertNull(response.getHeader(HttpHeaders.CONTENT_ENCODING));

        Request request = new Request("POST", "/company/_doc/1");
        request.setJsonEntity(SAMPLE_DOCUMENT);
        response = client().performRequest(request);
        assertEquals(201, response.getStatusLine().getStatusCode());
        assertNull(response.getHeader(HttpHeaders.CONTENT_ENCODING));
    }

}
