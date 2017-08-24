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
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Collections;

public class HttpCompressionIT extends ESIntegTestCase {
    private static final String GZIP_ENCODING = "gzip";

    private static final StringEntity SAMPLE_DOCUMENT = new StringEntity("{\n" +
        "   \"name\": {\n" +
        "      \"first name\": \"Steve\",\n" +
        "      \"last name\": \"Jobs\"\n" +
        "   }\n" +
        "}", ContentType.APPLICATION_JSON);

    @Override
    protected boolean ignoreExternalCluster() {
        return false;
    }

    public void testCompressesResponseIfRequested() throws IOException {
        ensureGreen();
        try (RestClient client = getRestClient()) {
            Response response = client.performRequest("GET", "/", new BasicHeader(HttpHeaders.ACCEPT_ENCODING, GZIP_ENCODING));
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals(GZIP_ENCODING, response.getHeader(HttpHeaders.CONTENT_ENCODING));
        }
    }

    public void testUncompressedResponseByDefault() throws IOException {
        ensureGreen();
        try (RestClient client = getRestClient()) {
            Response response = client.performRequest("GET", "/");
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertNull(response.getHeader(HttpHeaders.CONTENT_ENCODING));

            response = client.performRequest("POST", "/company/employees/1", Collections.emptyMap(), SAMPLE_DOCUMENT);
            assertEquals(201, response.getStatusLine().getStatusCode());
            assertNull(response.getHeader(HttpHeaders.CONTENT_ENCODING));
        }
    }

}
