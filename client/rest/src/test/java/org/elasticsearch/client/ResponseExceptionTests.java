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

package org.elasticsearch.client;

import org.elasticsearch.client.http.HttpEntity;
import org.elasticsearch.client.http.HttpHost;
import org.elasticsearch.client.http.HttpResponse;
import org.elasticsearch.client.http.ProtocolVersion;
import org.elasticsearch.client.http.RequestLine;
import org.elasticsearch.client.http.StatusLine;
import org.elasticsearch.client.http.entity.ContentType;
import org.elasticsearch.client.http.entity.InputStreamEntity;
import org.elasticsearch.client.http.entity.StringEntity;
import org.elasticsearch.client.http.message.BasicHttpResponse;
import org.elasticsearch.client.http.message.BasicRequestLine;
import org.elasticsearch.client.http.message.BasicStatusLine;
import org.elasticsearch.client.http.util.EntityUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class ResponseExceptionTests extends RestClientTestCase {

    public void testResponseException() throws IOException {
        ProtocolVersion protocolVersion = new ProtocolVersion("http", 1, 1);
        StatusLine statusLine = new BasicStatusLine(protocolVersion, 500, "Internal Server Error");
        HttpResponse httpResponse = new BasicHttpResponse(statusLine);

        String responseBody = "{\"error\":{\"root_cause\": {}}}";
        boolean hasBody = getRandom().nextBoolean();
        if (hasBody) {
            HttpEntity entity;
            if (getRandom().nextBoolean()) {
                entity = new StringEntity(responseBody, ContentType.APPLICATION_JSON);
            } else {
                //test a non repeatable entity
                entity = new InputStreamEntity(new ByteArrayInputStream(responseBody.getBytes(StandardCharsets.UTF_8)),
                    ContentType.APPLICATION_JSON);
            }
            httpResponse.setEntity(entity);
        }

        RequestLine requestLine = new BasicRequestLine("GET", "/", protocolVersion);
        HttpHost httpHost = new HttpHost("localhost", 9200);
        Response response = new Response(requestLine, httpHost, httpResponse);
        ResponseException responseException = new ResponseException(response);

        assertSame(response, responseException.getResponse());
        if (hasBody) {
            assertEquals(responseBody, EntityUtils.toString(responseException.getResponse().getEntity()));
        } else {
            assertNull(responseException.getResponse().getEntity());
        }

        String message = response.getRequestLine().getMethod() + " " + response.getHost() + response.getRequestLine().getUri()
                + ": " + response.getStatusLine().toString();
        if (hasBody) {
            message += "\n" + responseBody;
        }
        assertEquals(message, responseException.getMessage());
    }
}
