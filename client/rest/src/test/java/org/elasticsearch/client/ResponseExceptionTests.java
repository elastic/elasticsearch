/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.util.EntityUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

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

        String message = String.format(Locale.ROOT,
            "method [%s], host [%s], URI [%s], status line [%s]",
            response.getRequestLine().getMethod(),
            response.getHost(),
            response.getRequestLine().getUri(),
            response.getStatusLine().toString()
        );

        if (hasBody) {
            message += "\n" + responseBody;
        }
        assertEquals(message, responseException.getMessage());
    }
}
