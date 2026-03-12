/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.http;

import org.apache.http.HttpHeaders;
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;

public class HttpCompressionIT extends AbstractHttpSmokeTestIT {

    private static final String GZIP_ENCODING = "gzip";
    private static final String SAMPLE_DOCUMENT = """
        {
           "name": {
              "first name": "Steve",
              "last name": "Jobs"
           }
        }""";

    public void testCompressesResponseIfRequested() throws IOException {
        Request request = new Request("POST", "/company/_doc/2");
        request.setJsonEntity(SAMPLE_DOCUMENT);
        Response response = client().performRequest(request);
        assertEquals(201, response.getStatusLine().getStatusCode());
        assertNull(response.getHeader(HttpHeaders.CONTENT_ENCODING));
        assertThat(response.getEntity(), is(not(instanceOf(GzipDecompressingEntity.class))));

        request = new Request("GET", "/company/_doc/2");
        RequestOptions requestOptions = RequestOptions.DEFAULT.toBuilder().addHeader(HttpHeaders.ACCEPT_ENCODING, GZIP_ENCODING).build();

        request.setOptions(requestOptions);
        response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertNull(response.getHeader(HttpHeaders.CONTENT_ENCODING));
        assertThat(response.getEntity(), instanceOf(GzipDecompressingEntity.class));

        String body = EntityUtils.toString(response.getEntity());
        assertThat(body, containsString(SAMPLE_DOCUMENT));
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
        assertThat(response.getEntity(), is(not(instanceOf(GzipDecompressingEntity.class))));
    }

}
