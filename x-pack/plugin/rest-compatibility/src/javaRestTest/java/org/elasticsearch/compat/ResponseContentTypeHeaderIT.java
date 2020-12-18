/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.compat;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;

import static org.hamcrest.core.IsEqual.equalTo;

public class ResponseContentTypeHeaderIT extends ESRestTestCase {

    public void testResponseContentType() throws IOException {
        Request request = new Request("GET", "/");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Accept", "application/vnd.elasticsearch+json;compatible-with=" + Version.CURRENT.major);
        request.setOptions(options);
        Response response = client().performRequest(request);

        assertThat(
            response.getHeader("Content-Type"),
            equalTo("application/vnd.elasticsearch+json;compatible-with=" + Version.CURRENT.major)
        );
    }

    public void testRequestContentType() throws IOException {
        Request request = new Request("PUT", "/sample_index_name");
        Settings settings = Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0).build();
        String entity = "{\"settings\": " + Strings.toString(settings) + "}";
        StringEntity stringEntity = new StringEntity(
            entity,
            ContentType.parse("application/vnd.elasticsearch+json;compatible-with=" + Version.CURRENT.major)
        );
        request.setEntity(stringEntity);

        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Accept", "application/vnd.elasticsearch+json;compatible-with=" + Version.CURRENT.major);
        request.setOptions(options);

        Response response = client().performRequest(request);

        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    public void testInvalidRequestContentType() throws IOException {
        Request request = new Request("PUT", "/sample_index_name");
        Settings settings = Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0).build();
        String entity = "{\"settings\": " + Strings.toString(settings) + "}";
        StringEntity stringEntity = new StringEntity(
            entity,
            ContentType.parse("application/vnd.elasticsearch+xxx;compatible-with=" + Version.CURRENT.major)
        );
        request.setEntity(stringEntity);

        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Accept", "application/vnd.elasticsearch+json;compatible-with=" + Version.CURRENT.major);
        request.setOptions(options);

        ResponseException exc = expectThrows(ResponseException.class, () -> client().performRequest(request));

        assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.NOT_ACCEPTABLE.getStatus()));
        assertTrue(
            exc.getMessage().contains("Content-Type header [application/vnd.elasticsearch+xxx; compatible-with=8] is not supported")
        );
    }
}
