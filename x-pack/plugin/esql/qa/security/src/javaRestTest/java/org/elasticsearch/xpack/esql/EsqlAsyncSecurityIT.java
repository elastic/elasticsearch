/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.core.TimeValue.timeValueNanos;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class EsqlAsyncSecurityIT extends EsqlSecurityIT {

    private static final Logger LOGGER = LogManager.getLogger(EsqlAsyncSecurityIT.class);

    @Override
    protected Response runESQLCommand(String user, String command) throws IOException {
        var response = runAsync(user, command);
        assertOK(response);
        var respMap = entityAsMap(response.getEntity());
        String id = (String) respMap.get("id");
        assertThat((boolean) respMap.get("is_running"), either(is(true)).or(is(false)));
        var getResponse = runAsyncGet(user, id);
        assertOK(getResponse);
        var deleteResponse = runAsyncDelete(user, id);
        assertOK(deleteResponse);
        return getResponse;
    }

    @Override
    public void testUnauthorizedIndices() throws IOException {
        super.testUnauthorizedIndices();
        {
            var response = runAsync("user1", "from index-user1 | stats sum(value)");
            assertOK(response);
            var respMap = entityAsMap(response.getEntity());
            String id = (String) respMap.get("id");
            assertThat((boolean) respMap.get("is_running"), either(is(true)).or(is(false)));

            var getResponse = runAsyncGet("user1", id); // sanity
            assertOK(getResponse);
            ResponseException error;
            error = expectThrows(ResponseException.class, () -> runAsyncGet("user2", id));
            // resource not found exception if the authenticated user is not the creator of the original task
            assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(404));

            error = expectThrows(ResponseException.class, () -> runAsyncDelete("user2", id));
            // resource not found exception if the authenticated user is not the creator of the original task
            assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        }
        {
            var response = runAsync("user2", "from index-user2 | stats sum(value)");
            assertOK(response);
            var respMap = entityAsMap(response.getEntity());
            String id = (String) respMap.get("id");
            assertThat((boolean) respMap.get("is_running"), either(is(true)).or(is(false)));

            var getResponse = runAsyncGet("user2", id); // sanity
            assertOK(getResponse);
            ResponseException error;
            error = expectThrows(ResponseException.class, () -> runAsyncGet("user1", id));
            assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(404));

            error = expectThrows(ResponseException.class, () -> runAsyncDelete("user1", id));
            assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        }
    }

    // Keep_on_complete is always true, so we will always get an id
    private Response runAsync(String user, String command) throws IOException {
        if (command.toLowerCase(Locale.ROOT).contains("limit") == false) {
            // add a (high) limit to avoid warnings on default limit
            command += " | limit 10000000";
        }
        XContentBuilder json = JsonXContent.contentBuilder();
        json.startObject();
        json.field("query", command);
        addRandomPragmas(json);
        json.field("wait_for_completion_timeout", timeValueNanos(randomIntBetween(1, 1000)));
        json.field("keep_on_completion", "true");
        json.endObject();
        Request request = new Request("POST", "_query/async");
        request.setJsonEntity(Strings.toString(json));
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("es-security-runas-user", user));
        logRequest(request);
        Response response = client().performRequest(request);
        logResponse(response);
        return response;
    }

    private Response runAsyncGet(String user, String id) throws IOException {
        Request getRequest = new Request("GET", "_query/async/" + id + "?wait_for_completion_timeout=60s");
        getRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("es-security-runas-user", user));
        logRequest(getRequest);
        var response = client().performRequest(getRequest);
        logResponse(response);
        return response;
    }

    private Response runAsyncDelete(String user, String id) throws IOException {
        Request getRequest = new Request("DELETE", "_query/async/" + id);
        getRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("es-security-runas-user", user));
        logRequest(getRequest);
        var response = client().performRequest(getRequest);
        logResponse(response);
        return response;
    }

    static void logRequest(Request request) throws IOException {
        LOGGER.info("REQUEST={}", request);
        var entity = request.getEntity();
        if (entity != null) LOGGER.info("REQUEST body={}", entityAsMap(entity));
    }

    static void logResponse(Response response) {
        LOGGER.info("RESPONSE={}", response);
    }
}
