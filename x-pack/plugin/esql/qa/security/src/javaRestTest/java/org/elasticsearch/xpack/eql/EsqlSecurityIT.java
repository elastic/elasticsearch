/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class EsqlSecurityIT extends ESRestTestCase {

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test-admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private void indexDocument(String index, int id, double value, String org) throws IOException {
        Request indexDoc = new Request("PUT", index + "/_doc/" + id);
        indexDoc.setJsonEntity("{\"value\":" + value + ",\"org\":\"" + org + "\"}");
        client().performRequest(indexDoc);
    }

    @Before
    public void indexDocuments() throws IOException {
        String mapping = """
            "properties":{"value": {"type": "double"}, "org": {"type": "keyword"}}
            """;
        createIndex("index", Settings.EMPTY, mapping);
        indexDocument("index", 1, 10.0, "sales");
        indexDocument("index", 2, 20.0, "engineering");
        refresh("index");

        createIndex("index-user1", Settings.EMPTY, mapping);
        indexDocument("index-user1", 1, 12.0, "engineering");
        indexDocument("index-user1", 2, 31.0, "sales");
        refresh("index-user1");

        createIndex("index-user2", Settings.EMPTY, mapping);
        indexDocument("index-user2", 1, 32.0, "marketing");
        indexDocument("index-user2", 2, 40.0, "sales");
        refresh("index-user2");
    }

    public void testAllowedIndices() throws Exception {
        for (String user : List.of("test-admin", "user1", "user2")) {
            Response resp = runESQLCommand(user, "from index | stats sum=sum(value)");
            assertOK(resp);
            Map<String, Object> respMap = entityAsMap(resp);
            assertThat(respMap.get("columns"), equalTo(List.of(Map.of("name", "sum", "type", "double"))));
            assertThat(respMap.get("values"), equalTo(List.of(List.of(30.0))));
        }

        for (String user : List.of("test-admin", "user1")) {
            Response resp = runESQLCommand(user, "from index-user1 | stats sum=sum(value)");
            assertOK(resp);
            Map<String, Object> respMap = entityAsMap(resp);
            assertThat(respMap.get("columns"), equalTo(List.of(Map.of("name", "sum", "type", "double"))));
            assertThat(respMap.get("values"), equalTo(List.of(List.of(43.0))));
        }

        for (String user : List.of("test-admin", "user2")) {
            Response resp = runESQLCommand(user, "from index-user2 | stats sum=sum(value)");
            assertOK(resp);
            Map<String, Object> respMap = entityAsMap(resp);
            assertThat(respMap.get("columns"), equalTo(List.of(Map.of("name", "sum", "type", "double"))));
            assertThat(respMap.get("values"), equalTo(List.of(List.of(72.0))));
        }
    }

    public void testUnauthorizedIndices() {
        ResponseException error;
        error = expectThrows(ResponseException.class, () -> runESQLCommand("user1", "from index-user2 | stats sum(value)"));
        assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(400));

        error = expectThrows(ResponseException.class, () -> runESQLCommand("user2", "from index-user1 | stats sum(value)"));
        assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testDLS() throws Exception {
        Response resp = runESQLCommand("user3", "from index | stats sum=sum(value)");
        assertOK(resp);
        Map<String, Object> respMap = entityAsMap(resp);
        assertThat(respMap.get("columns"), equalTo(List.of(Map.of("name", "sum", "type", "double"))));
        assertThat(respMap.get("values"), equalTo(List.of(List.of(10.0))));
    }

    public void testRowCommand() throws Exception {
        String user = randomFrom("test-admin", "user1", "user2");
        Response resp = runESQLCommand(user, "row a = 5, b = 2 | stats count=sum(b) by a");
        assertOK(resp);
        Map<String, Object> respMap = entityAsMap(resp);
        assertThat(respMap.get("columns"), equalTo(List.of(Map.of("name", "count", "type", "long"), Map.of("name", "a", "type", "integer"))));
        assertThat(respMap.get("values"), equalTo(List.of(List.of(2, 5))));
    }

    private Response runESQLCommand(String user, String command) throws IOException {
        Request request = new Request("POST", "_esql");
        request.setJsonEntity("{\"query\":\"" + command + "\"}");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("es-security-runas-user", user));
        return client().performRequest(request);
    }

}
