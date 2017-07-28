/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.server;

import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.http.HttpEntity;
import org.elasticsearch.client.http.entity.ContentType;
import org.elasticsearch.client.http.entity.StringEntity;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.NotEqualMessageBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;

public class RestSqlIT extends ESRestTestCase {
    public void testBasicQuery() throws IOException {
        StringBuilder bulk = new StringBuilder();
        bulk.append("{\"index\":{\"_id\":\"1\"}}\n");
        bulk.append("{\"test\":\"test\"}\n");
        bulk.append("{\"index\":{\"_id\":\"2\"}}\n");
        bulk.append("{\"test\":\"test\"}\n");
        client().performRequest("POST", "/test/test/_bulk", singletonMap("refresh", "true"),
                new StringEntity(bulk.toString(), ContentType.APPLICATION_JSON));

        Map<String, Object> expected = new HashMap<>();
        expected.put("columns", singletonMap("test", singletonMap("type", "text")));
        expected.put("rows", Arrays.asList(singletonMap("test", "test"), singletonMap("test", "test")));
        expected.put("size", 2);
        assertResponse(expected, runSql("SELECT * FROM test.test"));
    }

    @AwaitsFix(bugUrl="https://github.com/elastic/x-pack-elasticsearch/issues/2074")
    public void testTimeZone() throws IOException {
        StringBuilder bulk = new StringBuilder();
        bulk.append("{\"index\":{\"_id\":\"1\"}}\n");
        bulk.append("{\"test\":\"2017-07-27 00:00:00\"}\n");
        bulk.append("{\"index\":{\"_id\":\"2\"}}\n");
        bulk.append("{\"test\":\"2017-07-27 01:00:00\"}\n");
        client().performRequest("POST", "/test/test/_bulk", singletonMap("refresh", "true"),
                new StringEntity(bulk.toString(), ContentType.APPLICATION_JSON));

        Map<String, Object> expected = new HashMap<>();
        expected.put("columns", singletonMap("test", singletonMap("type", "text")));
        expected.put("rows", Arrays.asList(singletonMap("test", "test"), singletonMap("test", "test")));
        expected.put("size", 2);

        // Default TimeZone is UTC
        assertResponse(expected, runSql(
                new StringEntity("{\"query\":\"SELECT DAY_OF_YEAR(test), COUNT(*) FROM test.test\"}", ContentType.APPLICATION_JSON)));
    }

    public void testMissingField() throws IOException {
        StringBuilder bulk = new StringBuilder();
        bulk.append("{\"index\":{\"_id\":\"1\"}}\n");
        bulk.append("{\"test\":\"test\"}\n");
        client().performRequest("POST", "/test/test/_bulk", singletonMap("refresh", "true"),
                new StringEntity(bulk.toString(), ContentType.APPLICATION_JSON));

        // NOCOMMIT test the error messages
        expectSqlThrows(() -> runSql("SELECT foo FROM test.test"), containsString("500"));
        expectSqlThrows(() -> runSql("SELECT DAY_OF_YEAR(foo) FROM test.test"), containsString("500"));
        expectSqlThrows(() -> runSql("SELECT foo, * FROM test.test GROUP BY DAY_OF_YEAR(foo)"), containsString("500"));
        expectSqlThrows(() -> runSql("SELECT * FROM test.test WHERE foo = 1"), containsString("500"));
        expectSqlThrows(() -> runSql("SELECT * FROM test.test WHERE DAY_OF_YEAR(foo) = 1"), containsString("500"));
        expectSqlThrows(() -> runSql("SELECT * FROM test.test ORDER BY foo"), containsString("500"));
        expectSqlThrows(() -> runSql("SELECT * FROM test.test ORDER BY DAY_OF_YEAR(foo)"), containsString("500"));
    }

    private void expectSqlThrows(ThrowingRunnable code, Matcher<String> errorMessageMatcher) {
        ResponseException e = expectThrows(ResponseException.class, code);
        assertThat(e.getMessage(), containsString("500"));
        // NOCOMMIT This should return a 400 or 422
        assertThat(e.getMessage(), errorMessageMatcher);
    }

    private Map<String, Object> runSql(String sql) throws IOException {
        return runSql(new StringEntity("{\"query\":\"" + sql + "\"}", ContentType.APPLICATION_JSON));
    }

    private Map<String, Object> runSql(HttpEntity sql) throws IOException {
        Response response = client().performRequest("POST", "/_sql", emptyMap(), sql);
        try (InputStream content = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }
    }

    private void assertResponse(Map<String, Object> expected, Map<String, Object> actual) {
        if (false == expected.equals(actual)) {
            NotEqualMessageBuilder message = new NotEqualMessageBuilder();
            message.compareMaps(actual, expected);
            fail("Response does not match:\n" + message.toString());
        }
    }
}
