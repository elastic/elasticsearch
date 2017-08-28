/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;

/**
 * Integration test for the rest sql action. That one that speaks json directly to a
 * user rather than to the JDBC driver or CLI.
 */
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
        expected.put("columns", singletonList(columnInfo("test", "text")));
        expected.put("rows", Arrays.asList(singletonList("test"), singletonList("test")));
        expected.put("size", 2);
        assertResponse(expected, runSql("SELECT * FROM test"));
    }

    public void testNextPage() throws IOException {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            // NOCOMMIT we need number2 because we can't process the same column twice in two ways
            bulk.append("{\"index\":{\"_id\":\"" + i + "\"}}\n");
            bulk.append("{\"text\":\"text" + i + "\", \"number\":" + i + ", \"number2\": " + i + "}\n");
        }
        client().performRequest("POST", "/test/test/_bulk", singletonMap("refresh", "true"),
                new StringEntity(bulk.toString(), ContentType.APPLICATION_JSON));

        // NOCOMMIT we need tests for inner hits extractor and const extractor
        String request = "{\"query\":\"SELECT text, number, SIN(number2) FROM test ORDER BY number\", \"fetch_size\":2}";

        String cursor = null;
        for (int i = 0; i < 20; i += 2) {
            Map<String, Object> response;
            if (i == 0) {
                response = runSql(new StringEntity(request, ContentType.APPLICATION_JSON));
            } else {
                response = runSql(new StringEntity("{\"cursor\":\"" + cursor + "\"}", ContentType.APPLICATION_JSON));
            }

            Map<String, Object> expected = new HashMap<>();
            if (i == 0) {
                expected.put("columns", Arrays.asList(
                        columnInfo("text", "text"),
                        columnInfo("number", "long"),
                        columnInfo("SIN(number2)", "double")));
            }
            expected.put("rows", Arrays.asList(
                    Arrays.asList("text" + i, i, Math.sin(i)),
                    Arrays.asList("text" + (i + 1), i + 1, Math.sin(i + 1))));
            expected.put("size", 2);
            cursor = (String) response.remove("cursor");
            assertResponse(expected, response);
            assertNotNull(cursor);
        }
        Map<String, Object> expected = new HashMap<>();
        expected.put("size", 0);
        expected.put("rows", emptyList());
        assertResponse(expected, runSql(new StringEntity("{\"cursor\":\"" + cursor + "\"}", ContentType.APPLICATION_JSON)));
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
                new StringEntity("{\"query\":\"SELECT DAY_OF_YEAR(test), COUNT(*) FROM test\"}", ContentType.APPLICATION_JSON)));
    }

    public void testMissingIndex() throws IOException {
        expectBadRequest(() -> runSql("SELECT foo FROM missing"), containsString("1:17: Cannot resolve index [missing]"));
    }

    public void testMissingField() throws IOException {
        StringBuilder bulk = new StringBuilder();
        bulk.append("{\"index\":{\"_id\":\"1\"}}\n");
        bulk.append("{\"test\":\"test\"}\n");
        client().performRequest("POST", "/test/test/_bulk", singletonMap("refresh", "true"),
                new StringEntity(bulk.toString(), ContentType.APPLICATION_JSON));

        // NOCOMMIT "unresolved" should probably be changed to something users will understand like "missing"
        expectBadRequest(() -> runSql("SELECT foo FROM test"), containsString("1:8: Unresolved item 'foo'"));
        // NOCOMMIT the ones below one should include (foo) but it looks like the function is missing
        expectBadRequest(() -> runSql("SELECT DAY_OF_YEAR(foo) FROM test"), containsString("1:20: Unresolved item 'DAY_OF_YEAR'"));
        expectBadRequest(() -> runSql("SELECT foo, * FROM test GROUP BY DAY_OF_YEAR(foo)"),
                both(containsString("1:8: Unresolved item 'foo'"))
                    .and(containsString("1:46: Unresolved item 'DAY_OF_YEAR'")));
        // NOCOMMIT broken because we bail on the resolution phase if we can't resolve something in a previous phase
//        expectBadRequest(() -> runSql("SELECT * FROM test WHERE foo = 1"), containsString("500"));
//        expectBadRequest(() -> runSql("SELECT * FROM test WHERE DAY_OF_YEAR(foo) = 1"), containsString("500"));
        // NOCOMMIT this should point to the column, no the (incorrectly capitalized) start or ORDER BY
        expectBadRequest(() -> runSql("SELECT * FROM test ORDER BY foo"), containsString("line 1:29: Unresolved item 'Order'"));
        expectBadRequest(() -> runSql("SELECT * FROM test ORDER BY DAY_OF_YEAR(foo)"),
                containsString("line 1:41: Unresolved item 'Order'"));
    }

    private void expectBadRequest(ThrowingRunnable code, Matcher<String> errorMessageMatcher) {
        ResponseException e = expectThrows(ResponseException.class, code);
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
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

    private Map<String, Object> columnInfo(String name, String type) {
        Map<String, Object> column = new HashMap<>();
        column.put("name", name);
        column.put("type", type);
        return unmodifiableMap(column);
    }
}
