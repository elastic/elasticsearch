/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql;

import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.http.Header;
import org.elasticsearch.client.http.entity.ContentType;
import org.elasticsearch.client.http.entity.StringEntity;
import org.elasticsearch.client.http.message.BasicHeader;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.NotEqualMessageBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;

public class SqlSecurityIT extends ESRestTestCase {
    /**
     * All tests run as a an administrative user but use
     * <code>es-security-runas-user</code> to become a less privileged user when needed.
     */
    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", token)
                .build();
    }

    @Before
    public void createTestData() throws IOException {
        StringBuilder bulk = new StringBuilder();
        bulk.append("{\"index\":{\"_id\":\"1\"}\n");
        bulk.append("{\"a\": 1, \"b\": 2, \"c\": 3}\n");
        bulk.append("{\"index\":{\"_id\":\"2\"}\n");
        bulk.append("{\"a\": 4, \"b\": 5, \"c\": 6}\n");
        client().performRequest("PUT", "/test/test/_bulk", singletonMap("refresh", "true"),
                new StringEntity(bulk.toString(), ContentType.APPLICATION_JSON));
    }

    // NOCOMMIT we're going to need to test jdbc and cli with these too!
    // NOCOMMIT we'll have to test scrolling as well
    // NOCOMMIT tests for describing a table and showing tables
    // NOCOMMIT tests with audit trail

    public void testSqlWorksAsAdmin() throws IOException {
        Map<String, Object> expected = new HashMap<>();
        Map<String, Object> columns = new HashMap<>();
        columns.put("a", singletonMap("type", "long"));
        columns.put("b", singletonMap("type", "long"));
        columns.put("c", singletonMap("type", "long"));
        expected.put("columns", columns);
        Map<String, Object> row1 = new HashMap<>();
        row1.put("a", 1);
        row1.put("b", 2);
        row1.put("c", 3);
        Map<String, Object> row2 = new HashMap<>();
        row2.put("a", 4);
        row2.put("b", 5);
        row2.put("c", 6);
        expected.put("rows", Arrays.asList(row1, row2));
        expected.put("size", 2);
        assertResponse(expected, runSql("SELECT * FROM test ORDER BY a", null));
    }

    public void testSqlWithFullAccess() throws IOException {
        createUser("full_access", "read_test");

        assertResponse(runSql("SELECT * FROM test ORDER BY a", null), runSql("SELECT * FROM test ORDER BY a", "full_access"));
    }

    public void testSqlNoAccess() throws IOException {
        createUser("no_access", "read_nothing");

        ResponseException e = expectThrows(ResponseException.class, () -> runSql("SELECT * FROM test", "no_access"));
        assertThat(e.getMessage(), containsString("403 Forbidden"));
    }

    public void testSqlWrongAccess() throws IOException {
        createUser("wrong_access", "read_something_else");

        ResponseException e = expectThrows(ResponseException.class, () -> runSql("SELECT * FROM test", "no_access"));
        assertThat(e.getMessage(), containsString("403 Forbidden"));
    }

    public void testSqlSingleFieldGranted() throws IOException {
        createUser("only_a", "read_test_a");

        assertResponse(runSql("SELECT a FROM test", null), runSql("SELECT * FROM test", "only_a"));
        expectBadRequest(() -> runSql("SELECT c FROM test", "only_a"), containsString("line 1:8: Unresolved item 'c'"));
    }

    public void testSqlSingleFieldExcepted() throws IOException {
        createUser("not_c", "read_test_a_and_b");

        assertResponse(runSql("SELECT a, b FROM test", null), runSql("SELECT * FROM test", "not_c"));
        expectBadRequest(() -> runSql("SELECT c FROM test", "not_c"), containsString("line 1:8: Unresolved item 'c'"));
    }

    public void testSqlDocumentExclued() throws IOException {
        createUser("no_3s", "read_test_without_c_3");

        assertResponse(runSql("SELECT * FROM test WHERE c != 3", null), runSql("SELECT * FROM test", "no_3s"));
    }

    private void expectBadRequest(ThrowingRunnable code, Matcher<String> errorMessageMatcher) {
        ResponseException e = expectThrows(ResponseException.class, code);
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), errorMessageMatcher);
    }

    private void assertResponse(Map<String, Object> expected, Map<String, Object> actual) {
        if (false == expected.equals(actual)) {
            NotEqualMessageBuilder message = new NotEqualMessageBuilder();
            message.compareMaps(actual, expected);
            fail("Response does not match:\n" + message.toString());
        }
    }

    private Map<String, Object> runSql(String sql, @Nullable String asUser) throws IOException {
        Header[] headers = asUser == null ? new Header[0] : new Header[] {new BasicHeader("es-security-runas-user", asUser)};
        Response response = client().performRequest("POST", "/_sql", emptyMap(),
                new StringEntity("{\"query\": \"" + sql + "\"}", ContentType.APPLICATION_JSON),
                headers);
        try (InputStream content = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }
    }

    private void createUser(String name, String role) throws IOException {
        XContentBuilder user = JsonXContent.contentBuilder().prettyPrint().startObject(); {
            user.field("password", "not_used");
            user.field("roles", role);
        }
        user.endObject();
        client().performRequest("PUT", "/_xpack/security/user/" + name, emptyMap(),
                new StringEntity(user.string(), ContentType.APPLICATION_JSON));
    }
}
