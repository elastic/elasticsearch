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
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.NotEqualMessageBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlAction;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;

public class SqlSecurityIT extends ESRestTestCase {
    private static boolean createdTestData = false;
    private static boolean auditFailure = false;

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

    @Override
    protected boolean preserveIndicesUponCompletion() {
        /* We can't wipe the cluster between tests because that nukes the audit
         * trail index which makes the auditing flaky. Instead we wipe all
         * indices after the entire class is finished. */
        return true;
    }

    @Before
    public void createTestData() throws IOException {
        if (createdTestData) {
            /* Since we don't wipe the cluster between tests we only need to
             * write the test data once. */
            return;
        }
        StringBuilder bulk = new StringBuilder();
        bulk.append("{\"index\":{\"_id\":\"1\"}\n");
        bulk.append("{\"a\": 1, \"b\": 2, \"c\": 3}\n");
        bulk.append("{\"index\":{\"_id\":\"2\"}\n");
        bulk.append("{\"a\": 4, \"b\": 5, \"c\": 6}\n");
        client().performRequest("PUT", "/test/test/_bulk", singletonMap("refresh", "true"),
                new StringEntity(bulk.toString(), ContentType.APPLICATION_JSON));
    }

    @After
    public void clearAuditLog() throws Exception {
        try {
            clearAuditEvents();
        } catch (ResponseException e) {
            // 404 here just means we had no indexes
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    @AfterClass
    public static void wipeIndicesAfterTests() throws IOException {
        try {
            adminClient().performRequest("DELETE", "*");
        } catch (ResponseException e) {
            // 404 here just means we had no indexes
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    // NOCOMMIT we're going to need to test jdbc and cli with these too!
    // NOCOMMIT we'll have to test scrolling as well
    // NOCOMMIT tests for describing a table and showing tables

    public void testSqlWorksAsAdmin() throws Exception {
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
        assertAuditForSqlGranted("test_admin", "test");
    }

    public void testSqlWithFullAccess() throws Exception {
        createUser("full_access", "read_test");

        assertResponse(runSql("SELECT * FROM test ORDER BY a", null), runSql("SELECT * FROM test ORDER BY a", "full_access"));
        assertAuditForSqlGranted("test_admin", "test");
        assertAuditForSqlGranted("full_access", "test");
    }

    public void testSqlNoAccess() throws Exception {
        createUser("no_access", "read_nothing");

        ResponseException e = expectThrows(ResponseException.class, () -> runSql("SELECT * FROM test", "no_access"));
        assertThat(e.getMessage(), containsString("403 Forbidden"));
        assertAuditEvents(m -> "access_denied".equals(m.get("event_type"))
                && m.get("indices") == null
                && "no_access".equals(m.get("principal")));
    }

    public void testSqlWrongAccess() throws Exception {
        createUser("wrong_access", "read_something_else");

        ResponseException e = expectThrows(ResponseException.class, () -> runSql("SELECT * FROM test", "wrong_access"));
        assertThat(e.getMessage(), containsString("403 Forbidden"));
        assertAuditEvents(
                /* This user has permission to run sql queries so they are
                 * given preliminary authorization. */
                m -> "access_granted".equals(m.get("event_type"))
                && null == m.get("indices")
                && "wrong_access".equals(m.get("principal")),
                /* But as soon as they attempt to resolve an index that
                 * they don't have access to they get denied. */
                m -> "access_denied".equals(m.get("event_type"))
                && singletonList("test").equals(m.get("indices"))
                && "wrong_access".equals(m.get("principal")));
    }

    public void testSqlSingleFieldGranted() throws Exception {
        createUser("only_a", "read_test_a");

        assertResponse(runSql("SELECT a FROM test", null), runSql("SELECT * FROM test", "only_a"));
        assertAuditForSqlGranted("test_admin", "test");
        assertAuditForSqlGranted("only_a", "test");
        clearAuditEvents();
        expectBadRequest(() -> runSql("SELECT c FROM test", "only_a"), containsString("line 1:8: Unresolved item 'c'"));
        /* The user has permission to query the index but one of the
         * columns that they explicitly mention is hidden from them
         * by field level access control. This *looks* like a successful
         * query from the audit side because all the permissions checked
         * out but it failed in SQL because it couldn't compile the
         * query without the metadata for the missing field. */
        assertAuditForSqlGranted("only_a", "test");
    }

    public void testSqlSingleFieldExcepted() throws Exception {
        createUser("not_c", "read_test_a_and_b");

        assertResponse(runSql("SELECT a, b FROM test", null), runSql("SELECT * FROM test", "not_c"));
        assertAuditForSqlGranted("test_admin", "test");
        assertAuditForSqlGranted("not_c", "test");
        clearAuditEvents();
        expectBadRequest(() -> runSql("SELECT c FROM test", "not_c"), containsString("line 1:8: Unresolved item 'c'"));
        /* The user has permission to query the index but one of the
         * columns that they explicitly mention is hidden from them
         * by field level access control. This *looks* like a successful
         * query from the audit side because all the permissions checked
         * out but it failed in SQL because it couldn't compile the
         * query without the metadata for the missing field. */
        assertAuditForSqlGranted("not_c", "test");
    }

    public void testSqlDocumentExclued() throws Exception {
        createUser("no_3s", "read_test_without_c_3");

        assertResponse(runSql("SELECT * FROM test WHERE c != 3", null), runSql("SELECT * FROM test", "no_3s"));
        assertAuditForSqlGranted("no_3s", "test");
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
        return toMap(response);
    }

    private Map<String, Object> toMap(Response response) throws IOException {
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

    private void assertAuditForSqlGranted(String user, String index) throws Exception {
        assertAuditEvents(
                m -> "access_granted".equals(m.get("event_type"))
                    && m.get("indices") == null
                    && user.equals(m.get("principal")),
                m -> "access_granted".equals(m.get("event_type"))
                    && singletonList(index).equals(m.get("indices"))
                    && user.equals(m.get("principal")));
    }

    /**
     * Asserts that audit events have been logged that match all the provided checkers.
     */
    @SafeVarargs
    private final void assertAuditEvents(CheckedFunction<Map<?, ?>, Boolean, Exception>... eventCheckers) throws Exception {
        assumeFalse("Previous test had an audit-related failure. All subsequent audit related assertions are bogus because we can't "
                + "guarantee that we fully cleaned up after the last test.", auditFailure);
        try {
            assertBusy(() -> {
                XContentBuilder search = JsonXContent.contentBuilder().prettyPrint();
                search.startObject(); {
                    search.array("_source", "@timestamp", "indices", "principal", "event_type");
                    search.startObject("query"); {
                        search.startObject("match").field("action", SqlAction.NAME).endObject();
                    }
                    search.endObject();
                }
                search.endObject();
                Map<String, Object> audit;
                try {
                    audit = toMap(client().performRequest("POST", "/.security_audit_log-*/_search",
                        singletonMap("filter_path", "hits.hits._source"), new StringEntity(search.string(), ContentType.APPLICATION_JSON)));
                } catch (ResponseException e) {
                    throw new AssertionError("ES failed to respond. Wrapping in assertion so we retry. Hopefully this is transient.", e);
                }
                Map<?, ?> hitsOuter = (Map<?, ?>) audit.get("hits");
                assertNotNull("expected some hits", hitsOuter);
                List<?> hits = (List<?>) hitsOuter.get("hits");
                verifier: for (CheckedFunction<Map<?, ?>, Boolean, Exception> eventChecker : eventCheckers) {
                    for (Object hit : hits) {
                        Map<?, ?> source = (Map<?, ?>)((Map<?, ?>) hit).get("_source");
                        if (eventChecker.apply(source)) {
                            continue verifier;
                        }
                    }
                    fail("didn't find audit event we were looking for. found [" + hits + "]");
                }
            });
        } catch (AssertionError e) {
            auditFailure = true;
            throw e;
        }
    }

    private void clearAuditEvents() throws Exception {
        try {
            assertBusy(() -> {
                try {
                    adminClient().performRequest("POST", "/.security_audit_log-*/_delete_by_query", emptyMap(),
                            new StringEntity("{\"query\":{\"match_all\":{}}}", ContentType.APPLICATION_JSON));
                } catch (ResponseException e) {
                    if (e.getResponse().getStatusLine().getStatusCode() == 409) {
                        throw new AssertionError("Conflict while clearing audit log. Wrapping in assertion so we retry.", e);
                    }
                    throw e;
                }
            });
        } catch (AssertionError e) {
            auditFailure = true;
            throw e;
        }
    }
}
