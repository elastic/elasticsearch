/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.security;

import org.apache.http.Header;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.NotEqualMessageBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matcher;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.qa.sql.rest.RestSqlTestCase.columnInfo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;

public class RestSqlSecurityIT extends ESRestTestCase {
    private static final String SQL_ACTION_NAME = "indices:data/read/sql";
    private static final String SQL_INDICES_ACTION_NAME = "indices:data/read/sql/tables";
    private static boolean oneTimeSetup = false;
    private static boolean auditFailure = false;

    /**
     * All tests run as a an administrative user but use
     * <code>es-security-runas-user</code> to become a less privileged user when needed.
     */
    @Override
    protected Settings restClientSettings() {
        return RestSqlIT.securitySettings();
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        /* We can't wipe the cluster between tests because that nukes the audit
         * trail index which makes the auditing flaky. Instead we wipe all
         * indices after the entire class is finished. */
        return true;
    }

    @Before
    public void oneTimeSetup() throws Exception {
        if (oneTimeSetup) {
            /* Since we don't wipe the cluster between tests we only need to
             * write the test data once. */
            return;
        }
        StringBuilder bulk = new StringBuilder();
        bulk.append("{\"index\":{\"_index\": \"test\", \"_type\": \"doc\", \"_id\":\"1\"}\n");
        bulk.append("{\"a\": 1, \"b\": 2, \"c\": 3}\n");
        bulk.append("{\"index\":{\"_index\": \"test\", \"_type\": \"doc\", \"_id\":\"2\"}\n");
        bulk.append("{\"a\": 4, \"b\": 5, \"c\": 6}\n");
        bulk.append("{\"index\":{\"_index\": \"bort\", \"_type\": \"doc\", \"_id\":\"1\"}\n");
        bulk.append("{\"a\": \"test\"}\n");
        client().performRequest("PUT", "/_bulk", singletonMap("refresh", "true"),
                new StringEntity(bulk.toString(), ContentType.APPLICATION_JSON));
        /* Wait for the audit log to go quiet and then clear it to protect
         * us from log events coming from other tests. */
        cleanAuditLog();
        oneTimeSetup = true;
    }

    /**
     * Wait for any running bulk tasks to complete because those
     * are likely audit log events and will cause the tests to
     * hang at best and at worst. Then remove all audit logs.
     */
    @Before
    public void cleanAuditLog() throws Exception {
        assertBusy(() -> {
            Set<String> bulks = new HashSet<>();
            Map<?, ?> nodes = (Map<?, ?>) entityAsMap(adminClient().performRequest("GET", "_tasks")).get("nodes");
            for (Map.Entry<?, ?> node : nodes.entrySet()) {
                Map<?, ?> nodeInfo = (Map<?, ?>) node.getValue();
                Map<?, ?> nodeTasks = (Map<?, ?>) nodeInfo.get("tasks");
                for (Map.Entry<?, ?> taskAndName : nodeTasks.entrySet()) {
                    Map<?, ?> task = (Map<?, ?>) taskAndName.getValue();
                    String action = task.get("action").toString();
                    if ("indices:data/write/bulk".equals(action) || "indices:data/write/bulk[s]".equals(action)) {
                        bulks.add(task.toString());
                    }
                }
            }
            if (false == bulks.isEmpty()) {
                String bulksString = Strings.join(bulks, '\n');
                logger.info("Waiting on bulk writes to finish:\n{}", bulksString);
                fail("Waiting on bulk writes to finish:\n" + bulksString);
            }
        }, 1, TimeUnit.MINUTES);
        try {
            clearAuditEvents();
        } catch (ResponseException e) {
            // 404 here just means we don't have any audit log index which shouldn't fail
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
    // NOCOMMIT assert that we don't have more audit logs then what we expect.

    public void testQueryWorksAsAdmin() throws Exception {
        Map<String, Object> expected = new HashMap<>();
        expected.put("columns", Arrays.asList(
                columnInfo("a", "long"),
                columnInfo("b", "long"),
                columnInfo("c", "long")));
        expected.put("rows", Arrays.asList(
                Arrays.asList(1, 2, 3),
                Arrays.asList(4, 5, 6)));
        expected.put("size", 2);
        assertResponse(expected, runSql("SELECT * FROM test ORDER BY a", null));
        assertAuditForSqlGetTableSyncGranted("test_admin", "test");
    }

    public void testQueryWithFullAccess() throws Exception {
        createUser("full_access", "read_all");

        assertResponse(runSql("SELECT * FROM test ORDER BY a", null), runSql("SELECT * FROM test ORDER BY a", "full_access"));
        assertAuditForSqlGetTableSyncGranted("test_admin", "test");
        assertAuditForSqlGetTableSyncGranted("full_access", "test");
    }

    public void testQueryNoAccess() throws Exception {
        createUser("no_access", "read_nothing");

        ResponseException e = expectThrows(ResponseException.class, () -> runSql("SELECT * FROM test", "no_access"));
        assertThat(e.getMessage(), containsString("403 Forbidden"));
        assertAuditEvents(m -> "access_denied".equals(m.get("event_type"))
                && m.get("indices") == null
                && "no_access".equals(m.get("principal")));
    }

    public void testQueryWrongAccess() throws Exception {
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

    public void testQuerySingleFieldGranted() throws Exception {
        createUser("only_a", "read_test_a");

        assertResponse(runSql("SELECT a FROM test", null), runSql("SELECT * FROM test", "only_a"));
        assertAuditForSqlGetTableSyncGranted("test_admin", "test");
        assertAuditForSqlGetTableSyncGranted("only_a", "test");
        clearAuditEvents();
        expectBadRequest(() -> runSql("SELECT c FROM test", "only_a"), containsString("line 1:8: Unknown column [c]"));
        /* The user has permission to query the index but one of the
         * columns that they explicitly mention is hidden from them
         * by field level access control. This *looks* like a successful
         * query from the audit side because all the permissions checked
         * out but it failed in SQL because it couldn't compile the
         * query without the metadata for the missing field. */
        assertAuditForSqlGetTableSyncGranted("only_a", "test");
    }

    public void testQuerySingleFieldExcepted() throws Exception {
        createUser("not_c", "read_test_a_and_b");

        assertResponse(runSql("SELECT a, b FROM test", null), runSql("SELECT * FROM test", "not_c"));
        assertAuditForSqlGetTableSyncGranted("test_admin", "test");
        assertAuditForSqlGetTableSyncGranted("not_c", "test");
        clearAuditEvents();
        expectBadRequest(() -> runSql("SELECT c FROM test", "not_c"), containsString("line 1:8: Unknown column [c]"));
        /* The user has permission to query the index but one of the
         * columns that they explicitly mention is hidden from them
         * by field level access control. This *looks* like a successful
         * query from the audit side because all the permissions checked
         * out but it failed in SQL because it couldn't compile the
         * query without the metadata for the missing field. */
        assertAuditForSqlGetTableSyncGranted("not_c", "test");
    }

    public void testQueryDocumentExclued() throws Exception {
        createUser("no_3s", "read_test_without_c_3");

        assertResponse(runSql("SELECT * FROM test WHERE c != 3", null), runSql("SELECT * FROM test", "no_3s"));
        assertAuditForSqlGetTableSyncGranted("test_admin", "test");
        assertAuditForSqlGetTableSyncGranted("no_3s", "test");
    }

    public void testShowTablesWorksAsAdmin() throws Exception {
        Map<String, Object> expected = new HashMap<>();
        expected.put("columns", singletonList(columnInfo("table", "keyword")));
        expected.put("rows", Arrays.asList(
                singletonList("bort"),
                singletonList("test")));
        expected.put("size", 2);
        assertResponse(expected, runSql("SHOW TABLES", null));
        assertAuditEvents(
                audit(true, SQL_ACTION_NAME, "test_admin", null),
                audit(true, SQL_INDICES_ACTION_NAME, "test_admin", hasItems("test", "bort")));
    }

    public void testShowTablesWorksAsFullAccess() throws Exception {
        createUser("full_access", "read_all");

        assertResponse(runSql("SHOW TABLES", null), runSql("SHOW TABLES", "full_access"));
        assertAuditEvents(
                audit(true, SQL_ACTION_NAME, "test_admin", null),
                audit(true, SQL_INDICES_ACTION_NAME, "test_admin", hasItems("test", "bort")),
                audit(true, SQL_ACTION_NAME, "full_access", null),
                audit(true, SQL_INDICES_ACTION_NAME, "full_access", hasItems("test", "bort")));
    }

    public void testShowTablesWithNoAccess() throws Exception {
        createUser("no_access", "read_nothing");

        ResponseException e = expectThrows(ResponseException.class, () -> runSql("SHOW TABLES", "no_access"));
        assertThat(e.getMessage(), containsString("403 Forbidden"));
        assertAuditEvents(audit(false, SQL_ACTION_NAME, "no_access", null));
    }

    public void testShowTablesWithLimitedAccess() throws Exception {
        createUser("read_bort", "read_bort");

        assertResponse(runSql("SHOW TABLES LIKE 'bort'", null), runSql("SHOW TABLES", "read_bort"));
        assertAuditForSqlGetTableSyncGranted("test_admin", "bort");
        assertAuditEvents(
                audit(true, SQL_ACTION_NAME, "test_admin", null),
                audit(true, SQL_INDICES_ACTION_NAME, "test_admin", contains("bort")),
                audit(true, SQL_ACTION_NAME, "read_bort", null),
                audit(true, SQL_INDICES_ACTION_NAME, "read_bort", contains("bort")));
    }

    public void testShowTablesWithLimitedAccessAndPattern() throws Exception {
        createUser("read_bort", "read_bort");

        Map<String, Object> expected = new HashMap<>();
        expected.put("columns", singletonList(columnInfo("table", "keyword")));
        expected.put("rows", emptyList());
        expected.put("size", 0);

        assertResponse(expected, runSql("SHOW TABLES LIKE 'test'", "read_bort"));
        assertAuditEvents(
                audit(true, SQL_ACTION_NAME, "read_bort", null),
                audit(true, SQL_INDICES_ACTION_NAME, "read_bort", contains("*", "-*")));
    }

    public void testDescribeWorksAsAdmin() throws Exception {
        Map<String, Object> expected = new HashMap<>();
        expected.put("columns", Arrays.asList(
                columnInfo("column", "keyword"),
                columnInfo("type", "keyword")));
        expected.put("rows", Arrays.asList(
                Arrays.asList("a", "BIGINT"),
                Arrays.asList("b", "BIGINT"),
                Arrays.asList("c", "BIGINT")));
        expected.put("size", 3);
        assertResponse(expected, runSql("DESCRIBE test", null));
        assertAuditForSqlGetTableSyncGranted("test_admin", "test");
    }

    public void testDescribeWorksAsFullAccess() throws Exception {
        createUser("full_access", "read_all");

        assertResponse(runSql("DESCRIBE test", null), runSql("DESCRIBE test", "full_access"));
        assertAuditForSqlGetTableSyncGranted("test_admin", "test");
        assertAuditForSqlGetTableSyncGranted("full_access", "test");
    }

    public void testDescribeWithNoAccess() throws Exception {
        createUser("no_access", "read_nothing");

        ResponseException e = expectThrows(ResponseException.class, () -> runSql("DESCRIBE test", "no_access"));
        assertThat(e.getMessage(), containsString("403 Forbidden"));
        assertAuditEvents(m -> "access_denied".equals(m.get("event_type"))
                && m.get("indices") == null
                && "no_access".equals(m.get("principal")));
    }

    public void testDescribeWithWrongAccess() throws Exception {
        createUser("wrong_access", "read_something_else");

        ResponseException e = expectThrows(ResponseException.class, () -> runSql("DESCRIBE test", "wrong_access"));
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
    
    public void testDescribeSingleFieldGranted() throws Exception {
        createUser("only_a", "read_test_a");

        Map<String, Object> expected = new HashMap<>();
        expected.put("columns", Arrays.asList(
                columnInfo("column", "keyword"),
                columnInfo("type", "keyword")));
        expected.put("rows", singletonList(Arrays.asList("a", "BIGINT")));
        expected.put("size", 1);

        assertResponse(expected, runSql("DESCRIBE test", "only_a"));
        assertAuditForSqlGetTableSyncGranted("only_a", "test");
        clearAuditEvents();
    }

    public void testDescribeSingleFieldExcepted() throws Exception {
        createUser("not_c", "read_test_a_and_b");

        Map<String, Object> expected = new HashMap<>();
        expected.put("columns", Arrays.asList(
                columnInfo("column", "keyword"),
                columnInfo("type", "keyword")));
        expected.put("rows", Arrays.asList(
                Arrays.asList("a", "BIGINT"),
                Arrays.asList("b", "BIGINT")));
        expected.put("size", 2);

        assertResponse(expected, runSql("DESCRIBE test", "not_c"));
        assertAuditForSqlGetTableSyncGranted("not_c", "test");
        clearAuditEvents();
    }

    public void testDescribeDocumentExclued() throws Exception {
        createUser("no_3s", "read_test_without_c_3");

        assertResponse(runSql("DESCRIBE test", null), runSql("DESCRIBE test", "no_3s"));
        assertAuditForSqlGetTableSyncGranted("test_admin", "test");
        assertAuditForSqlGetTableSyncGranted("no_3s", "test");
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

    private void assertAuditForSqlGetTableSyncGranted(String user, String index) throws Exception {
        assertAuditEvents(
                m -> "access_granted".equals(m.get("event_type"))
                    && SQL_ACTION_NAME.equals(m.get("action"))
                    && m.get("indices") == null
                    && user.equals(m.get("principal")),
                m -> "access_granted".equals(m.get("event_type"))
                    && SQL_ACTION_NAME.equals(m.get("action"))
                    && singletonList(index).equals(m.get("indices"))
                    && user.equals(m.get("principal")));
    }

    /**
     * Asserts that audit events have been logged that match all the provided checkers.
     */
    @SafeVarargs
    private final void assertAuditEvents(CheckedFunction<Map<?, ?>, Boolean, Exception>... eventCheckers) throws Exception {
        assertFalse("Previous test had an audit-related failure. All subsequent audit related assertions are bogus because we can't "
                + "guarantee that we fully cleaned up after the last test.", auditFailure);
        try {
            assertBusy(() -> {
                XContentBuilder search = JsonXContent.contentBuilder().prettyPrint();
                search.startObject(); {
                    search.startObject("query"); {
                        search.startObject("bool"); {
                            search.startArray("should"); {
                                search.startObject(); {
                                    search.startObject("match").field("action", SQL_ACTION_NAME).endObject();
                                }
                                search.endObject();
                                search.startObject(); {
                                    search.startObject("match").field("action", SQL_INDICES_ACTION_NAME).endObject();
                                }
                                search.endObject();
                            }
                            search.endArray();
                        }
                        search.endObject();
                    }
                    search.endObject();
                }
                search.endObject();
                Map<String, Object> audit;
                try {
                    audit = toMap(client().performRequest("POST", "/.security_audit_log-*/_search?size=1000",
                            emptyMap(), new StringEntity(search.string(), ContentType.APPLICATION_JSON)));
                } catch (ResponseException e) {
                    throw new AssertionError("ES failed to respond. Wrapping in assertion so we retry. Hopefully this is transient.", e);
                }
                Map<?, ?> hitsOuter = (Map<?, ?>) audit.get("hits");
                if (hitsOuter == null) {
                    fail("expected some hit but got:\n" + audit);
                }
                List<?> hits = (List<?>) hitsOuter.get("hits");
                verifier: for (CheckedFunction<Map<?, ?>, Boolean, Exception> eventChecker : eventCheckers) {
                    for (Object hit : hits) {
                        Map<?, ?> source = (Map<?, ?>)((Map<?, ?>) hit).get("_source");
                        if (eventChecker.apply(source)) {
                            continue verifier;
                        }
                    }
                    fail("didn't find audit event we were looking for. found " + hits);
                }
            }, 1, TimeUnit.MINUTES);
        } catch (AssertionError e) {
            auditFailure = true;
            logger.warn("Failed to find an audit log. Skipping remaining tests in this class after this the missing audit"
                    + "logs could turn up later.");
            Map<String, Object> audit = toMap(
                    client().performRequest("POST", "/.security_audit_log-*/_search?size=50&sort=@timestamp:desc"));
            Map<?, ?> hitsOuter = (Map<?, ?>) audit.get("hits");
            List<?> hits = hitsOuter == null ? null : (List<?>) hitsOuter.get("hits");
            if (hits == null || hits.isEmpty()) {
                logger.warn("Didn't find any audit logs. Here is the whole response:\n{}", audit);
            } else {
                logger.warn("Here are the last 500 indexed audit logs:");
                for (Object hit : hits) {
                    logger.warn(hit.toString());
                }
            }
            throw e;
        }
    }

    private CheckedFunction<Map<?, ?>, Boolean, Exception> audit(boolean granted, String action,
            String principal, Matcher<? extends Iterable<? extends String>> indicesMatcher) {
        String eventType = granted ? "access_granted" : "access_denied";
        return m -> eventType.equals(m.get("event_type"))
            && action.equals(m.get("action"))
            && principal.equals(m.get("principal"))
            && (indicesMatcher == null ? false == m.containsKey("indices") : indicesMatcher.matches(m.get("indices")));
    }

    private void clearAuditEvents() throws Exception {
        try {
            assertBusy(() -> {
                try {
                    adminClient().performRequest("POST", "/.security_audit_log-*/_delete_by_query", emptyMap(),
                            new StringEntity("{\"query\":{\"match_all\":{}}}", ContentType.APPLICATION_JSON));
                } catch (ResponseException e) {
                    logger.info("Conflict while clearing audit logs");
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