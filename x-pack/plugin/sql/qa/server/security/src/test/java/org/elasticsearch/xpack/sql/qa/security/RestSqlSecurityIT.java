/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.security;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.cbor.CborXContent;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.NotEqualMessageBuilder;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.io.InputStream;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.sql.qa.rest.BaseRestSqlTestCase.cursor;
import static org.elasticsearch.xpack.sql.qa.rest.BaseRestSqlTestCase.query;
import static org.elasticsearch.xpack.sql.qa.rest.BaseRestSqlTestCase.randomMode;
import static org.elasticsearch.xpack.sql.qa.rest.RestSqlTestCase.SQL_QUERY_REST_ENDPOINT;
import static org.elasticsearch.xpack.sql.qa.rest.RestSqlTestCase.columnInfo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class RestSqlSecurityIT extends SqlSecurityTestCase {
    private static class RestActions implements Actions {
        @Override
        public String minimalPermissionsForAllActions() {
            return "rest_minimal";
        }

        @Override
        public void queryWorksAsAdmin() throws Exception {
            String mode = randomMode();
            Map<String, Object> expected = new HashMap<>();
            expected.put(
                "columns",
                Arrays.asList(
                    columnInfo(mode, "a", "long", JDBCType.BIGINT, 20),
                    columnInfo(mode, "b", "long", JDBCType.BIGINT, 20),
                    columnInfo(mode, "c", "long", JDBCType.BIGINT, 20)
                )
            );
            expected.put("rows", Arrays.asList(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6)));

            assertResponse(expected, runSql(null, mode, "SELECT * FROM test ORDER BY a", false));
        }

        @Override
        public void expectMatchesAdmin(String adminSql, String user, String userSql) throws Exception {
            String mode = randomMode();
            assertResponse(runSql(null, mode, adminSql, false), runSql(user, mode, userSql, false));
        }

        @Override
        public void expectScrollMatchesAdmin(String adminSql, String user, String userSql) throws Exception {
            String mode = randomMode();
            Map<String, Object> adminResponse = runSql(
                null,
                new StringEntity(query(adminSql).mode(mode).fetchSize(1).toString(), ContentType.APPLICATION_JSON),
                mode,
                false
            );
            Map<String, Object> otherResponse = runSql(
                user,
                new StringEntity(query(adminSql).mode(mode).fetchSize(1).toString(), ContentType.APPLICATION_JSON),
                mode,
                false
            );

            String adminCursor = (String) adminResponse.remove("cursor");
            String otherCursor = (String) otherResponse.remove("cursor");
            assertNotNull(adminCursor);
            assertNotNull(otherCursor);
            assertResponse(adminResponse, otherResponse);
            while (true) {
                adminResponse = runSql(
                    null,
                    new StringEntity(cursor(adminCursor).mode(mode).toString(), ContentType.APPLICATION_JSON),
                    mode,
                    false
                );
                otherResponse = runSql(
                    user,
                    new StringEntity(cursor(otherCursor).mode(mode).toString(), ContentType.APPLICATION_JSON),
                    mode,
                    false
                );
                adminCursor = (String) adminResponse.remove("cursor");
                otherCursor = (String) otherResponse.remove("cursor");
                assertResponse(adminResponse, otherResponse);
                if (adminCursor == null) {
                    assertNull(otherCursor);
                    return;
                }
                assertNotNull(otherCursor);
            }
        }

        @Override
        public void expectDescribe(Map<String, List<String>> columns, String user) throws Exception {
            String mode = randomMode();
            Map<String, Object> expected = new HashMap<>(3);
            expected.put(
                "columns",
                Arrays.asList(
                    columnInfo(mode, "column", "keyword", JDBCType.VARCHAR, 32766),
                    columnInfo(mode, "type", "keyword", JDBCType.VARCHAR, 32766),
                    columnInfo(mode, "mapping", "keyword", JDBCType.VARCHAR, 32766)
                )
            );
            List<List<String>> rows = new ArrayList<>(columns.size());
            for (Map.Entry<String, List<String>> column : columns.entrySet()) {
                List<String> cols = new ArrayList<>();
                cols.add(column.getKey());
                cols.addAll(column.getValue());
                rows.add(cols);
            }
            expected.put("rows", rows);

            assertResponse(expected, runSql(user, mode, "DESCRIBE test", false));
        }

        @Override
        public void expectShowTables(List<String> tables, String user) throws Exception {
            String mode = randomMode();
            List<Object> columns = new ArrayList<>();
            columns.add(columnInfo(mode, "name", "keyword", JDBCType.VARCHAR, 32766));
            columns.add(columnInfo(mode, "type", "keyword", JDBCType.VARCHAR, 32766));
            columns.add(columnInfo(mode, "kind", "keyword", JDBCType.VARCHAR, 32766));
            Map<String, Object> expected = new HashMap<>();
            expected.put("columns", columns);
            List<List<String>> rows = new ArrayList<>();
            for (String table : tables) {
                List<String> fields = new ArrayList<>();
                fields.add(table);
                fields.add("TABLE");
                fields.add("INDEX");
                rows.add(fields);
            }
            expected.put("rows", rows);

            // Allow system index deprecation warnings, because this may return `.security*` indices.
            Map<String, Object> actual = runSql(user, mode, "SHOW TABLES", true);
            /*
             * Security automatically creates either a `.security` or a
             * `.security6` index but it might not have created the index
             * by the time the test runs.
             */
            @SuppressWarnings("unchecked")
            List<List<String>> rowsNoSecurity = ((List<List<String>>) actual.get("rows")).stream()
                .filter(ls -> ls.get(0).startsWith(".security") == false)
                .collect(Collectors.toList());
            actual.put("rows", rowsNoSecurity);
            assertResponse(expected, actual);
        }

        @Override
        public void expectForbidden(String user, String sql) {
            ResponseException e = expectThrows(ResponseException.class, () -> runSql(user, randomMode(), sql, false));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(e.getMessage(), containsString("unauthorized"));
        }

        @Override
        public void expectUnknownIndex(String user, String sql) {
            ResponseException e = expectThrows(ResponseException.class, () -> runSql(user, randomMode(), sql, false));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(e.getMessage(), containsString("Unknown index"));
        }

        @Override
        public void expectUnknownColumn(String user, String sql, String column) throws Exception {
            ResponseException e = expectThrows(ResponseException.class, () -> runSql(user, randomMode(), sql, false));
            assertThat(e.getMessage(), containsString("Unknown column [" + column + "]"));
        }

        @Override
        public void checkNoMonitorMain(String user) throws Exception {
            // Without monitor/main everything should work just fine
            expectMatchesAdmin("SELECT * FROM test", user, "SELECT * FROM test");
            expectMatchesAdmin("SHOW TABLES LIKE 'test'", user, "SHOW TABLES LIKE 'test'");
            expectMatchesAdmin("DESCRIBE test", user, "DESCRIBE test");
        }

        private static Map<String, Object> runSql(
            @Nullable String asUser,
            String mode,
            String sql,
            boolean allowSystemIndexDeprecationWarning
        ) throws IOException {
            return runSql(
                asUser,
                new StringEntity(query(sql).mode(mode).toString(), ContentType.APPLICATION_JSON),
                mode,
                allowSystemIndexDeprecationWarning
            );
        }

        private static Map<String, Object> runSql(
            @Nullable String asUser,
            HttpEntity entity,
            String mode,
            boolean allowSystemIndexDeprecationWarning
        ) throws IOException {
            Request request = new Request("POST", SQL_QUERY_REST_ENDPOINT);
            RequestOptions.Builder options = request.getOptions().toBuilder();
            if (asUser != null) {
                options.addHeader("es-security-runas-user", asUser);
            }
            if (allowSystemIndexDeprecationWarning) {
                options.setWarningsHandler(warnings -> {
                    if (warnings.isEmpty()) {
                        // No warnings is OK
                        return false;
                    } else if (warnings.size() > 2) {
                        return true;
                    } else {
                        for (String warning : warnings) {
                            if (warning.startsWith("this request accesses system indices: ") == false
                                && warning.startsWith("this request accesses aliases with names reserved for system indices: ") == false) {
                                return true;
                            }
                        }
                        return false;
                    }
                });
            }
            request.setOptions(options);
            request.setEntity(entity);
            return toMap(client().performRequest(request), mode);
        }

        private static void assertResponse(Map<String, Object> expected, Map<String, Object> actual) {
            if (false == expected.equals(actual)) {
                NotEqualMessageBuilder message = new NotEqualMessageBuilder();
                message.compareMaps(actual, expected);
                fail("Response does not match:\n" + message.toString());
            }
        }

        private static Map<String, Object> toMap(Response response, String mode) throws IOException {
            try (InputStream content = response.getEntity().getContent()) {
                if (mode.equalsIgnoreCase("jdbc")) {
                    return XContentHelper.convertToMap(CborXContent.cborXContent, content, false);
                } else {
                    return XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
                }
            }
        }
    }

    public RestSqlSecurityIT() {
        super(new RestActions());
    }

    @Override
    protected AuditLogAsserter createAuditLogAsserter() {
        return new RestAuditLogAsserter();
    }

    /**
     * Test the hijacking a scroll fails. This test is only implemented for
     * REST because it is the only API where it is simple to hijack a scroll.
     * It should exercise the same code as the other APIs but if we were truly
     * paranoid we'd hack together something to test the others as well.
     */
    public void testHijackScrollFails() throws Exception {
        createUser("full_access", "rest_minimal");
        final String mode = randomMode();

        Map<String, Object> adminResponse = RestActions.runSql(
            null,
            new StringEntity(query("SELECT * FROM test").mode(mode).fetchSize(1).toString(), ContentType.APPLICATION_JSON),
            mode,
            false
        );

        String cursor = (String) adminResponse.remove("cursor");
        assertNotNull(cursor);

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> RestActions.runSql(
                "full_access",
                new StringEntity(cursor(cursor).mode(mode).toString(), ContentType.APPLICATION_JSON),
                mode,
                false
            )
        );
        // TODO return a better error message for bad scrolls
        assertThat(e.getMessage(), containsString("No search context found for id"));
        assertEquals(404, e.getResponse().getStatusLine().getStatusCode());

        createAuditLogAsserter().expectSqlCompositeActionFieldCaps("test_admin", "test")
            .expect(true, SQL_ACTION_NAME, "full_access", empty())
            // one scroll access denied per shard
            .expect("access_denied", SQL_ACTION_NAME, "full_access", "default_native", empty(), "InternalScrollSearchRequest")
            .assertLogs();
    }

    protected class RestAuditLogAsserter extends AuditLogAsserter {
        @Override
        public AuditLogAsserter expect(
            String eventType,
            String action,
            String principal,
            String realm,
            Matcher<? extends Iterable<? extends String>> indicesMatcher,
            String request
        ) {
            final Matcher<String> runByPrincipalMatcher = principal.equals("test_admin")
                ? Matchers.nullValue(String.class)
                : Matchers.is("test_admin");
            final Matcher<String> runByRealmMatcher = realm.equals("default_file")
                ? Matchers.nullValue(String.class)
                : Matchers.is("default_file");
            logCheckers.add(
                m -> eventType.equals(m.get("event.action"))
                    && action.equals(m.get("action"))
                    && principal.equals(m.get("user.name"))
                    && realm.equals(m.get("user.realm"))
                    && runByPrincipalMatcher.matches(m.get("user.run_by.name"))
                    && runByRealmMatcher.matches(m.get("user.run_by.realm"))
                    && indicesMatcher.matches(m.get("indices"))
                    && request.equals(m.get("request.name"))
            );
            return this;
        }

    }
}
