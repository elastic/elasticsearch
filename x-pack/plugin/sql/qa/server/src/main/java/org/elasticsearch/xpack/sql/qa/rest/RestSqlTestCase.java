/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.rest;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.NotEqualMessageBuilder;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonStringEncoder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.sql.proto.CoreProtocol;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.StringUtils;
import org.elasticsearch.xpack.sql.qa.ErrorsTestCase;
import org.hamcrest.Matcher;
import org.junit.After;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.JDBCType;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.Strings.hasText;
import static org.elasticsearch.xpack.ql.TestUtils.getNumberOfSearchContexts;
import static org.elasticsearch.xpack.ql.index.VersionCompatibilityChecks.INTRODUCING_UNSIGNED_LONG;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.COLUMNS_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.HEADER_NAME_ASYNC_ID;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.HEADER_NAME_ASYNC_PARTIAL;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.HEADER_NAME_ASYNC_RUNNING;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.HEADER_NAME_CURSOR;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.ID_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.IS_PARTIAL_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.IS_RUNNING_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.ROWS_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.SQL_ASYNC_DELETE_REST_ENDPOINT;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.SQL_ASYNC_REST_ENDPOINT;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.SQL_ASYNC_STATUS_REST_ENDPOINT;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.URL_PARAM_DELIMITER;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.URL_PARAM_FORMAT;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.WAIT_FOR_COMPLETION_TIMEOUT_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Integration test for the rest sql action. The one that speaks json directly to a
 * user rather than to the JDBC driver or CLI.
 */
public abstract class RestSqlTestCase extends BaseRestSqlTestCase implements ErrorsTestCase {

    public static String SQL_QUERY_REST_ENDPOINT = CoreProtocol.SQL_QUERY_REST_ENDPOINT;
    private static String SQL_TRANSLATE_REST_ENDPOINT = CoreProtocol.SQL_TRANSLATE_REST_ENDPOINT;

    /**
     * Builds that map that is returned in the header for each column.
     */
    public static Map<String, Object> columnInfo(String mode, String name, String type, JDBCType jdbcType, int size) {
        Map<String, Object> column = new HashMap<>();
        column.put("name", name);
        column.put("type", type);
        if ("jdbc".equals(mode)) {
            column.put("display_size", size);
        }
        return unmodifiableMap(column);
    }

    protected String indexPattern(String pattern) {
        return pattern;
    }

    @After
    private void cleanup() throws IOException {
        try {
            deleteTestIndex();
        } catch (ResponseException e) {
            // while the majority of tests use the index(...) test method, few create their own index
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    public void testBasicQuery() throws IOException {
        index("{\"test\":\"test\"}", "{\"test\":\"test\"}");

        Map<String, Object> expected = new HashMap<>();
        String mode = randomMode();
        boolean columnar = randomBoolean();

        expected.put("columns", singletonList(columnInfo(mode, "test", "text", JDBCType.VARCHAR, Integer.MAX_VALUE)));
        if (columnar) {
            expected.put("values", singletonList(Arrays.asList("test", "test")));
        } else {
            expected.put("rows", Arrays.asList(singletonList("test"), singletonList("test")));
        }
        assertResponse(expected, runSql(mode, "SELECT * FROM " + indexPattern("test"), columnar));
    }

    public void testNextPage() throws IOException {
        final int count = 20;
        bulkLoadTestData(count);

        String mode = randomMode();
        boolean columnar = randomBoolean();
        String sqlRequest = query(
            "SELECT text, number, SQRT(number) AS s, SCORE() FROM " + indexPattern("test") + " ORDER BY number, SCORE()"
        ).mode(mode).fetchSize(2).columnar(columnarValue(columnar)).toString();

        Number value = xContentDependentFloatingNumberValue(mode, 1f);
        String cursor = null;
        for (int i = 0; i < count; i += 2) {
            Map<String, Object> response;
            if (i == 0) {
                response = runSql(new StringEntity(sqlRequest, ContentType.APPLICATION_JSON), "", mode);
            } else {
                columnar = randomBoolean();
                response = runSql(
                    new StringEntity(cursor(cursor).mode(mode).columnar(columnarValue(columnar)).toString(), ContentType.APPLICATION_JSON),
                    StringUtils.EMPTY,
                    mode
                );
            }

            Map<String, Object> expected = new HashMap<>();
            if (i == 0) {
                expected.put(
                    "columns",
                    Arrays.asList(
                        columnInfo(mode, "text", "text", JDBCType.VARCHAR, Integer.MAX_VALUE),
                        columnInfo(mode, "number", "long", JDBCType.BIGINT, 20),
                        columnInfo(mode, "s", "double", JDBCType.DOUBLE, 25),
                        columnInfo(mode, "SCORE()", "float", JDBCType.REAL, 15)
                    )
                );
            }

            if (columnar) {
                expected.put(
                    "values",
                    Arrays.asList(
                        Arrays.asList("text" + i, "text" + (i + 1)),
                        Arrays.asList(i, i + 1),
                        Arrays.asList(Math.sqrt(i), Math.sqrt(i + 1)),
                        Arrays.asList(value, value)
                    )
                );
            } else {
                expected.put(
                    "rows",
                    Arrays.asList(
                        Arrays.asList("text" + i, i, Math.sqrt(i), value),
                        Arrays.asList("text" + (i + 1), i + 1, Math.sqrt(i + 1), value)
                    )
                );
            }
            cursor = (String) response.remove("cursor");
            assertResponse(expected, response);
            assertNotNull(cursor);
        }
        Map<String, Object> expected = new HashMap<>();
        columnar = randomBoolean();
        if (columnar) {
            expected.put("values", emptyList());
        } else {
            expected.put("rows", emptyList());
        }
        assertResponse(
            expected,
            runSql(
                new StringEntity(cursor(cursor).mode(mode).columnar(columnarValue(columnar)).toString(), ContentType.APPLICATION_JSON),
                StringUtils.EMPTY,
                mode
            )
        );
    }

    public void testNextPageWithDatetimeAndTimezoneParam() throws IOException {
        Request request = new Request("PUT", "/test_date_timezone");
        XContentBuilder createIndex = JsonXContent.contentBuilder().startObject();
        createIndex.startObject("mappings");
        {
            createIndex.startObject("properties");
            {
                createIndex.startObject("date").field("type", "date").field("format", "epoch_millis");
                createIndex.endObject();
            }
            createIndex.endObject();
        }
        createIndex.endObject().endObject();
        request.setJsonEntity(Strings.toString(createIndex));
        provisioningClient().performRequest(request);

        request = new Request("PUT", "/test_date_timezone/_bulk");
        request.addParameter("refresh", "true");
        StringBuilder bulk = new StringBuilder();
        long[] datetimes = new long[] { 1_000, 10_000, 100_000, 1_000_000, 10_000_000 };
        for (long datetime : datetimes) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"date\":").append(datetime).append("}\n");
        }
        request.setJsonEntity(bulk.toString());
        assertEquals(200, provisioningClient().performRequest(request).getStatusLine().getStatusCode());

        ZoneId zoneId = randomZone();
        String mode = randomMode();
        String sqlRequest = query("SELECT DATE_PART('TZOFFSET', date) AS tz FROM " + indexPattern("test_date_timezone") + " ORDER BY date")
            .timeZone(zoneId.getId())
            .mode(mode)
            .fetchSize(2)
            .toString();

        String cursor = null;
        for (int i = 0; i <= datetimes.length; i += 2) {
            Map<String, Object> expected = new HashMap<>();
            Map<String, Object> response;

            if (i == 0) {
                expected.put("columns", singletonList(columnInfo(mode, "tz", "integer", JDBCType.INTEGER, 11)));
                response = runSql(new StringEntity(sqlRequest, ContentType.APPLICATION_JSON), "", mode);
            } else {
                assertNotNull(cursor);
                response = runSql(
                    new StringEntity(cursor(cursor).mode(mode).toString(), ContentType.APPLICATION_JSON),
                    StringUtils.EMPTY,
                    mode
                );
            }

            List<Object> values = new ArrayList<>(2);
            for (int j = 0; j < (i < datetimes.length - 1 ? 2 : 1); j++) {
                values.add(
                    singletonList(
                        ZonedDateTime.ofInstant(Instant.ofEpochMilli(datetimes[i + j]), zoneId).getOffset().getTotalSeconds() / 60
                    )
                );
            }
            expected.put("rows", values);
            assertTrue(response.containsKey("cursor") == false || response.get("cursor") != null);
            cursor = (String) response.remove("cursor");
            assertResponse(expected, response);
        }

        assertNull(cursor);

        deleteIndexWithProvisioningClient("test_date_timezone");
    }

    @AwaitsFix(bugUrl = "Unclear status, https://github.com/elastic/x-pack-elasticsearch/issues/2074")
    public void testTimeZone() throws IOException {
        String mode = randomMode();
        boolean columnar = randomBoolean();
        index("{\"test\":\"2017-07-27 00:00:00\"}", "{\"test\":\"2017-07-27 01:00:00\"}");

        Map<String, Object> expected = new HashMap<>();
        expected.put("columns", singletonMap("test", singletonMap("type", "text")));
        if (columnar) {
            expected.put("values", Arrays.asList(singletonMap("test", "test"), singletonMap("test", "test")));
        } else {
            // TODO: what exactly is this test suppossed to do. We need to check the 2074 issue above.
            expected.put("rows", Arrays.asList(singletonMap("test", "test"), singletonMap("test", "test")));
        }
        expected.put("size", 2);

        // Default TimeZone is UTC
        assertResponse(expected, runSql(mode, "SELECT DAY_OF_YEAR(test), COUNT(*) FROM " + indexPattern("test"), columnar));
    }

    public void testScoreWithFieldNamedScore() throws IOException {
        Request request = new Request("POST", "/test/_bulk");
        request.addParameter("refresh", "true");
        String mode = randomMode();
        String bulk = """
            {"index":{"_id":"1"}}
            {"name":"test", "score":10}
            """;
        request.setJsonEntity(bulk);
        provisioningClient().performRequest(request);

        Map<String, Object> expected = new HashMap<>();
        boolean columnar = randomBoolean();
        expected.put(
            "columns",
            Arrays.asList(
                columnInfo(mode, "name", "text", JDBCType.VARCHAR, Integer.MAX_VALUE),
                columnInfo(mode, "score", "long", JDBCType.BIGINT, 20),
                columnInfo(mode, "SCORE()", "float", JDBCType.REAL, 15)
            )
        );
        Number value = xContentDependentFloatingNumberValue(mode, 1f);
        if (columnar) {
            expected.put("values", Arrays.asList(singletonList("test"), singletonList(10), singletonList(value)));
        } else {
            expected.put("rows", singletonList(Arrays.asList("test", 10, value)));
        }

        assertResponse(expected, runSql(mode, "SELECT *, SCORE() FROM " + indexPattern("test") + " ORDER BY SCORE()", columnar));
        assertResponse(
            expected,
            runSql(mode, "SELECT name, \\\"score\\\", SCORE() FROM " + indexPattern("test") + " ORDER BY SCORE()", columnar)
        );
    }

    public void testSelectWithJoinFails() throws Exception {
        // Normal join not supported
        expectBadRequest(
            () -> runSql(randomMode(), "SELECT * FROM " + indexPattern("test") + " JOIN other"),
            containsString(": Queries with JOIN are not yet supported")
        );
        // Neither is a self join
        expectBadRequest(
            () -> runSql(randomMode(), "SELECT * FROM " + indexPattern("test") + " JOIN test"),
            containsString(": Queries with JOIN are not yet supported")
        );
        // Nor fancy stuff like CTEs
        expectBadRequest(
            () -> runSql(
                randomMode(),
                "WITH evil AS (SELECT * FROM " + indexPattern("foo") + ") SELECT * FROM " + indexPattern("test") + " JOIN evil"
            ),
            containsString(": Queries with JOIN are not yet supported")
        );
    }

    public void testSelectGroupByAllFails() throws Exception {
        index("{\"foo\":1}", "{\"foo\":2}");
        expectBadRequest(
            () -> runSql(randomMode(), "SELECT foo FROM " + indexPattern("test") + " GROUP BY ALL foo"),
            containsString(": GROUP BY ALL is not supported")
        );
    }

    @Override
    public void testSelectInvalidSql() {
        String mode = randomFrom("jdbc", "plain");
        expectBadRequest(() -> runSql(mode, "SELECT * FRO"), containsString("1:8: Cannot determine columns for [*]"));
    }

    @Override
    public void testSelectFromMissingIndex() {
        String mode = randomFrom("jdbc", "plain");
        expectBadRequest(() -> runSql(mode, "SELECT * FROM missing"), containsString("1:15: Unknown index [missing]"));
    }

    @Override
    public void testSelectColumnFromMissingIndex() throws Exception {
        String mode = randomFrom("jdbc", "plain");
        expectBadRequest(() -> runSql(mode, "SELECT abc FROM missing"), containsString("1:17: Unknown index [missing]"));
    }

    @Override
    public void testSelectColumnFromEmptyIndex() throws Exception {
        Request request = new Request("PUT", "/test");
        request.setJsonEntity("{}");
        provisioningClient().performRequest(request);
        String mode = randomFrom("jdbc", "plain");
        expectBadRequest(() -> runSql(mode, "SELECT abc FROM " + indexPattern("test")), containsString("1:8: Unknown column [abc]"));
    }

    @Override
    public void testSelectMissingField() throws IOException {
        index("{\"test\":\"test\"}");
        String mode = randomFrom("jdbc", "plain");
        expectBadRequest(() -> runSql(mode, "SELECT foo FROM " + indexPattern("test")), containsString("1:8: Unknown column [foo]"));
    }

    @Override
    public void testSelectMissingFunction() throws Exception {
        index("{\"foo\":1}");
        expectBadRequest(
            () -> runSql(randomMode(), "SELECT missing(foo) FROM " + indexPattern("test")),
            containsString("1:8: Unknown function [missing]")
        );
    }

    @Override
    public void testSelectProjectScoreInAggContext() throws Exception {
        index("{\"foo\":1}");
        expectBadRequest(
            () -> runSql(randomMode(), "     SELECT foo, SCORE(), COUNT(*)" + "     FROM " + indexPattern("test") + " GROUP BY foo"),
            containsString("Cannot use non-grouped column [SCORE()], expected [foo]")
        );
    }

    @Override
    public void testSelectOrderByScoreInAggContext() throws Exception {
        index("{\"foo\":1}");
        expectBadRequest(
            () -> runSql(randomMode(), "SELECT foo, COUNT(*) FROM " + indexPattern("test") + " GROUP BY foo ORDER BY SCORE()"),
            containsString("Cannot order by non-grouped column [SCORE()], expected [foo]")
        );
    }

    @Override
    public void testSelectGroupByScore() throws Exception {
        index("{\"foo\":1}");
        expectBadRequest(
            () -> runSql(randomMode(), "SELECT COUNT(*) FROM " + indexPattern("test") + " GROUP BY SCORE()"),
            containsString("Cannot use [SCORE()] for grouping")
        );
    }

    public void testCountAndCountDistinct() throws IOException {
        String mode = randomMode();
        index(
            "test",
            "{\"gender\":\"m\", \"langs\": 1}",
            "{\"gender\":\"m\", \"langs\": 1}",
            "{\"gender\":\"m\", \"langs\": 2}",
            "{\"gender\":\"m\", \"langs\": 3}",
            "{\"gender\":\"m\", \"langs\": 3}",
            "{\"gender\":\"f\", \"langs\": 1}",
            "{\"gender\":\"f\", \"langs\": 2}",
            "{\"gender\":\"f\", \"langs\": 2}",
            "{\"gender\":\"f\", \"langs\": 2}",
            "{\"gender\":\"f\", \"langs\": 3}",
            "{\"gender\":\"f\", \"langs\": 3}"
        );

        Map<String, Object> expected = new HashMap<>();
        boolean columnar = randomBoolean();
        expected.put(
            "columns",
            Arrays.asList(
                columnInfo(mode, "gender", "text", JDBCType.VARCHAR, Integer.MAX_VALUE),
                columnInfo(mode, "cnt", "long", JDBCType.BIGINT, 20),
                columnInfo(mode, "cnt_dist", "long", JDBCType.BIGINT, 20)
            )
        );
        if (columnar) {
            expected.put("values", Arrays.asList(Arrays.asList("f", "m"), Arrays.asList(6, 5), Arrays.asList(3, 3)));
        } else {
            expected.put("rows", Arrays.asList(Arrays.asList("f", 6, 3), Arrays.asList("m", 5, 3)));
        }

        Map<String, Object> response = runSql(
            mode,
            "SELECT gender, COUNT(langs) AS cnt, COUNT(DISTINCT langs) AS cnt_dist "
                + "FROM "
                + indexPattern("test")
                + " GROUP BY gender ORDER BY gender",
            columnar
        );

        String cursor = (String) response.remove("cursor");
        assertNull(cursor);
        assertResponse(expected, response);
    }

    @Override
    public void testSelectScoreSubField() throws Exception {
        index("{\"foo\":1}");
        expectBadRequest(
            () -> runSql(randomMode(), "SELECT SCORE().bar FROM " + indexPattern("test")),
            containsString(": mismatched input '.' expecting {<EOF>, ")
        );
    }

    @Override
    public void testHardLimitForSortOnAggregate() throws Exception {
        index("{\"a\": 1, \"b\": 2}");
        expectBadRequest(
            () -> runSql(randomMode(), "SELECT max(a) max FROM " + indexPattern("test") + " GROUP BY b ORDER BY max LIMIT 120000"),
            containsString("The maximum LIMIT for aggregate sorting is [65536], received [120000]")
        );
    }

    public void testUseColumnarForUnsupportedFormats() throws Exception {
        String format = randomFrom("txt", "csv", "tsv");
        index("{\"foo\":1}");

        Request request = new Request("POST", SQL_QUERY_REST_ENDPOINT);
        request.addParameter("error_trace", "true");
        request.addParameter("pretty", "true");
        request.addParameter("format", format);
        request.setEntity(
            new StringEntity(
                query("SELECT * FROM " + indexPattern("test")).mode(
                    randomValueOtherThan(Mode.JDBC.toString(), BaseRestSqlTestCase::randomMode)
                ).columnar(true).toString(),
                ContentType.APPLICATION_JSON
            )
        );
        expectBadRequest(() -> {
            client().performRequest(request);
            return emptyMap();
        }, containsString("Invalid use of [columnar] argument: cannot be used in combination with txt, csv or tsv formats"));
    }

    public void testUseColumnarForTranslateRequest() throws IOException {
        index("{\"test\":\"test\"}", "{\"test\":\"test\"}");

        String mode = randomMode();
        Request request = new Request("POST", SQL_TRANSLATE_REST_ENDPOINT);
        request.setEntity(
            new StringEntity(
                query("SELECT * FROM " + indexPattern("test")).mode(mode).columnar(true).toString(),
                ContentType.APPLICATION_JSON
            )
        );
        expectBadRequest(() -> {
            client().performRequest(request);
            return emptyMap();
        }, containsString("unknown field [columnar]"));
    }

    public void testValidateRuntimeMappingsInSqlQuery() throws IOException {
        testValidateRuntimeMappingsInQuery(SQL_QUERY_REST_ENDPOINT);

        String mode = randomMode();
        Request request = new Request("POST", SQL_QUERY_REST_ENDPOINT);
        index("{\"test\":true}", "{\"test\":false}");
        String runtimeMappings = "{\"bool_as_long\": {\"type\":\"long\", \"script\": {\"source\":\"if(doc['test'].value == true) emit(1);"
            + "else emit(0);\"}}}";
        request.setEntity(
            new StringEntity(
                query("SELECT * FROM " + indexPattern("test")).mode(mode).runtimeMappings(runtimeMappings).toString(),
                ContentType.APPLICATION_JSON
            )
        );
        Map<String, Object> expected = new HashMap<>();
        expected.put(
            "columns",
            Arrays.asList(
                columnInfo(mode, "bool_as_long", "long", JDBCType.BIGINT, 20),
                columnInfo(mode, "test", "boolean", JDBCType.BOOLEAN, 1)
            )
        );
        expected.put("rows", Arrays.asList(Arrays.asList(1, true), Arrays.asList(0, false)));
        assertResponse(
            expected,
            runSql(
                new StringEntity(
                    query("SELECT * FROM " + indexPattern("test")).mode(mode).runtimeMappings(runtimeMappings).toString(),
                    ContentType.APPLICATION_JSON
                ),
                StringUtils.EMPTY,
                mode
            )
        );
    }

    public void testValidateRuntimeMappingsInTranslateQuery() throws IOException {
        testValidateRuntimeMappingsInQuery(SQL_TRANSLATE_REST_ENDPOINT);

        index("{\"test\":true}", "{\"test\":false}");
        String runtimeMappings = "{\"bool_as_long\": {\"type\":\"long\", \"script\": {\"source\":\"if(doc['test'].value == true) emit(1);"
            + "else emit(0);\"}}}";
        Map<String, Object> response = runTranslateSql(
            query("SELECT * FROM " + indexPattern("test")).runtimeMappings(runtimeMappings).toString()
        );
        assertEquals(response.get("size"), 1000);
        assertFalse((Boolean) response.get("_source"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> source = (List<Map<String, Object>>) response.get("fields");
        assertEquals(Arrays.asList(singletonMap("field", "bool_as_long"), singletonMap("field", "test")), source);

        assertNull(response.get("query"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> sort = (List<Map<String, Object>>) response.get("sort");
        assertEquals(singletonList(singletonMap("_doc", singletonMap("order", "asc"))), sort);
    }

    private void testValidateRuntimeMappingsInQuery(String queryTypeEndpoint) {
        String mode = randomMode();
        String runtimeMappings = "{\"address\": {\"script\": \"return\"}}";
        Request request = new Request("POST", queryTypeEndpoint);
        request.setEntity(
            new StringEntity(
                query("SELECT * FROM " + indexPattern("test")).mode(mode).runtimeMappings(runtimeMappings).toString(),
                ContentType.APPLICATION_JSON
            )
        );
        expectBadRequest(() -> {
            client().performRequest(request);
            return emptyMap();
        }, containsString("No type specified for runtime field [address]"));

        runtimeMappings = "{\"address\": [{\"script\": \"return\"}]}";
        request.setEntity(
            new StringEntity(
                query("SELECT * FROM " + indexPattern("test")).mode(mode).runtimeMappings(runtimeMappings).toString(),
                ContentType.APPLICATION_JSON
            )
        );
        expectBadRequest(() -> {
            client().performRequest(request);
            return emptyMap();
        }, containsString("Expected map for runtime field [address] definition but got [String]"));
    }

    public static void expectBadRequest(CheckedSupplier<Map<String, Object>, Exception> code, Matcher<String> errorMessageMatcher) {
        try {
            Map<String, Object> result = code.get();
            fail("expected ResponseException but got " + result);
        } catch (ResponseException e) {
            if (400 != e.getResponse().getStatusLine().getStatusCode()) {
                String body;
                try {
                    body = Streams.copyToString(new InputStreamReader(e.getResponse().getEntity().getContent(), StandardCharsets.UTF_8));
                } catch (IOException bre) {
                    throw new RuntimeException("error reading body after remote sent bad status", bre);
                }
                fail("expected [400] response but get [" + e.getResponse().getStatusLine().getStatusCode() + "] with body:\n" + body);
            }
            assertThat(e.getMessage(), errorMessageMatcher);
        } catch (Exception e) {
            throw new AssertionError("expected ResponseException but got [" + e.getClass() + "]", e);
        }
    }

    private Map<String, Object> runSql(String mode, String sql) throws IOException {
        return runSql(mode, sql, StringUtils.EMPTY, randomBoolean());
    }

    private Map<String, Object> runSql(String mode, String sql, boolean columnar) throws IOException {
        return runSql(mode, sql, StringUtils.EMPTY, columnar);
    }

    private Map<String, Object> runSql(String mode, String sql, String suffix, boolean columnar) throws IOException {
        // put an explicit "columnar": false parameter or omit it altogether, it should make no difference
        return runSql(
            new StringEntity(query(sql).mode(mode).columnar(columnarValue(columnar)).toString(), ContentType.APPLICATION_JSON),
            suffix,
            mode
        );
    }

    protected Map<String, Object> runTranslateSql(String sql) throws IOException {
        return runSql(new StringEntity(sql, ContentType.APPLICATION_JSON), "/translate/", Mode.PLAIN.toString());
    }

    private static Boolean columnarValue(boolean columnar) {
        return columnar ? Boolean.TRUE : (randomBoolean() ? null : Boolean.FALSE);
    }

    protected Map<String, Object> runSql(HttpEntity sql, String suffix, String mode) throws IOException {
        Request request = new Request("POST", SQL_QUERY_REST_ENDPOINT + suffix);
        request.addParameter("error_trace", "true");   // Helps with debugging in case something crazy happens on the server.
        request.addParameter("pretty", "true");        // Improves error reporting readability
        if (randomBoolean()) {
            // We default to JSON but we force it randomly for extra coverage
            request.addParameter("format", "json");
        }
        if (randomBoolean()) {
            // JSON is the default but randomly set it sometime for extra coverage
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Accept", randomFrom("*/*", "application/json"));
            request.setOptions(options);
        }
        request.setEntity(sql);
        return toMap(client().performRequest(request), mode);
    }

    public void testPrettyPrintingEnabled() throws IOException {
        boolean columnar = randomBoolean();
        String expected = "";
        if (columnar) {
            expected = """
                {
                  "columns" : [
                    {
                      "name" : "test1",
                      "type" : "text"
                    }
                  ],
                  "values" : [
                    [
                      "test1",
                      "test2"
                    ]
                  ]
                }
                """;
        } else {
            expected = """
                {
                  "columns" : [
                    {
                      "name" : "test1",
                      "type" : "text"
                    }
                  ],
                  "rows" : [
                    [
                      "test1"
                    ],
                    [
                      "test2"
                    ]
                  ]
                }
                """;
        }
        executeAndAssertPrettyPrinting(expected, "true", columnar);
    }

    public void testPrettyPrintingDisabled() throws IOException {
        boolean columnar = randomBoolean();
        String expected = "";
        if (columnar) {
            expected = """
                {"columns":[{"name":"test1","type":"text"}],"values":[["test1","test2"]]}""";
        } else {
            expected = """
                {"columns":[{"name":"test1","type":"text"}],"rows":[["test1"],["test2"]]}""";
        }
        executeAndAssertPrettyPrinting(expected, randomFrom("false", null), columnar);
    }

    private void executeAndAssertPrettyPrinting(String expectedJson, String prettyParameter, boolean columnar) throws IOException {
        index("{\"test1\":\"test1\"}", "{\"test1\":\"test2\"}");

        Request request = new Request("POST", SQL_QUERY_REST_ENDPOINT);
        if (prettyParameter != null) {
            request.addParameter("pretty", prettyParameter);
        }
        if (randomBoolean()) {
            // We default to JSON but we force it randomly for extra coverage
            request.addParameter("format", "json");
        }
        if (randomBoolean()) {
            // JSON is the default but randomly set it sometime for extra coverage
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Accept", randomFrom("*/*", "application/json"));
            request.setOptions(options);
        }
        request.setEntity(
            new StringEntity(
                query("SELECT * FROM " + indexPattern("test")).mode(Mode.PLAIN).columnar(columnarValue(columnar)).toString(),
                ContentType.APPLICATION_JSON
            )
        );

        Response response = client().performRequest(request);
        try (InputStream content = response.getEntity().getContent()) {
            String actualJson = new BytesArray(content.readAllBytes()).utf8ToString();
            assertEquals(expectedJson, actualJson);
        }
    }

    public void testBasicTranslateQuery() throws IOException {
        index("{\"test\":\"test\"}", "{\"test\":\"test\"}");

        Map<String, Object> response = runTranslateSql(query("SELECT * FROM " + indexPattern("test")).toString());
        assertEquals(1000, response.get("size"));
        assertFalse((Boolean) response.get("_source"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> source = (List<Map<String, Object>>) response.get("fields");
        assertEquals(singletonList(singletonMap("field", "test")), source);
    }

    public void testBasicQueryWithFilter() throws IOException {
        String mode = randomMode();
        index("{\"test\":\"foo\"}", "{\"test\":\"bar\"}");

        Map<String, Object> expected = new HashMap<>();
        expected.put("columns", singletonList(columnInfo(mode, "test", "text", JDBCType.VARCHAR, Integer.MAX_VALUE)));
        expected.put("rows", singletonList(singletonList("foo")));
        assertResponse(
            expected,
            runSql(
                new StringEntity(
                    query("SELECT * FROM " + indexPattern("test")).mode(mode).filter("{\"match\": {\"test\": \"foo\"}}").toString(),
                    ContentType.APPLICATION_JSON
                ),
                StringUtils.EMPTY,
                mode
            )
        );
    }

    public void testBasicQueryWithParameters() throws IOException {
        String mode = randomMode();
        boolean columnar = randomBoolean();
        index("{\"test\":\"foo\"}", "{\"test\":\"bar\"}");

        Map<String, Object> expected = new HashMap<>();
        expected.put(
            "columns",
            Arrays.asList(
                columnInfo(mode, "test", "text", JDBCType.VARCHAR, Integer.MAX_VALUE),
                columnInfo(mode, "param", "integer", JDBCType.INTEGER, 11)
            )
        );
        if (columnar) {
            expected.put("values", Arrays.asList(singletonList("foo"), singletonList(10)));
        } else {
            expected.put("rows", Arrays.asList(Arrays.asList("foo", 10)));
        }

        String params = mode.equals("jdbc")
            ? "{\"type\": \"integer\", \"value\": 10}, {\"type\": \"keyword\", \"value\": \"foo\"}"
            : "10, \"foo\"";
        assertResponse(
            expected,
            runSql(
                new StringEntity(
                    query("SELECT test, ? param FROM " + indexPattern("test") + " WHERE test = ?").mode(mode)
                        .columnar(columnarValue(columnar))
                        .params("[" + params + "]")
                        .toString(),
                    ContentType.APPLICATION_JSON
                ),
                StringUtils.EMPTY,
                mode
            )
        );
    }

    public void testBasicTranslateQueryWithFilter() throws IOException {
        index("{\"test\":\"foo\"}", "{\"test\":\"bar\"}");

        Map<String, Object> response = runTranslateSql(
            query("SELECT * FROM " + indexPattern("test")).filter("{\"match\": {\"test\": \"foo\"}}").toString()
        );

        assertEquals(response.get("size"), 1000);
        assertFalse((Boolean) response.get("_source"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> source = (List<Map<String, Object>>) response.get("fields");
        assertEquals(singletonList(singletonMap("field", "test")), source);

        @SuppressWarnings("unchecked")
        Map<String, Object> query = (Map<String, Object>) response.get("query");
        assertNotNull(query);

        @SuppressWarnings("unchecked")
        Map<String, Object> bool = (Map<String, Object>) query.get("bool");
        assertNotNull(bool);

        @SuppressWarnings("unchecked")
        List<Object> filter = (List<Object>) bool.get("filter");
        assertNotNull(filter);

        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) filter.get(0);
        assertNotNull(map);

        @SuppressWarnings("unchecked")
        Map<String, Object> matchQ = (Map<String, Object>) map.get("match");

        @SuppressWarnings("unchecked")
        Map<String, Object> matchQuery = (Map<String, Object>) matchQ.get("test");

        assertNotNull(matchQuery);
        assertEquals("foo", matchQuery.get("query"));
    }

    public void testTranslateQueryWithGroupByAndHaving() throws IOException {
        index("{\"salary\":100}", "{\"age\":20}");

        Map<String, Object> response = runTranslateSql(
            query("SELECT avg(salary) FROM " + indexPattern("test") + " GROUP BY abs(age) HAVING avg(salary) > 50 LIMIT 10").toString()
        );

        assertEquals(response.get("size"), 0);
        assertEquals(false, response.get("_source"));
        assertNull(response.get("stored_fields"));

        @SuppressWarnings("unchecked")
        Map<String, Object> aggregations = (Map<String, Object>) response.get("aggregations");
        assertEquals(1, aggregations.size());
        assertNotNull(aggregations);

        @SuppressWarnings("unchecked")
        Map<String, Object> groupby = (Map<String, Object>) aggregations.get("groupby");
        assertEquals(2, groupby.size());

        @SuppressWarnings("unchecked")
        Map<String, Object> composite = (Map<String, Object>) groupby.get("composite");
        assertEquals(2, composite.size());
        assertEquals(10, composite.get("size"));

        @SuppressWarnings("unchecked")
        List<Object> sources = (List<Object>) composite.get("sources");
        assertEquals(1, sources.size());

        @SuppressWarnings("unchecked")
        Map<String, Object> sourcesListMap = (Map<String, Object>) ((Map<String, Object>) sources.get(0)).values().iterator().next();
        assertEquals(1, sourcesListMap.size());

        @SuppressWarnings("unchecked")
        Map<String, Object> terms = (Map<String, Object>) sourcesListMap.get("terms");
        assertEquals(4, terms.size());
        assertEquals("long", terms.get("value_type"));
        assertEquals(true, terms.get("missing_bucket"));
        assertEquals("asc", terms.get("order"));

        @SuppressWarnings("unchecked")
        Map<String, Object> termsScript = (Map<String, Object>) terms.get("script");
        assertEquals(3, termsScript.size());
        assertEquals("InternalSqlScriptUtils.abs(InternalQlScriptUtils.docValue(doc,params.v0))", termsScript.get("source"));
        assertEquals("painless", termsScript.get("lang"));

        @SuppressWarnings("unchecked")
        Map<String, Object> termsScriptParams = (Map<String, Object>) termsScript.get("params");
        assertEquals(1, termsScriptParams.size());
        assertEquals("age", termsScriptParams.get("v0"));

        @SuppressWarnings("unchecked")
        Map<String, Object> aggregations2 = (Map<String, Object>) groupby.get("aggregations");
        assertEquals(2, aggregations2.size());

        List<String> aggKeys = new ArrayList<>(2);
        String aggFilterKey = null;
        for (Map.Entry<String, Object> entry : aggregations2.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("having")) {
                aggFilterKey = key;
            } else {
                aggKeys.add(key);
                @SuppressWarnings("unchecked")
                Map<String, Object> aggr = (Map<String, Object>) entry.getValue();
                assertEquals(1, aggr.size());
                @SuppressWarnings("unchecked")
                Map<String, Object> avg = (Map<String, Object>) aggr.get("avg");
                assertEquals(1, avg.size());
                assertEquals("salary", avg.get("field"));
            }
        }
        Collections.sort(aggKeys);
        assertEquals("having." + aggKeys.get(0), aggFilterKey);

        @SuppressWarnings("unchecked")
        Map<String, Object> having = (Map<String, Object>) aggregations2.get(aggFilterKey);
        assertEquals(1, having.size());

        @SuppressWarnings("unchecked")
        Map<String, Object> bucketSelector = (Map<String, Object>) having.get("bucket_selector");
        assertEquals(3, bucketSelector.size());
        assertEquals("skip", bucketSelector.get("gap_policy"));

        @SuppressWarnings("unchecked")
        Map<String, Object> bucketsPath = (Map<String, Object>) bucketSelector.get("buckets_path");
        assertEquals(1, bucketsPath.size());
        assertEquals(aggKeys.get(0).toString(), bucketsPath.get("a0"));

        @SuppressWarnings("unchecked")
        Map<String, Object> filterScript = (Map<String, Object>) bucketSelector.get("script");
        assertEquals(3, filterScript.size());
        assertEquals("InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.gt(params.a0,params.v0))", filterScript.get("source"));
        assertEquals("painless", filterScript.get("lang"));
        @SuppressWarnings("unchecked")
        Map<String, Object> filterScriptParams = (Map<String, Object>) filterScript.get("params");
        assertEquals(1, filterScriptParams.size());
        assertEquals(50, filterScriptParams.get("v0"));
    }

    public void testBasicQueryText() throws IOException {
        index("{\"test\":\"test\"}", "{\"test\":\"test\"}");

        String expected = """
                 test     \s
            ---------------
            test          \s
            test          \s
            """;
        Tuple<String, String> response = runSqlAsText(query("SELECT * FROM " + indexPattern("test")), "text/plain");
        assertEquals(expected, response.v1());
    }

    public void testNextPageText() throws IOException {
        executeQueryWithNextPage("text/plain", "     text      |    number     |      sum      \n", "%-15s|%-15d|%-15d\n");
    }

    // CSV/TSV tests

    private static String toJson(String value) {
        return "\"" + new String(JsonStringEncoder.getInstance().quoteAsString(value)) + "\"";
    }

    public void testDefaultQueryInCSV() throws IOException {
        index(
            "{\"name\":" + toJson("first") + ", \"number\" : 1 }",
            "{\"name\":" + toJson("second\t") + ", \"number\": 2 }",
            "{\"name\":" + toJson("\"third,\"") + ", \"number\": 3 }"
        );

        String expected = """
            name,number\r
            first,1\r
            second\t,2\r
            ""\"third,""\",3\r
            """;

        String query = "SELECT * FROM " + indexPattern("test") + " ORDER BY number";
        Tuple<String, String> response = runSqlAsText(query(query), "text/csv");
        assertEquals(expected, response.v1());

        response = runSqlAsTextWithFormat(query, "csv");
        assertEquals(expected, response.v1());
    }

    public void testQueryWithoutHeaderInCSV() throws IOException {
        index(
            "{\"name\":" + toJson("first") + ", \"number\" : 1 }",
            "{\"name\":" + toJson("second\t") + ", \"number\": 2 }",
            "{\"name\":" + toJson("\"third,\"") + ", \"number\": 3 }"
        );

        String expected = """
            first,1\r
            second\t,2\r
            ""\"third,""\",3\r
            """;

        String query = "SELECT * FROM " + indexPattern("test") + " ORDER BY number";
        Tuple<String, String> response = runSqlAsText(query(query), "text/csv; header=absent");
        assertEquals(expected, response.v1());
    }

    public void testNextPageCSV() throws IOException {
        executeQueryWithNextPage("text/csv; header=present", "text,number,sum\r\n", "%s,%d,%d\r\n");
    }

    public void testCSVWithDelimiterParameter() throws IOException {
        String format = randomFrom("txt", "tsv", "json", "yaml", "smile", "cbor");
        String query = "SELECT * FROM " + indexPattern("test");
        index("{\"foo\":1}");

        Request badRequest = new Request("POST", SQL_QUERY_REST_ENDPOINT);
        badRequest.addParameter("format", format);
        badRequest.addParameter("delimiter", ";");
        badRequest.setEntity(
            new StringEntity(
                query(query).mode(randomValueOtherThan(Mode.JDBC.toString(), BaseRestSqlTestCase::randomMode)).toString(),
                ContentType.APPLICATION_JSON
            )
        );
        expectBadRequest(() -> {
            client().performRequest(badRequest);
            return emptyMap();
        }, containsString("request [/_sql] contains unrecognized parameter: [delimiter]"));

        Request csvRequest = new Request("POST", SQL_QUERY_REST_ENDPOINT + "?format=csv&delimiter=%3B");
        csvRequest.setEntity(
            new StringEntity(
                query(query).mode(randomValueOtherThan(Mode.JDBC.toString(), BaseRestSqlTestCase::randomMode)).toString(),
                ContentType.APPLICATION_JSON
            )
        );
        assertOK(client().performRequest(csvRequest));
    }

    public void testQueryInTSV() throws IOException {
        index(
            "{\"name\":" + toJson("first") + ", \"number\" : 1 }",
            "{\"name\":" + toJson("second\t") + ", \"number\": 2 }",
            "{\"name\":" + toJson("\"third,\"") + ", \"number\": 3 }"
        );

        String expected = """
            name\tnumber
            first\t1
            second\\t\t2
            "third,"\t3
            """;

        String query = "SELECT * FROM " + indexPattern("test") + " ORDER BY number";
        Tuple<String, String> response = runSqlAsText(query(query), "text/tab-separated-values");
        assertEquals(expected, response.v1());
        response = runSqlAsTextWithFormat(query, "tsv");
        assertEquals(expected, response.v1());
    }

    public void testNextPageTSV() throws IOException {
        executeQueryWithNextPage("text/tab-separated-values", "text\tnumber\tsum\n", "%s\t%d\t%d\n");
    }

    public void testBinaryFieldFiltering() throws IOException {
        XContentBuilder createIndex = JsonXContent.contentBuilder().startObject();
        createIndex.startObject("mappings");
        {
            createIndex.startObject("properties");
            {
                createIndex.startObject("id").field("type", "long").endObject();
                createIndex.startObject("binary").field("type", "binary").field("doc_values", true).endObject();
            }
            createIndex.endObject();
        }
        createIndex.endObject().endObject();

        Request request = new Request("PUT", "/test_binary");
        request.setJsonEntity(Strings.toString(createIndex));
        assertEquals(200, provisioningClient().performRequest(request).getStatusLine().getStatusCode());

        long nonNullId = randomLong();
        long nullId = randomLong();
        StringBuilder bulk = new StringBuilder();
        bulk.append("{").append(toJson("index")).append(":{}}\n");
        bulk.append("{");
        {
            bulk.append(toJson("id")).append(":").append(nonNullId);
            bulk.append(",");
            bulk.append(toJson("binary")).append(":").append(toJson("U29tZSBiaW5hcnkgYmxvYg=="));
        }
        bulk.append("}\n");
        bulk.append("{").append(toJson("index")).append(":{}}\n");
        bulk.append("{");
        {
            bulk.append(toJson("id")).append(":").append(nullId);
        }
        bulk.append("}\n");

        request = new Request("PUT", "/test_binary/_bulk?refresh=true");
        request.setJsonEntity(bulk.toString());
        assertEquals(200, provisioningClient().performRequest(request).getStatusLine().getStatusCode());

        String mode = randomMode();
        Map<String, Object> expected = new HashMap<>();
        expected.put("columns", singletonList(columnInfo(mode, "id", "long", JDBCType.BIGINT, 20)));
        expected.put("rows", singletonList(singletonList(nonNullId)));
        assertResponse(expected, runSql(mode, "SELECT id FROM " + indexPattern("test_binary") + " WHERE binary IS NOT NULL", false));

        expected.put("rows", singletonList(singletonList(nullId)));
        assertResponse(expected, runSql(mode, "SELECT id FROM " + indexPattern("test_binary") + " WHERE binary IS NULL", false));

        deleteIndexWithProvisioningClient("test_binary");
    }

    public void testPreventedUnsignedLongMaskedAccess() throws IOException {
        loadUnsignedLongTestData();
        Version version = VersionUtils.randomVersionBetween(random(), null, VersionUtils.getPreviousVersion(INTRODUCING_UNSIGNED_LONG));
        String query = query("SELECT unsigned_long::STRING FROM " + indexPattern("test")).version(version.toString()).toString();
        expectBadRequest(
            () -> runSql(new StringEntity(query, ContentType.APPLICATION_JSON), "", randomMode()),
            containsString("Cannot use field [unsigned_long] with unsupported type [UNSIGNED_LONG]")
        );
    }

    public void testTsdbMetrics() throws IOException {
        createTsdb1();
        createTsdb2();

        String mode = randomMode();
        // select all from tsdb1
        Map<String, Object> expected = new HashMap<>();
        expected.put(
            "columns",
            Arrays.asList(
                columnInfo(mode, "@timestamp", "datetime", JDBCType.DATE, 34),
                columnInfo(mode, "k8s.pod.ip", "ip", JDBCType.VARCHAR, 45),
                columnInfo(mode, "k8s.pod.name", "keyword", JDBCType.VARCHAR, 32766),
                columnInfo(mode, "k8s.pod.network.rx", "long", JDBCType.BIGINT, 20),
                columnInfo(mode, "k8s.pod.network.tx", "long", JDBCType.BIGINT, 20),
                columnInfo(mode, "k8s.pod.uid", "keyword", JDBCType.VARCHAR, 32766),
                columnInfo(mode, "metricset", "keyword", JDBCType.VARCHAR, 32766)
            )
        );
        expected.put(
            "rows",
            singletonList(
                Arrays.asList(
                    "2021-04-28T18:50:04.467Z",
                    "10.10.55.1",
                    "cat",
                    802133794,
                    2001818691,
                    "947e4ced-1786-4e53-9e0c-5c447e959507",
                    "pod"
                )
            )
        );
        assertResponse(expected, runSql(mode, "SELECT * FROM tsdb1", false));

        // select all from tsdb2
        expected = new HashMap<>();
        expected.put(
            "columns",
            Arrays.asList(
                columnInfo(mode, "@timestamp", "datetime", JDBCType.DATE, 34),
                columnInfo(mode, "dim", "keyword", JDBCType.VARCHAR, 32766),
                columnInfo(mode, "k8s.pod.ip", "ip", JDBCType.VARCHAR, 45),
                columnInfo(mode, "k8s.pod.network.tx", "long", JDBCType.BIGINT, 20)
            )
        );
        expected.put("rows", singletonList(Arrays.asList("2021-04-28T18:50:04.467Z", "A", null, null)));
        assertResponse(expected, runSql(mode, "SELECT * FROM tsdb2", false));

        // select all from both indices
        expected = new HashMap<>();
        expected.put(
            "columns",
            Arrays.asList(
                columnInfo(mode, "@timestamp", "datetime", JDBCType.DATE, 34),
                columnInfo(mode, "dim", "keyword", JDBCType.VARCHAR, 32766),
                columnInfo(mode, "k8s.pod.ip", "ip", JDBCType.VARCHAR, 45),
                columnInfo(mode, "k8s.pod.name", "keyword", JDBCType.VARCHAR, 32766),
                columnInfo(mode, "k8s.pod.network.rx", "long", JDBCType.BIGINT, 20),
                columnInfo(mode, "k8s.pod.network.tx", "long", JDBCType.BIGINT, 20),
                columnInfo(mode, "k8s.pod.uid", "keyword", JDBCType.VARCHAR, 32766),
                columnInfo(mode, "metricset", "keyword", JDBCType.VARCHAR, 32766)
            )
        );
        expected.put(
            "rows",
            Arrays.asList(
                Arrays.asList(
                    "2021-04-28T18:50:04.467Z",
                    null,
                    "10.10.55.1",
                    "cat",
                    802133794,
                    2001818691,
                    "947e4ced-1786-4e53-9e0c-5c447e959507",
                    "pod"
                ),
                Arrays.asList("2021-04-28T18:50:04.467Z", "A", null, null, null, null, null, null)
            )
        );
        assertResponse(expected, runSql(mode, "SELECT * FROM \\\"tsdb*\\\"", false));

        // select the column that is both mapped as counter and as long
        expected = new HashMap<>();
        expected.put("columns", Arrays.asList(columnInfo(mode, "k8s.pod.network.tx", "long", JDBCType.BIGINT, 20)));
        expected.put("rows", Arrays.asList(singletonList(2001818691), singletonList(null)));
        assertResponse(expected, runSql(mode, "SELECT k8s.pod.network.tx FROM \\\"tsdb*\\\"", false));

        deleteIndex(client(), "tsdb1");
        deleteIndex(client(), "tsdb2");
    }

    private void createTsdb2() throws IOException {
        Request request = new Request("PUT", "/tsdb2");
        request.setJsonEntity("""
            {
                  "settings": {
                      "index": {
                          "mode": "time_series",
                          "routing_path": ["dim"],
                          "time_series": {
                              "start_time": "2021-04-28T00:00:00Z",
                              "end_time": "2021-04-29T00:00:00Z"
                          }
                      }
                  },
                  "mappings": {
                      "properties": {
                          "@timestamp": {
                              "type": "date"
                          },
                          "dim": {
                              "type": "keyword",
                              "time_series_dimension": "true"
                          },
                          "agg_metric": {
                              "type": "aggregate_metric_double",
                              "metrics": ["max"],
                              "default_metric": "max"
                          },
                          "k8s": {
                              "properties": {
                                  "pod": {
                                      "properties": {
                                          "ip": {
                                              "type": "ip"
                                          },
                                          "network": {
                                              "properties": {
                                                  "tx": {
                                                      "type": "long"
                                                  }
                                              }
                                          }
                                      }
                                  }
                              }
                          }
                      }
                  }
              }""");
        assertOK(client().performRequest(request));

        request = new Request("POST", "/tsdb2/_doc");
        request.addParameter("refresh", "true");
        request.setJsonEntity("{\"@timestamp\": \"2021-04-28T18:50:04.467Z\", \"dim\": \"A\", \"agg_metric\": {\"max\": 10}}");
        client().performRequest(request);
    }

    private void createTsdb1() throws IOException {
        Request request = new Request("PUT", "/tsdb1");
        request.setJsonEntity("""
            {
                 "settings": {
                     "index": {
                         "mode": "time_series",
                         "routing_path": [
                             "metricset",
                             "k8s.pod.uid"
                         ],
                         "time_series": {
                             "start_time": "2021-04-28T00:00:00Z",
                             "end_time": "2021-04-29T00:00:00Z"
                         }
                     }
                 },
                 "mappings": {
                     "properties": {
                         "@timestamp": {
                             "type": "date"
                         },
                         "metricset": {
                             "type": "keyword",
                             "time_series_dimension": true
                         },
                         "k8s": {
                             "properties": {
                                 "pod": {
                                     "properties": {
                                         "uid": {
                                             "type": "keyword",
                                             "time_series_dimension": true
                                         },
                                         "name": {
                                             "type": "keyword"
                                         },
                                         "ip": {
                                             "type": "ip"
                                         },
                                         "network": {
                                             "properties": {
                                                 "tx": {
                                                     "type": "long",
                                                     "time_series_metric": "counter"
                                                 },
                                                 "rx": {
                                                     "type": "long",
                                                     "time_series_metric": "counter"
                                                 }
                                             }
                                         }
                                     }
                                 }
                             }
                         }
                     }
                 }
             }""");
        assertOK(client().performRequest(request));

        request = new Request("POST", "/tsdb1/_doc");
        request.addParameter("refresh", "true");
        request.setJsonEntity(
            "{\"@timestamp\": \"2021-04-28T18:50:04.467Z\", \"metricset\": \"pod\","
                + "\"k8s\": {\"pod\": {\"name\": \"cat\", \"uid\":\"947e4ced-1786-4e53-9e0c-5c447e959507\", \"ip\": \"10.10.55.1\","
                + "\"network\": {\"tx\": 2001818691, \"rx\": 802133794}}}}"
        );
        client().performRequest(request);
    }

    private void executeQueryWithNextPage(String format, String expectedHeader, String expectedLineFormat) throws IOException {
        int size = 20;
        String[] docs = new String[size];
        for (int i = 0; i < size; i++) {
            docs[i] = org.elasticsearch.core.Strings.format("""
                {"text":"text%s", "number":%s}
                """, i, i);
        }
        index(docs);

        String optionalGroupBy = randomFrom("", " GROUP BY text, number, sum");

        RequestObjectBuilder request = query(
            "SELECT text, number, number + 5 AS sum FROM " + indexPattern("test") + optionalGroupBy + " ORDER BY number"
        ).fetchSize(2);

        String cursor = null;
        for (int i = 0; i <= size; i += 2) {
            Tuple<String, String> response;
            if (i == 0) {
                response = runSqlAsText(request, format);
            } else {
                response = runSqlAsText(cursor(cursor), format);
            }

            StringBuilder expected = new StringBuilder();
            if (i == 0) {
                expected.append(expectedHeader);
                if (format.equals("text/plain")) {
                    expected.append("---------------+---------------+---------------\n");
                }
            }

            cursor = response.v2();
            if (i < 20) {
                expected.append(String.format(Locale.ROOT, expectedLineFormat, "text" + i, i, i + 5));
                expected.append(String.format(Locale.ROOT, expectedLineFormat, "text" + (i + 1), i + 1, i + 6));
                assertEquals(expected.toString(), response.v1());
                assertNotNull(cursor);
            } else {
                assertNull(cursor);
            }
        }

        assertEquals(0, getNumberOfSearchContexts(provisioningClient(), "test"));
    }

    private void bulkLoadTestData(int count) throws IOException {
        Request request = new Request("POST", "/test/_bulk");
        request.addParameter("refresh", "true");
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < count; i++) {
            bulk.append(org.elasticsearch.core.Strings.format("""
                {"index":{"_id":"%s"}}
                {"text":"text%s", "number":%s}
                """, i, i, i));
        }
        request.setJsonEntity(bulk.toString());
        provisioningClient().performRequest(request);
    }

    private void loadUnsignedLongTestData() throws IOException {
        Request request = new Request("PUT", "/test");
        request.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "unsigned_long": {
                    "type": "unsigned_long"
                  }
                }
              }
            }""");
        provisioningClient().performRequest(request);
        index("{\"unsigned_long\": 18446744073709551615}");
    }

    private static String responseBody(Response response) throws IOException {
        return Streams.copyToString(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8));
    }

    /**
     * Run SQL as text using the {@code format} parameter to specify the format
     * rather than an {@code Accept} header.
     */
    private static Tuple<String, String> runSqlAsTextWithFormat(String sql, String format) throws IOException {
        Request request = new Request("POST", SQL_QUERY_REST_ENDPOINT);
        request.addParameter("error_trace", "true");
        request.addParameter("format", format);
        request.setJsonEntity(query(sql).toString());

        Response response = client().performRequest(request);
        return new Tuple<>(responseBody(response), response.getHeader("Cursor"));
    }

    public static void assertResponse(Map<String, Object> expected, Map<String, Object> actual) {
        if (false == expected.equals(actual)) {
            NotEqualMessageBuilder message = new NotEqualMessageBuilder();
            message.compareMaps(actual, expected);
            fail("Response does not match:\n" + message.toString());
        }
    }

    //
    // Async text formatting tests
    //

    public void testBasicAsyncWait() throws IOException {
        RequestObjectBuilder builder = query("SELECT 1").waitForCompletionTimeout("1d") // wait "forever"
            .keepOnCompletion(false);

        String mode = randomMode();

        Map<String, Object> expected = new HashMap<>();
        expected.put(IS_PARTIAL_NAME, false);
        expected.put(IS_RUNNING_NAME, false);
        expected.put(COLUMNS_NAME, singletonList(columnInfo(mode, "1", "integer", JDBCType.INTEGER, 11)));
        expected.put(ROWS_NAME, singletonList(singletonList(1)));
        assertAsyncResponse(expected, runSql(builder, mode));
    }

    public void testAsyncTextWait() throws IOException {
        RequestObjectBuilder builder = query("SELECT 1").waitForCompletionTimeout("1d").keepOnCompletion(false);

        Map<String, String> contentMap = Map.of(
            "txt",
            "       1       \n---------------\n1              \n",
            "csv",
            "1\r\n1\r\n",
            "tsv",
            "1\n1\n"
        );

        for (var format : contentMap.entrySet()) {
            Response response = runSqlAsTextWithFormat(builder, format.getKey());

            assertEquals(format.getValue(), responseBody(response));

            assertTrue(hasText(response.getHeader(HEADER_NAME_ASYNC_ID)));
            assertEquals("false", response.getHeader(HEADER_NAME_ASYNC_PARTIAL));
            assertEquals("false", response.getHeader(HEADER_NAME_ASYNC_RUNNING));
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/80089")
    public void testAsyncTextPaginated() throws IOException, InterruptedException {
        final Map<String, String> acceptMap = Map.of("txt", "text/plain", "csv", "text/csv", "tsv", "text/tab-separated-values");
        final int fetchSize = randomIntBetween(1, 10);
        final int fetchCount = randomIntBetween(1, 9);
        bulkLoadTestData(fetchSize * fetchCount); // NB: product needs to stay below 100, for txt format tests

        String format = randomFrom(acceptMap.keySet());
        String mode = randomMode();
        String cursor = null;
        for (int i = 0; i <= fetchCount; i++) { // the last iteration (the equality in `<=`) checks on no-cursor & no-results
            // start the query
            RequestObjectBuilder builder = (hasText(cursor) ? cursor(cursor) : query("SELECT text, number FROM " + indexPattern("test")))
                .fetchSize(fetchSize)
                .waitForCompletionTimeout("0d") // don't wait at all
                .keepOnCompletion(true)
                .keepAlive("1d") // keep "forever"
                .mode(mode)
                .binaryFormat(false); // prevent JDBC mode to (ignore `format` and) enforce CBOR
            Response response = runSqlAsTextWithFormat(builder, format);

            Character csvDelimiter = ',';

            assertEquals(200, response.getStatusLine().getStatusCode());
            assertTrue(response.getHeader(HEADER_NAME_ASYNC_PARTIAL).equals(response.getHeader(HEADER_NAME_ASYNC_RUNNING)));
            String asyncId = response.getHeader(HEADER_NAME_ASYNC_ID);
            assertTrue(hasText(asyncId));

            // it happens though rarely that the whole response comes through despite the given 0-wait
            if (response.getHeader(HEADER_NAME_ASYNC_PARTIAL).equals("true")) {

                // potentially wait for it to complete
                boolean pollForCompletion = randomBoolean();
                if (pollForCompletion) {
                    Request request = new Request("GET", SQL_ASYNC_STATUS_REST_ENDPOINT + asyncId);
                    Map<String, Object> asyncStatus = null;
                    long millis = 1;
                    for (boolean isRunning = true; isRunning; Thread.sleep(millis *= 2)) {
                        asyncStatus = toMap(client().performRequest(request), null);
                        isRunning = (boolean) asyncStatus.get(IS_RUNNING_NAME);
                    }
                    assertEquals(200, (int) asyncStatus.get("completion_status"));
                    assertEquals(asyncStatus.get(IS_RUNNING_NAME), asyncStatus.get(IS_PARTIAL_NAME));
                    assertEquals(asyncId, asyncStatus.get(ID_NAME));
                }

                // fetch the results (potentially waiting now to complete)
                Request request = new Request("GET", SQL_ASYNC_REST_ENDPOINT + asyncId);
                if (pollForCompletion == false) {
                    request.addParameter(WAIT_FOR_COMPLETION_TIMEOUT_NAME, "1d");
                }
                if (randomBoolean()) {
                    request.addParameter(URL_PARAM_FORMAT, format);
                    if (format.equals("csv")) {
                        csvDelimiter = ';';
                        request.addParameter(URL_PARAM_DELIMITER, URLEncoder.encode(String.valueOf(csvDelimiter), StandardCharsets.UTF_8));
                    }
                } else {
                    request.setOptions(request.getOptions().toBuilder().addHeader("Accept", acceptMap.get(format)));
                }
                response = client().performRequest(request);

                assertEquals(200, response.getStatusLine().getStatusCode());
                assertEquals(asyncId, response.getHeader(HEADER_NAME_ASYNC_ID));
                assertEquals("false", response.getHeader(HEADER_NAME_ASYNC_PARTIAL));
                assertEquals("false", response.getHeader(HEADER_NAME_ASYNC_RUNNING));
            }

            cursor = response.getHeader(HEADER_NAME_CURSOR);
            String body = responseBody(response);
            if (i == fetchCount) {
                assertNull(cursor);
                assertFalse(hasText(body));
            } else {
                String expected = expectedTextBody(format, fetchSize, i, csvDelimiter);
                assertEquals(expected, body);

                if (hasText(cursor) == false) { // depending on index and fetch size, the last page might or not have a cursor
                    assertEquals(i, fetchCount - 1);
                    i++; // end the loop after deleting the async resources
                }
            }

            // delete the query results
            Request request = new Request("DELETE", SQL_ASYNC_DELETE_REST_ENDPOINT + asyncId);
            Map<String, Object> deleteStatus = toMap(client().performRequest(request), null);
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertTrue((boolean) deleteStatus.get("acknowledged"));
        }
    }

    public void testCompressCursor() throws IOException {
        String doc = IntStream.range(0, 1000)
            .mapToObj(i -> String.format(Locale.ROOT, "\"field%d\": %d", i, i))
            .collect(Collectors.joining(","));
        index("{" + doc + "}");

        String mode = randomMode();
        Map<String, Object> resp = toMap(runSql(query("SHOW COLUMNS FROM " + indexPattern("test")).fetchSize(1).mode(mode)), mode);

        // without compression, the cursor is at least <avg. fieldname length> * 1000 bytes (in fact it is ~35kb)
        assertThat(resp.get("cursor").toString().length(), lessThan(5000));
    }

    public void testFetchAllPagesSearchHitCursorTxt() throws IOException {
        testFetchAllPagesSearchHitCursor("text/plain");
    }

    public void testFetchAllPagesSearchHitCursorCsv() throws IOException {
        testFetchAllPagesSearchHitCursor("text/csv");
    }

    public void testFetchAllPagesSearchHitCursorTsv() throws IOException {
        testFetchAllPagesSearchHitCursor("text/tab-separated-values");
    }

    public void testFetchAllPagesSearchHitCursor(String format) throws IOException {
        int size = randomIntBetween(4, 20);
        int pageSize = randomIntBetween(1, size + 1);

        List<String> texts = IntStream.range(0, size).mapToObj(i -> String.format(Locale.ROOT, "text%02d", i)).toList();
        index(texts.stream().map(t -> "{\"field\": \"" + t + "\"}").toArray(String[]::new));

        testFetchAllPages(format, "SELECT field FROM " + indexPattern("test") + " ORDER BY field", texts, pageSize, size % pageSize == 0);
    }

    /**
     * Special case of `testFetchAllPagesSearchHitCursorTxt` covering https://github.com/elastic/elasticsearch/issues/83788.
     */
    public void testFetchAllPagesSearchHitCursorWithNonFullLastPageTxt() throws IOException {
        int size = randomIntBetween(4, 20);
        int pageSize = size / 2 + 1;

        List<String> texts = IntStream.range(0, size).mapToObj(i -> String.format(Locale.ROOT, "text%02d", i)).toList();
        index(texts.stream().map(t -> "{\"field\": \"" + t + "\"}").toArray(String[]::new));

        testFetchAllPages("text/plain", "SELECT field FROM " + indexPattern("test") + " ORDER BY field", texts, pageSize, false);
    }

    public void testFetchAllPagesCompositeAggCursorTxt() throws IOException {
        testFetchAllPagesCompositeAggCursor("text/plain");
    }

    public void testFetchAllPagesCompositeAggCursorCsv() throws IOException {
        testFetchAllPagesCompositeAggCursor("text/csv");
    }

    public void testFetchAllPagesCompositeAggCursorTsv() throws IOException {
        testFetchAllPagesCompositeAggCursor("text/tab-separated-values");
    }

    public void testFetchAllPagesCompositeAggCursor(String format) throws IOException {
        int size = randomIntBetween(4, 20);
        int pageSize = randomIntBetween(1, size + 1);

        List<String> texts = IntStream.range(0, size).mapToObj(i -> String.format(Locale.ROOT, "text%02d", i)).toList();
        index(texts.stream().map(t -> "{\"field\": \"" + t + "\"}").toArray(String[]::new));

        testFetchAllPages(
            format,
            "SELECT field FROM " + indexPattern("test") + " GROUP BY field ORDER BY field",
            texts,
            pageSize,
            size % pageSize == 0
        );
    }

    public void testFetchAllPagesCompositeAggCursorWithFilterOnAggregateTxt() throws IOException {
        testFetchAllPagesCompositeAggCursorWithFilterOnAggregate("text/plain");
    }

    public void testFetchAllPagesCompositeAggCursorWithFilterOnAggregateCsv() throws IOException {
        testFetchAllPagesCompositeAggCursorWithFilterOnAggregate("text/csv");
    }

    public void testFetchAllPagesCompositeAggCursorWithFilterOnAggregateTsv() throws IOException {
        testFetchAllPagesCompositeAggCursorWithFilterOnAggregate("text/tab-separated-values");
    }

    public void testFetchAllPagesCompositeAggCursorWithFilterOnAggregate(String format) throws IOException {
        int size = randomIntBetween(4, 20);
        int pageSize = randomIntBetween(1, size + 1);

        List<String> texts = IntStream.range(0, size).mapToObj(i -> String.format(Locale.ROOT, "text%02d", i)).toList();
        index(texts.stream().map(t -> "{\"field\": \"" + t + "\"}").toArray(String[]::new));

        testFetchAllPages(
            format,
            "SELECT field, COUNT(*) c FROM " + indexPattern("test") + " GROUP BY field HAVING c = 1 ORDER BY field",
            texts,
            pageSize,
            true
        );
    }

    public void testFetchAllPagesListCursorTxt() throws IOException {
        testFetchAllPagesListCursor("text/plain");
    }

    public void testFetchAllPagesListCursorCsv() throws IOException {
        testFetchAllPagesListCursor("text/csv");
    }

    public void testFetchAllPagesListCursorTsv() throws IOException {
        testFetchAllPagesListCursor("text/tab-separated-values");
    }

    public void testFetchAllPagesListCursor(String format) throws IOException {
        int size = randomIntBetween(4, 20);
        int pageSize = randomIntBetween(1, size + 1);

        List<String> fields = IntStream.range(0, size).mapToObj(i -> String.format(Locale.ROOT, "text%02d", i)).toList();
        index(fields.stream().map(f -> "{\"" + f + "\": 1}").toArray(String[]::new));

        testFetchAllPages(format, "SHOW COLUMNS FROM " + indexPattern("test"), fields, pageSize, false);
    }

    /**
     * Generic paging test that ensures that
     * 1. All expected documents are fetched when scrolling through all pages
     * 2. There are at most `expectedValues.size() / pageSize + 1` pages (the last one might or might not be empty)
     * 3. Optionally: That the last page is not empty.
     */
    private static void testFetchAllPages(String format, String query, List<String> expectedValues, int pageSize, boolean emptyLastPage)
        throws IOException {
        int remainingPages = expectedValues.size() / pageSize + 1;

        Tuple<String, String> response = runSqlAsText(query(query).fetchSize(pageSize), format);
        StringBuilder allResults = new StringBuilder(response.v1());

        while (response.v2() != null && remainingPages > 0) {
            response = runSqlAsText(cursor(response.v2()), format);
            allResults.append("\n").append(response.v1());

            assertThat(response.v1().split("\n").length, lessThanOrEqualTo(pageSize));

            remainingPages--;
        }

        if (emptyLastPage) {
            assertTrue("Expected empty last page but got " + response.v1(), Strings.isNullOrEmpty(response.v1()));
        } else {
            assertFalse("Expected non-empty last page but got " + response.v1(), Strings.isNullOrEmpty(response.v1()));
        }

        assertNull(response.v2());

        String results = allResults.toString();
        for (String v : expectedValues) {
            assertThat(results, containsString(v));
        }
    }

    public void testDataStreamInShowTables() throws IOException {
        // TODO: list aliases and data streams in CCS setups
        assumeTrue("Data streams in remote clusters are not available", provisioningClient().equals(client()));
        expectDataStreamInShowTables("test-datastream", "SHOW TABLES");
    }

    public void testDataStreamInShowTablesFiltered() throws IOException {
        assumeTrue("Data streams in remote clusters are not available", provisioningClient().equals(client()));
        String dataStreamName = "test-datastream";
        expectDataStreamInShowTables(dataStreamName, "SHOW TABLES \\\"" + dataStreamName + "*\\\"");
    }

    private void expectDataStreamInShowTables(String dataStreamName, String sql) throws IOException {
        try {
            createDataStream(dataStreamName);

            String mode = randomMode();
            Map<String, Object> answer = toMap(runSql(query(sql).mode(mode)), mode);
            List<String> expected = Arrays.asList("javaRestTest", dataStreamName, "VIEW", "ALIAS");
            @SuppressWarnings("unchecked")
            List<List<String>> rows = (List<List<String>>) (answer.get("rows"));
            assertTrue(rows.contains(expected));
        } finally {
            deleteDataStream(dataStreamName);
        }
    }

    public void testQueryDataStream() throws IOException {
        assumeTrue("Data streams in remote clusters are not available", provisioningClient().equals(client()));
        String dataStreamName = "test-datastream";
        try {
            createDataStream(dataStreamName);
            indexWithIndexName(dataStreamName, "{\"@timestamp\": \"2001-01-01T01:01:01Z\", \"foo\":\"bar\"}");

            String mode = randomMode();
            Map<String, Object> expected = new HashMap<>();
            expected.put("columns", singletonList(columnInfo(mode, "foo", "text", JDBCType.VARCHAR, Integer.MAX_VALUE)));
            expected.put("rows", singletonList(singletonList("bar")));
            assertResponse(expected, runSql(mode, "SELECT foo FROM \\\"" + dataStreamName + "\\\"", false));
        } finally {
            deleteDataStream(dataStreamName);
        }

    }

    public void testAllowPartialSearchResults() throws IOException {
        final String mappingTemplate = """
            {
              "mappings": {
                "properties": {
                  "bool": {
                    "type": "boolean",
                      "index": %s,
                      "doc_values": %s
                  }
                }
              }
            }""";

        // must match org.elasticsearch.xpack.sql.execution.search.Querier.BaseActionListener.MAX_WARNING_HEADERS
        final int maxWarningHeaders = 20;
        final int extraBadShards = randomIntBetween(1, 5);
        final int okShards = randomIntBetween(1, 5);

        final String suppressMessage = " remaining shard failure" + (extraBadShards > 1 ? "s" : "") + " suppressed";
        final String reason = "Cannot search on field [bool] since it is not indexed nor has doc values";
        final String warnMessage = "org.elasticsearch.index.query.QueryShardException: failed to create query: " + reason;

        for (int i = 0; i < maxWarningHeaders - 1 + okShards + extraBadShards; i++) {
            Request request = new Request("PUT", "/test" + i);
            boolean indexWithDocVals = i < okShards;
            request.setJsonEntity(String.format(Locale.ROOT, mappingTemplate, indexWithDocVals, indexWithDocVals));
            assertOK(provisioningClient().performRequest(request));
            indexWithIndexName("test" + i, "{\"bool\": " + randomBoolean() + "}");
        }

        Request request = new Request("POST", SQL_QUERY_REST_ENDPOINT);
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        RequestObjectBuilder builder = query("SELECT * FROM " + indexPattern("\\\"test*\\\"") + " WHERE bool=" + randomBoolean());

        // first, the negative test: allowPartialSearchResults=false leads to an error
        request.setEntity(new StringEntity(builder.toString(), ContentType.APPLICATION_JSON));
        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertTrue(exception.getMessage().contains(reason));

        // then, allowPartialSearchResults=true will generate a response with shard failures warnings
        request.setEntity(new StringEntity(builder.allowPartialSearchResults(true).toString(), ContentType.APPLICATION_JSON));
        Response response = client().performRequest(request);
        assertOK(response);

        int failedShards = 0;
        boolean hasSupressMessage = false;
        for (Header header : response.getHeaders()) {
            if (header.getName().toLowerCase(Locale.ROOT).equals("warning")) {
                String headerVal = header.getValue();
                if (headerVal.contains(warnMessage)) {
                    failedShards++;
                } else if (headerVal.contains(extraBadShards + suppressMessage)) {
                    hasSupressMessage = true;
                }
            }
        }
        assertEquals(maxWarningHeaders - 1, failedShards);
        assertTrue(hasSupressMessage);
    }

    static Map<String, Object> runSql(RequestObjectBuilder builder, String mode) throws IOException {
        return toMap(runSql(builder.mode(mode)), mode);
    }

    static Response runSql(RequestObjectBuilder builder) throws IOException {
        return runSqlAsTextWithFormat(builder, null);
    }

    static Response runSqlAsTextWithFormat(RequestObjectBuilder builder, @Nullable String format) throws IOException {
        Request request = new Request("POST", SQL_QUERY_REST_ENDPOINT);
        request.addParameter("error_trace", "true");   // Helps with debugging in case something crazy happens on the server.
        request.addParameter("pretty", "true");        // Improves error reporting readability
        if (format != null) {
            request.addParameter(URL_PARAM_FORMAT, format);        // Improves error reporting readability
        }
        request.setEntity(new StringEntity(builder.toString(), ContentType.APPLICATION_JSON));
        return client().performRequest(request);

    }

    static void assertAsyncResponse(Map<String, Object> expected, Map<String, Object> actual) {
        String actualId = (String) actual.get("id");
        assertTrue("async ID missing in response", hasText(actualId));
        expected.put("id", actualId);
        assertResponse(expected, actual);
    }

    private static String expectedTextBody(String format, int fetchSize, int count, Character csvDelimiter) {
        StringBuilder sb = new StringBuilder();
        if (count == 0) { // add the header
            switch (format) {
                case "txt":
                    sb.append("     text      |    number     \n");
                    sb.append("---------------+---------------\n");
                    break;
                case "csv":
                    sb.append("text").append(csvDelimiter).append("number\r\n");
                    break;
                case "tsv":
                    sb.append("text\tnumber\n");
                    break;
                default:
                    assert false : "unexpected format type [" + format + "]";
            }
        }
        for (int i = 0; i < fetchSize; i++) {
            int val = fetchSize * count + i;
            sb.append("text").append(val);
            switch (format) {
                case "txt" -> sb.append(val < 10 ? " " : StringUtils.EMPTY).append("         |");
                case "csv" -> sb.append(csvDelimiter);
                case "tsv" -> sb.append('\t');
            }
            sb.append(val);
            if (format.equals("txt")) {
                sb.append("             ").append(val < 10 ? " " : StringUtils.EMPTY);
            }
            sb.append(format.equals("csv") ? "\r\n" : "\n");
        }
        return sb.toString();
    }
}
