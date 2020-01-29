/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.rest;

import com.fasterxml.jackson.core.io.JsonStringEncoder;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.NotEqualMessageBuilder;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.StringUtils;
import org.elasticsearch.xpack.sql.qa.ErrorsTestCase;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static org.hamcrest.Matchers.containsString;

/**
 * Integration test for the rest sql action. The one that speaks json directly to a
 * user rather than to the JDBC driver or CLI.
 */
public abstract class RestSqlTestCase extends BaseRestSqlTestCase implements ErrorsTestCase {
    
    public static String SQL_QUERY_REST_ENDPOINT = org.elasticsearch.xpack.sql.proto.Protocol.SQL_QUERY_REST_ENDPOINT;
    private static String SQL_TRANSLATE_REST_ENDPOINT = org.elasticsearch.xpack.sql.proto.Protocol.SQL_TRANSLATE_REST_ENDPOINT;
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

    public void testBasicQuery() throws IOException {
        index("{\"test\":\"test\"}",
            "{\"test\":\"test\"}");

        Map<String, Object> expected = new HashMap<>();
        String mode = randomMode();
        boolean columnar = randomBoolean();
        
        expected.put("columns", singletonList(columnInfo(mode, "test", "text", JDBCType.VARCHAR, Integer.MAX_VALUE)));
        if (columnar) {
            expected.put("values", singletonList(Arrays.asList("test", "test")));
        } else {
            expected.put("rows", Arrays.asList(singletonList("test"), singletonList("test")));
        }
        assertResponse(expected, runSql(mode, "SELECT * FROM test", columnar));
    }

    public void testNextPage() throws IOException {
        Request request = new Request("POST", "/test/_bulk");
        request.addParameter("refresh", "true");
        String mode = randomMode();
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            bulk.append("{\"index\":{\"_id\":\"" + i + "\"}}\n");
            bulk.append("{\"text\":\"text" + i + "\", \"number\":" + i + "}\n");
        }
        request.setJsonEntity(bulk.toString());
        client().performRequest(request);
        
        boolean columnar = randomBoolean();
        String sqlRequest =
                  "{\"query\":\""
                + "   SELECT text, number, SQRT(number) AS s, SCORE()"
                + "     FROM test"
                + " ORDER BY number, SCORE()\", "
                + "\"mode\":\"" + mode + "\", "
            + "\"fetch_size\":2" + columnarParameter(columnar) + "}";

        Number value = xContentDependentFloatingNumberValue(mode, 1f);
        String cursor = null;
        for (int i = 0; i < 20; i += 2) {
            Map<String, Object> response;
            if (i == 0) {
                response = runSql(new StringEntity(sqlRequest, ContentType.APPLICATION_JSON), "", mode);
            } else {
                columnar = randomBoolean();
                response = runSql(new StringEntity("{\"cursor\":\"" + cursor + "\"" + mode(mode) + columnarParameter(columnar) + "}",
                        ContentType.APPLICATION_JSON), StringUtils.EMPTY, mode);
            }

            Map<String, Object> expected = new HashMap<>();
            if (i == 0) {
                expected.put("columns", Arrays.asList(
                        columnInfo(mode, "text", "text", JDBCType.VARCHAR, Integer.MAX_VALUE),
                        columnInfo(mode, "number", "long", JDBCType.BIGINT, 20),
                        columnInfo(mode, "s", "double", JDBCType.DOUBLE, 25),
                        columnInfo(mode, "SCORE()", "float", JDBCType.REAL, 15)));
            }
            
            if (columnar) {
                expected.put("values", Arrays.asList(
                        Arrays.asList("text" + i, "text" + (i + 1)),
                        Arrays.asList(i, i + 1),
                        Arrays.asList(Math.sqrt(i), Math.sqrt(i + 1)),
                        Arrays.asList(value, value)));
            } else {
                expected.put("rows", Arrays.asList(
                        Arrays.asList("text" + i, i, Math.sqrt(i), value),
                        Arrays.asList("text" + (i + 1), i + 1, Math.sqrt(i + 1), value)));
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
        assertResponse(expected, runSql(new StringEntity("{ \"cursor\":\"" + cursor + "\"" + mode(mode) + columnarParameter(columnar) + "}",
                ContentType.APPLICATION_JSON), StringUtils.EMPTY, mode));
    }

    @AwaitsFix(bugUrl = "Unclear status, https://github.com/elastic/x-pack-elasticsearch/issues/2074")
    public void testTimeZone() throws IOException {
        String mode = randomMode();
        boolean columnar = randomBoolean();
        index("{\"test\":\"2017-07-27 00:00:00\"}",
            "{\"test\":\"2017-07-27 01:00:00\"}");

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
        assertResponse(expected, runSql(mode, "SELECT DAY_OF_YEAR(test), COUNT(*) FROM test", columnar));
    }

    public void testScoreWithFieldNamedScore() throws IOException {
        Request request = new Request("POST", "/test/_bulk");
        request.addParameter("refresh", "true");
        String mode = randomMode();
        StringBuilder bulk = new StringBuilder();
        bulk.append("{\"index\":{\"_id\":\"1\"}}\n");
        bulk.append("{\"name\":\"test\", \"score\":10}\n");
        request.setJsonEntity(bulk.toString());
        client().performRequest(request);

        Map<String, Object> expected = new HashMap<>();
        boolean columnar = randomBoolean();
        expected.put("columns", Arrays.asList(
            columnInfo(mode, "name", "text", JDBCType.VARCHAR, Integer.MAX_VALUE),
            columnInfo(mode, "score", "long", JDBCType.BIGINT, 20),
            columnInfo(mode, "SCORE()", "float", JDBCType.REAL, 15)));
        Number value = xContentDependentFloatingNumberValue(mode, 1f);
        if (columnar) {
            expected.put("values", Arrays.asList(singletonList("test"), singletonList(10), singletonList(value)));
        } else {
            expected.put("rows", singletonList(Arrays.asList("test", 10, value)));
        }
        
        assertResponse(expected, runSql(mode, "SELECT *, SCORE() FROM test ORDER BY SCORE()", columnar));
        assertResponse(expected, runSql(mode, "SELECT name, \\\"score\\\", SCORE() FROM test ORDER BY SCORE()", columnar));
    }

    public void testSelectWithJoinFails() throws Exception {
        // Normal join not supported
        expectBadRequest(() -> runSql(randomMode(), "SELECT * FROM test JOIN other"),
            containsString("line 1:21: Queries with JOIN are not yet supported"));
        // Neither is a self join
        expectBadRequest(() -> runSql(randomMode(), "SELECT * FROM test JOIN test"),
            containsString("line 1:21: Queries with JOIN are not yet supported"));
        // Nor fancy stuff like CTEs
        expectBadRequest(() -> runSql(randomMode(),
            "    WITH evil"
            + "  AS (SELECT *"
            + "        FROM foo)"
            + "SELECT *"
            + "  FROM test"
            + "  JOIN evil"),
            containsString("line 1:67: Queries with JOIN are not yet supported"));
    }

    public void testSelectDistinctFails() throws Exception {
        index("{\"name\":\"test\"}");
        expectBadRequest(() -> runSql(randomMode(), "SELECT DISTINCT name FROM test"),
            containsString("line 1:8: SELECT DISTINCT is not yet supported"));
    }

    public void testSelectGroupByAllFails() throws Exception {
        index("{\"foo\":1}", "{\"foo\":2}");
        expectBadRequest(() -> runSql(randomMode(), "SELECT foo FROM test GROUP BY ALL foo"),
            containsString("line 1:32: GROUP BY ALL is not supported"));
    }

    public void testSelectWhereExistsFails() throws Exception {
        index("{\"foo\":1}", "{\"foo\":2}");
        expectBadRequest(() -> runSql(randomMode(), "SELECT foo FROM test WHERE EXISTS (SELECT * FROM test t WHERE t.foo = test.foo)",
                randomBoolean()), containsString("line 1:28: EXISTS is not yet supported"));
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
    public void testSelectFromIndexWithoutTypes() throws Exception {
        // Create an index without any types
        Request request = new Request("PUT", "/test");
        request.setJsonEntity("{}");
        client().performRequest(request);
        String mode = randomFrom("jdbc", "plain");
        expectBadRequest(() -> runSql(mode, "SELECT * FROM test"),
                // see https://github.com/elastic/elasticsearch/issues/34719
            //containsString("1:15: [test] doesn't have any types so it is incompatible with sql"));
            containsString("1:15: Unknown index [test]"));
    }

    @Override
    public void testSelectMissingField() throws IOException {
        index("{\"test\":\"test\"}");
        String mode = randomFrom("jdbc", "plain");
        expectBadRequest(() -> runSql(mode, "SELECT foo FROM test"), containsString("1:8: Unknown column [foo]"));
    }

    @Override
    public void testSelectMissingFunction() throws Exception {
        index("{\"foo\":1}");
        expectBadRequest(() -> runSql(randomMode(), "SELECT missing(foo) FROM test"),
                containsString("1:8: Unknown function [missing]"));
    }

    @Override
    public void testSelectProjectScoreInAggContext() throws Exception {
        index("{\"foo\":1}");
        expectBadRequest(() -> runSql(randomMode(),
            "     SELECT foo, SCORE(), COUNT(*)"
            + "     FROM test"
            + " GROUP BY foo"),
                containsString("Cannot use non-grouped column [SCORE()], expected [foo]"));
    }

    @Override
    public void testSelectOrderByScoreInAggContext() throws Exception {
        index("{\"foo\":1}");
        expectBadRequest(() -> runSql(randomMode(),
            "     SELECT foo, COUNT(*)"
            + "     FROM test"
            + " GROUP BY foo"
            + " ORDER BY SCORE()"),
                containsString("Cannot order by non-grouped column [SCORE()], expected [foo]"));
    }

    @Override
    public void testSelectGroupByScore() throws Exception {
        index("{\"foo\":1}");
        expectBadRequest(() -> runSql(randomMode(), "SELECT COUNT(*) FROM test GROUP BY SCORE()"),
                containsString("Cannot use [SCORE()] for grouping"));
    }

    @Override
    public void testSelectScoreSubField() throws Exception {
        index("{\"foo\":1}");
        expectBadRequest(() -> runSql(randomMode(), "SELECT SCORE().bar FROM test"),
            containsString("line 1:15: extraneous input '.' expecting {<EOF>, ','"));
    }

    @Override
    public void testSelectScoreInScalar() throws Exception {
        index("{\"foo\":1}");
        expectBadRequest(() -> runSql(randomMode(), "SELECT SIN(SCORE()) FROM test"),
            containsString("line 1:12: [SCORE()] cannot be an argument to a function"));
    }

    @Override
    public void testHardLimitForSortOnAggregate() throws Exception {
        index("{\"a\": 1, \"b\": 2}");
        expectBadRequest(() -> runSql(randomMode(), "SELECT max(a) max FROM test GROUP BY b ORDER BY max LIMIT 12000"),
            containsString("The maximum LIMIT for aggregate sorting is [10000], received [12000]"));
    }

    public void testUseColumnarForUnsupportedFormats() throws Exception {
        String format = randomFrom("txt", "csv", "tsv");
        index("{\"foo\":1}");
        
        Request request = new Request("POST", SQL_QUERY_REST_ENDPOINT);
        request.addParameter("error_trace", "true");
        request.addParameter("pretty", "true");
        request.addParameter("format", format);
        request.setEntity(new StringEntity("{\"columnar\":true,\"query\":\"SELECT * FROM test\""
                + mode(randomValueOtherThan("jdbc", () -> randomMode())) + "}",
                ContentType.APPLICATION_JSON));
        expectBadRequest(() -> {
                client().performRequest(request);
                return Collections.emptyMap();
            }, containsString("Invalid use of [columnar] argument: cannot be used in combination with txt, csv or tsv formats"));
    }
    
    public void testUseColumnarForTranslateRequest() throws IOException {
        index("{\"test\":\"test\"}", "{\"test\":\"test\"}");
        
        Request request = new Request("POST", SQL_TRANSLATE_REST_ENDPOINT);
        request.setEntity(new StringEntity("{\"columnar\":true,\"query\":\"SELECT * FROM test\"" + mode(randomMode()) + "}",
                ContentType.APPLICATION_JSON));
        expectBadRequest(() -> {
                client().performRequest(request);
                return Collections.emptyMap();
            }, containsString("unknown field [columnar]"));
    }

    public static void expectBadRequest(CheckedSupplier<Map<String, Object>, Exception> code, Matcher<String> errorMessageMatcher) {
        try {
            Map<String, Object> result = code.get();
            fail("expected ResponseException but got " + result);
        } catch (ResponseException e) {
            if (400 != e.getResponse().getStatusLine().getStatusCode()) {
                String body;
                try {
                    body = Streams.copyToString(new InputStreamReader(
                        e.getResponse().getEntity().getContent(), StandardCharsets.UTF_8));
                } catch (IOException bre) {
                    throw new RuntimeException("error reading body after remote sent bad status", bre);
                }
                fail("expected [400] response but get [" + e.getResponse().getStatusLine().getStatusCode() + "] with body:\n" +  body);
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
        return runSql(new StringEntity("{\"query\":\"" + sql + "\"" + mode(mode) + columnarParameter(columnar) + "}",
                ContentType.APPLICATION_JSON), suffix, mode);
    }
    
    protected Map<String, Object> runTranslateSql(String sql) throws IOException {
        return runSql(new StringEntity(sql, ContentType.APPLICATION_JSON), "/translate/", Mode.PLAIN.toString());
    }
    
    private String columnarParameter(boolean columnar) {
        if (columnar == false && randomBoolean()) {
            return "";
        } else {
            return ",\"columnar\":" + columnar;
        }
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
            expected = "{\n" +
                    "  \"columns\" : [\n" +
                    "    {\n" +
                    "      \"name\" : \"test1\",\n" +
                    "      \"type\" : \"text\"\n" +
                    "    }\n" +
                    "  ],\n" +
                    "  \"values\" : [\n" +
                    "    [\n" +
                    "      \"test1\",\n" +
                    "      \"test2\"\n" +
                    "    ]\n" +
                    "  ]\n" +
                    "}\n";
        } else {
            expected = "{\n" +
                    "  \"columns\" : [\n" +
                    "    {\n" +
                    "      \"name\" : \"test1\",\n" +
                    "      \"type\" : \"text\"\n" +
                    "    }\n" +
                    "  ],\n" +
                    "  \"rows\" : [\n" +
                    "    [\n" +
                    "      \"test1\"\n" +
                    "    ],\n" +
                    "    [\n" +
                    "      \"test2\"\n" +
                    "    ]\n" +
                    "  ]\n" +
                    "}\n";
        }
        executeAndAssertPrettyPrinting(expected, "true", columnar);
    }
    
    public void testPrettyPrintingDisabled() throws IOException {
        boolean columnar = randomBoolean();
        String expected = "";
        if (columnar) {
            expected = "{\"columns\":[{\"name\":\"test1\",\"type\":\"text\"}],\"values\":[[\"test1\",\"test2\"]]}";
        } else {
            expected = "{\"columns\":[{\"name\":\"test1\",\"type\":\"text\"}],\"rows\":[[\"test1\"],[\"test2\"]]}";
        }
        executeAndAssertPrettyPrinting(expected, randomFrom("false", null), columnar);
    }
    
    private void executeAndAssertPrettyPrinting(String expectedJson, String prettyParameter, boolean columnar)
            throws IOException {
        index("{\"test1\":\"test1\"}",
              "{\"test1\":\"test2\"}");

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
        request.setEntity(new StringEntity("{\"query\":\"SELECT * FROM test\"" + mode("plain") + columnarParameter(columnar) + "}",
                  ContentType.APPLICATION_JSON));
        
        Response response = client().performRequest(request);
        try (InputStream content = response.getEntity().getContent()) {
            String actualJson = new BytesArray(content.readAllBytes()).utf8ToString();
            assertEquals(expectedJson, actualJson);
        }
    }

    public void testBasicTranslateQuery() throws IOException {
        index("{\"test\":\"test\"}", "{\"test\":\"test\"}");
        
        Map<String, Object> response = runTranslateSql("{\"query\":\"SELECT * FROM test\"}");
        assertEquals(1000, response.get("size"));
        @SuppressWarnings("unchecked")
        Map<String, Object> source = (Map<String, Object>) response.get("_source");
        assertNotNull(source);
        assertEquals(emptyList(), source.get("excludes"));
        assertEquals(singletonList("test"), source.get("includes"));
    }

    public void testBasicQueryWithFilter() throws IOException {
        String mode = randomMode();
        index("{\"test\":\"foo\"}",
            "{\"test\":\"bar\"}");

        Map<String, Object> expected = new HashMap<>();
        expected.put("columns", singletonList(columnInfo(mode, "test", "text", JDBCType.VARCHAR, Integer.MAX_VALUE)));
        expected.put("rows", singletonList(singletonList("foo")));
        assertResponse(expected, runSql(new StringEntity("{\"query\":\"SELECT * FROM test\", " +
                "\"filter\":{\"match\": {\"test\": \"foo\"}}" + mode(mode) + "}",
                ContentType.APPLICATION_JSON), StringUtils.EMPTY, mode));
    }

    public void testBasicQueryWithParameters() throws IOException {
        String mode = randomMode();
        boolean columnar = randomBoolean();
        index("{\"test\":\"foo\"}",
                "{\"test\":\"bar\"}");

        Map<String, Object> expected = new HashMap<>();
        expected.put("columns", Arrays.asList(
                columnInfo(mode, "test", "text", JDBCType.VARCHAR, Integer.MAX_VALUE),
                columnInfo(mode, "param", "integer", JDBCType.INTEGER, 11)
        ));
        if (columnar) {
            expected.put("values", Arrays.asList(singletonList("foo"), singletonList(10)));
        } else {
            expected.put("rows", Arrays.asList(Arrays.asList("foo", 10)));
        }
        
        String params = mode.equals("jdbc") ? "{\"type\": \"integer\", \"value\": 10}, {\"type\": \"keyword\", \"value\": \"foo\"}" :
            "10, \"foo\"";
        assertResponse(expected, runSql(new StringEntity("{\"query\":\"SELECT test, ? param FROM test WHERE test = ?\", " +
                "\"params\":[" + params + "]"
                + mode(mode) + columnarParameter(columnar) + "}", ContentType.APPLICATION_JSON), StringUtils.EMPTY, mode));
    }

    public void testBasicTranslateQueryWithFilter() throws IOException {
        index("{\"test\":\"foo\"}",
            "{\"test\":\"bar\"}");

        Map<String, Object> response = runTranslateSql("{\"query\":\"SELECT * FROM test\", \"filter\":{\"match\": {\"test\": \"foo\"}}}");

        assertEquals(response.get("size"), 1000);
        @SuppressWarnings("unchecked")
        Map<String, Object> source = (Map<String, Object>) response.get("_source");
        assertNotNull(source);
        assertEquals(emptyList(), source.get("excludes"));
        assertEquals(singletonList("test"), source.get("includes"));

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
        index("{\"salary\":100}",
            "{\"age\":20}");

        Map<String, Object> response = runTranslateSql("{\"query\":\"SELECT avg(salary) FROM test GROUP BY abs(age) "
                + "HAVING avg(salary) > 50 LIMIT 10\"}");

        assertEquals(response.get("size"), 0);
        assertEquals(false, response.get("_source"));
        assertEquals("_none_", response.get("stored_fields"));

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
        Map<String, Object> sourcesListMap =
            (Map<String, Object>) ((Map<String, Object>) sources.get(0)).values().iterator().next();
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
        assertEquals("InternalSqlScriptUtils.abs(InternalSqlScriptUtils.docValue(doc,params.v0))", termsScript.get("source"));
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
        assertEquals("InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.gt(params.a0,params.v0))",
            filterScript.get("source"));
        assertEquals("painless", filterScript.get("lang"));
        @SuppressWarnings("unchecked")
        Map<String, Object> filterScriptParams = (Map<String, Object>) filterScript.get("params");
        assertEquals(1, filterScriptParams.size());
        assertEquals(50, filterScriptParams.get("v0"));
    }

    public void testBasicQueryText() throws IOException {
        index("{\"test\":\"test\"}",
            "{\"test\":\"test\"}");

        String expected =
                "     test      \n" +
                "---------------\n" +
                "test           \n" +
                "test           \n";
        Tuple<String, String> response = runSqlAsText("SELECT * FROM test", "text/plain");
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
        index("{\"name\":" + toJson("first") + ", \"number\" : 1 }",
              "{\"name\":" + toJson("second\t") + ", \"number\": 2 }",
              "{\"name\":" + toJson("\"third,\"") + ", \"number\": 3 }");

        String expected =
                "name,number\r\n" +
                "first,1\r\n" +
                "second\t,2\r\n" +
                "\"\"\"third,\"\"\",3\r\n";

        String query = "SELECT * FROM test ORDER BY number";
        Tuple<String, String> response = runSqlAsText(query, "text/csv");
        assertEquals(expected, response.v1());

        response = runSqlAsTextFormat(query, "csv");
        assertEquals(expected, response.v1());
    }

    public void testQueryWithoutHeaderInCSV() throws IOException {
        index("{\"name\":" + toJson("first") + ", \"number\" : 1 }",
              "{\"name\":" + toJson("second\t") + ", \"number\": 2 }",
              "{\"name\":" + toJson("\"third,\"") + ", \"number\": 3 }");

        String expected =
                "first,1\r\n" +
                "second\t,2\r\n" +
                "\"\"\"third,\"\"\",3\r\n";

        String query = "SELECT * FROM test ORDER BY number";
        Tuple<String, String> response = runSqlAsText(query, "text/csv; header=absent");
        assertEquals(expected, response.v1());
    }
    
    public void testNextPageCSV() throws IOException {
        executeQueryWithNextPage("text/csv; header=present", "text,number,sum\r\n", "%s,%d,%d\r\n");
    }

    public void testQueryInTSV() throws IOException {
        index("{\"name\":" + toJson("first") + ", \"number\" : 1 }",
              "{\"name\":" + toJson("second\t") + ", \"number\": 2 }",
              "{\"name\":" + toJson("\"third,\"") + ", \"number\": 3 }");

        String expected =
                "name\tnumber\n" +
                "first\t1\n" +
                "second\\t\t2\n" +
                "\"third,\"\t3\n";

        String query = "SELECT * FROM test ORDER BY number";
        Tuple<String, String> response = runSqlAsText(query, "text/tab-separated-values");
        assertEquals(expected, response.v1());
        response = runSqlAsTextFormat(query, "tsv");
        assertEquals(expected, response.v1());
    }
    
    public void testNextPageTSV() throws IOException {
        executeQueryWithNextPage("text/tab-separated-values", "text\tnumber\tsum\n", "%s\t%d\t%d\n");
    }
    
    private void executeQueryWithNextPage(String format, String expectedHeader, String expectedLineFormat) throws IOException {
        int size = 20;
        String[] docs = new String[size];
        for (int i = 0; i < size; i++) {
            docs[i] = "{\"text\":\"text" + i + "\", \"number\":" + i + "}\n";
        }
        index(docs);

        String request = "{\"query\":\"SELECT text, number, number + 5 AS sum FROM test ORDER BY number\", \"fetch_size\":2}";

        String cursor = null;
        for (int i = 0; i < 20; i += 2) {
            Tuple<String, String> response;
            if (i == 0) {
                response = runSqlAsText(StringUtils.EMPTY, new StringEntity(request, ContentType.APPLICATION_JSON), format);
            } else {
                response = runSqlAsText(StringUtils.EMPTY, new StringEntity("{\"cursor\":\"" + cursor + "\"}",
                        ContentType.APPLICATION_JSON), format);
            }

            StringBuilder expected = new StringBuilder();
            if (i == 0) {
                expected.append(expectedHeader);
                if (format == "text/plain") {
                    expected.append("---------------+---------------+---------------\n");
                }
            }
            expected.append(String.format(Locale.ROOT, expectedLineFormat, "text" + i, i, i + 5));
            expected.append(String.format(Locale.ROOT, expectedLineFormat, "text" + (i + 1), i + 1, i + 6));
            cursor = response.v2();
            assertEquals(expected.toString(), response.v1());
            assertNotNull(cursor);
        }
        Map<String, Object> expected = new HashMap<>();
        expected.put("rows", emptyList());
        assertResponse(expected, runSql(new StringEntity("{\"cursor\":\"" + cursor + "\"}", ContentType.APPLICATION_JSON),
                StringUtils.EMPTY, Mode.PLAIN.toString()));

        Map<String, Object> response = runSql(new StringEntity("{\"cursor\":\"" + cursor + "\"}", ContentType.APPLICATION_JSON),
                "/close", Mode.PLAIN.toString());
        assertEquals(true, response.get("succeeded"));

        assertEquals(0, getNumberOfSearchContexts("test"));
    }

    private Tuple<String, String> runSqlAsText(String sql, String accept) throws IOException {
        return runSqlAsText(StringUtils.EMPTY, new StringEntity("{\"query\":\"" + sql + "\"}", ContentType.APPLICATION_JSON), accept);
    }

    /**
     * Run SQL as text using the {@code Accept} header to specify the format
     * rather than the {@code format} parameter.
     */
    private Tuple<String, String> runSqlAsText(String suffix, HttpEntity entity, String accept) throws IOException {
        Request request = new Request("POST", SQL_QUERY_REST_ENDPOINT + suffix);
        request.addParameter("error_trace", "true");
        request.setEntity(entity);
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Accept", accept);
        request.setOptions(options);
        Response response = client().performRequest(request);
        return new Tuple<>(
                Streams.copyToString(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8)),
                response.getHeader("Cursor")
        );
    }

    /**
     * Run SQL as text using the {@code format} parameter to specify the format
     * rather than an {@code Accept} header.
     */
    private Tuple<String, String> runSqlAsTextFormat(String sql, String format) throws IOException {
        Request request = new Request("POST", SQL_QUERY_REST_ENDPOINT);
        request.addParameter("error_trace", "true");
        request.addParameter("format", format);
        request.setJsonEntity("{\"query\":\"" + sql + "\"}");

        Response response = client().performRequest(request);
        return new Tuple<>(
                Streams.copyToString(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8)),
                response.getHeader("Cursor")
        );
    }

    public static void assertResponse(Map<String, Object> expected, Map<String, Object> actual) {
        if (false == expected.equals(actual)) {
            NotEqualMessageBuilder message = new NotEqualMessageBuilder();
            message.compareMaps(actual, expected);
            fail("Response does not match:\n" + message.toString());
        }
    }

    public static int getNumberOfSearchContexts(String index) throws IOException {
        return getOpenContexts(searchStats(), index);
    }

    public static void assertNoSearchContexts() throws IOException {
        Map<String, Object> stats = searchStats();
        @SuppressWarnings("unchecked")
        Map<String, Object> indicesStats = (Map<String, Object>) stats.get("indices");
        for (String index : indicesStats.keySet()) {
            if (index.startsWith(".") == false) { // We are not interested in internal indices
                assertEquals(index + " should have no search contexts", 0, getOpenContexts(stats, index));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static int getOpenContexts(Map<String, Object> stats, String index) {
        stats = (Map<String, Object>) stats.get("indices");
        stats = (Map<String, Object>) stats.get(index);
        stats = (Map<String, Object>) stats.get("total");
        stats = (Map<String, Object>) stats.get("search");
        return (Integer) stats.get("open_contexts");
    }

    private static Map<String, Object> searchStats() throws IOException {
        Response response = client().performRequest(new Request("GET", "/_stats/search"));
        try (InputStream content = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }
    }
}
