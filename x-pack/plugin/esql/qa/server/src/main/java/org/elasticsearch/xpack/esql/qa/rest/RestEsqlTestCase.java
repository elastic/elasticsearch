/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;

import static java.util.Collections.emptySet;
import static java.util.Map.entry;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.Mode.ASYNC;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.Mode.SYNC;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToString;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public abstract class RestEsqlTestCase extends ESRestTestCase {

    // Test runner will run multiple suites in parallel, with some of them requiring preserving state between
    // tests (like EsqlSpecTestCase), so test data (like index name) needs not collide and cleanup must be done locally.
    private static final String TEST_INDEX_NAME = "rest-esql-test";

    private static final Logger LOGGER = LogManager.getLogger(RestEsqlTestCase.class);

    private static final String MAPPING_ALL_TYPES;

    private static final String MAPPING_ALL_TYPES_LOOKUP;

    static {
        String properties = EsqlTestUtils.loadUtf8TextFile("/mapping-all-types.json");
        MAPPING_ALL_TYPES = "{\"mappings\": " + properties + "}";
        String settings = "{\"settings\" : {\"mode\" : \"lookup\"}";
        MAPPING_ALL_TYPES_LOOKUP = settings + ", " + "\"mappings\": " + properties + "}";
    }

    private static final String DOCUMENT_TEMPLATE = """
        {"index":{"_id":"{}"}}
        {"boolean": {}, "byte": {}, "date": {}, "double": {}, "float": {}, "half_float": {}, "scaled_float": {}, "integer": {},""" + """
        "ip": {}, "keyword": {}, "long": {}, "unsigned_long": {}, "short": {}, "text": {},""" + """
         "version": {}, "wildcard": {}}
        """;

    // larger than any (unsigned) long
    private static final String HUMONGOUS_DOUBLE = "1E300";

    public static boolean shouldLog() {
        return false;
    }

    public enum Mode {
        SYNC,
        ASYNC
    }

    protected final Mode mode;

    protected RestEsqlTestCase(Mode mode) {
        this.mode = mode;
    }

    public record TypeAndValues(String type, List<?> values) {}

    public static class RequestObjectBuilder {
        private final XContentBuilder builder;
        private boolean isBuilt = false;

        private Map<String, Map<String, TypeAndValues>> tables;

        private Boolean keepOnCompletion = null;

        private Boolean profile = null;
        private Boolean includeCCSMetadata = null;

        private CheckedConsumer<XContentBuilder, IOException> filter;
        private Boolean allowPartialResults = null;

        public RequestObjectBuilder() throws IOException {
            this(randomFrom(XContentType.values()));
        }

        public RequestObjectBuilder(XContentType type) throws IOException {
            builder = XContentBuilder.builder(type, emptySet(), emptySet());
            builder.startObject();
        }

        public RequestObjectBuilder query(String query) throws IOException {
            builder.field("query", query);
            return this;
        }

        public RequestObjectBuilder tables(Map<String, Map<String, TypeAndValues>> tables) {
            this.tables = tables;
            return this;
        }

        public RequestObjectBuilder columnar(boolean columnar) throws IOException {
            builder.field("columnar", columnar);
            return this;
        }

        public RequestObjectBuilder params(String rawParams) throws IOException {
            builder.rawField("params", new BytesArray(rawParams).streamInput(), XContentType.JSON);
            return this;
        }

        public RequestObjectBuilder timeZone(ZoneId zoneId) throws IOException {
            builder.field("time_zone", zoneId);
            return this;
        }

        public RequestObjectBuilder waitForCompletion(TimeValue timeout) throws IOException {
            builder.field("wait_for_completion_timeout", timeout);
            return this;
        }

        public RequestObjectBuilder keepOnCompletion(boolean value) throws IOException {
            keepOnCompletion = value;
            builder.field("keep_on_completion", value);
            return this;
        }

        /**
         * Allow sending pragmas even in non-snapshot builds.
         */
        public RequestObjectBuilder pragmasOk() throws IOException {
            builder.field("accept_pragma_risks", true);
            return this;
        }

        Boolean keepOnCompletion() {
            return keepOnCompletion;
        }

        public RequestObjectBuilder keepAlive(TimeValue timeout) throws IOException {
            builder.field("keep_alive", timeout);
            return this;
        }

        public RequestObjectBuilder pragmas(Settings pragmas) throws IOException {
            builder.startObject("pragma");
            pragmas.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return this;
        }

        public RequestObjectBuilder profile(boolean profile) {
            this.profile = profile;
            return this;
        }

        public RequestObjectBuilder includeCCSMetadata(boolean includeCCSMetadata) {
            this.includeCCSMetadata = includeCCSMetadata;
            return this;
        }

        public RequestObjectBuilder filter(CheckedConsumer<XContentBuilder, IOException> filter) {
            this.filter = filter;
            return this;
        }

        public RequestObjectBuilder allowPartialResults(boolean allowPartialResults) {
            this.allowPartialResults = allowPartialResults;
            return this;
        }

        public Boolean allowPartialResults() {
            return allowPartialResults;
        }

        public RequestObjectBuilder build() throws IOException {
            if (isBuilt == false) {
                if (tables != null) {
                    builder.startObject("tables");
                    for (var table : tables.entrySet()) {
                        builder.startObject(table.getKey());
                        for (var column : table.getValue().entrySet()) {
                            builder.startObject(column.getKey());
                            builder.field(column.getValue().type(), column.getValue().values());
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                if (profile != null) {
                    builder.field("profile", profile);
                }
                if (includeCCSMetadata != null) {
                    builder.field("include_ccs_metadata", includeCCSMetadata);
                }
                if (filter != null) {
                    builder.startObject("filter");
                    filter.accept(builder);
                    builder.endObject();
                }
                builder.endObject();
                isBuilt = true;
            }
            return this;
        }

        public OutputStream getOutputStream() throws IOException {
            if (isBuilt == false) {
                throw new IllegalStateException("object not yet built");
            }
            builder.flush();
            return builder.getOutputStream();
        }

        public XContentType contentType() {
            return builder.contentType();
        }

        public static RequestObjectBuilder jsonBuilder() throws IOException {
            return new RequestObjectBuilder(XContentType.JSON);
        }
    }

    public void testGetAnswer() throws IOException {
        Map<String, Object> answer = runEsql(requestObjectBuilder().query("row a = 1, b = 2"));
        assertEquals(6, answer.size());
        assertThat(((Integer) answer.get("took")).intValue(), greaterThanOrEqualTo(0));
        Map<String, String> colA = Map.of("name", "a", "type", "integer");
        Map<String, String> colB = Map.of("name", "b", "type", "integer");
        assertMap(
            answer,
            matchesMap().entry("took", greaterThanOrEqualTo(0))
                .entry("is_partial", any(Boolean.class))
                .entry("documents_found", 0)
                .entry("values_loaded", 0)
                .entry("columns", List.of(colA, colB))
                .entry("values", List.of(List.of(1, 2)))
        );
    }

    public void testUseUnknownIndex() throws IOException {
        ResponseException e = expectThrows(ResponseException.class, () -> runEsql(requestObjectBuilder().query("from doesNotExist")));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("verification_exception"));
        assertThat(e.getMessage(), containsString("Unknown index [doesNotExist]"));
    }

    public void testNullInAggs() throws IOException {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            b.append(String.format(Locale.ROOT, """
                {"create":{"_index":"%s"}}
                """, testIndexName()));
            if (i % 10 == 0) {
                b.append(String.format(Locale.ROOT, """
                    {"group":%d}
                    """, i % 2));
            } else {
                b.append(String.format(Locale.ROOT, """
                    {"group":%d,"value":%d}
                    """, i % 2, i));
            }
        }
        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.addParameter("filter_path", "errors");
        bulk.setJsonEntity(b.toString());
        Response response = client().performRequest(bulk);
        assertThat(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8), equalTo("{\"errors\":false}"));

        RequestObjectBuilder builder = requestObjectBuilder().query(fromIndex() + " | stats min(value)");
        assertResultMap(runEsql(builder), List.of(Map.of("name", "min(value)", "type", "long")), List.of(List.of(1)));

        builder = requestObjectBuilder().query(fromIndex() + " | stats min(value) by group | sort group, `min(value)`");
        assertResultMap(
            runEsql(builder),
            List.of(Map.of("name", "min(value)", "type", "long"), Map.of("name", "group", "type", "long")),
            List.of(List.of(2, 0), List.of(1, 1))
        );
    }

    public void testColumnarMode() throws IOException {
        int docCount = randomIntBetween(3, 10);
        bulkLoadTestData(docCount);

        boolean columnar = randomBoolean();
        var query = requestObjectBuilder().query(fromIndex() + " | keep keyword, integer | sort integer asc");
        if (columnar || randomBoolean()) {
            query.columnar(columnar);
        }
        Map<String, Object> answer = runEsql(query);

        Map<String, String> colKeyword = Map.of("name", "keyword", "type", "keyword");
        Map<String, String> colInteger = Map.of("name", "integer", "type", "integer");
        assertEquals(List.of(colKeyword, colInteger), answer.get("columns"));

        if (columnar) {
            List<String> valKeyword = new ArrayList<>();
            List<Integer> valInteger = new ArrayList<>();
            for (int i = 0; i < docCount; i++) {
                valKeyword.add("keyword" + i);
                valInteger.add(i);
            }
            assertEquals(List.of(valKeyword, valInteger), answer.get("values"));
        } else {
            List<Object> rows = new ArrayList<>();
            for (int i = 0; i < docCount; i++) {
                rows.add(List.of("keyword" + i, i));
            }
            assertEquals(rows, answer.get("values"));
        }
    }

    public void testTextMode() throws IOException {
        int count = randomIntBetween(0, 100);
        bulkLoadTestData(count);
        var builder = requestObjectBuilder().query(fromIndex() + " | keep keyword, integer | sort integer asc | limit 100");
        assertEquals(expectedTextBody("txt", count, null), runEsqlAsTextWithFormat(builder, "txt", null, mode));
    }

    public void testCSVMode() throws IOException {
        int count = randomIntBetween(0, 100);
        bulkLoadTestData(count);
        var builder = requestObjectBuilder().query(fromIndex() + " | keep keyword, integer | sort integer asc | limit 100");
        assertEquals(expectedTextBody("csv", count, '|'), runEsqlAsTextWithFormat(builder, "csv", '|', mode));
    }

    public void testTSVMode() throws IOException {
        int count = randomIntBetween(0, 100);
        bulkLoadTestData(count);
        var builder = requestObjectBuilder().query(fromIndex() + " | keep keyword, integer | sort integer asc | limit 100");
        assertEquals(expectedTextBody("tsv", count, null), runEsqlAsTextWithFormat(builder, "tsv", null, mode));
    }

    public void testCSVNoHeaderMode() throws IOException {
        bulkLoadTestData(1);
        var builder = requestObjectBuilder().query(fromIndex() + " | keep keyword, integer | sort integer asc | limit 100");
        Request request = prepareRequest(SYNC);
        String mediaType = attachBody(builder.build(), request);
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Content-Type", mediaType);
        options.addHeader("Accept", "text/csv; header=absent");
        request.setOptions(options);
        HttpEntity entity = performRequest(request, new AssertWarnings.NoWarnings());
        String actual = Streams.copyToString(new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8));
        assertEquals("keyword0,0\r\n", actual);
    }

    public void testOutOfRangeComparisons() throws IOException {
        final int NUM_SINGLE_VALUE_ROWS = 100;
        bulkLoadTestData(NUM_SINGLE_VALUE_ROWS);
        bulkLoadTestData(10, NUM_SINGLE_VALUE_ROWS, false, RestEsqlTestCase::createDocumentWithMVs);
        bulkLoadTestData(5, NUM_SINGLE_VALUE_ROWS + 10, false, RestEsqlTestCase::createDocumentWithNulls);

        List<String> dataTypes = List.of(
            "alias_integer",
            "byte",
            "short",
            "integer",
            "long",
            // TODO: https://github.com/elastic/elasticsearch/issues/102935
            // "unsigned_long",
            "half_float",
            "float",
            "double",
            "scaled_float"
        );

        String lessOrLessEqual = randomFrom(" < ", " <= ");
        String largerOrLargerEqual = randomFrom(" > ", " >= ");
        String inEqualPlusMinus = randomFrom(" != ", " != -");
        String equalPlusMinus = randomFrom(" == ", " == -");
        // TODO: once we do not support infinity and NaN anymore, remove INFINITY/NAN cases.
        // https://github.com/elastic/elasticsearch/issues/98698#issuecomment-1847423390

        List<String> trueForSingleValuesPredicates = List.of(
            lessOrLessEqual + HUMONGOUS_DOUBLE,
            largerOrLargerEqual + " -" + HUMONGOUS_DOUBLE,
            inEqualPlusMinus + HUMONGOUS_DOUBLE
        );
        List<String> alwaysFalsePredicates = List.of(
            lessOrLessEqual + " -" + HUMONGOUS_DOUBLE,
            largerOrLargerEqual + HUMONGOUS_DOUBLE,
            equalPlusMinus + HUMONGOUS_DOUBLE,
            lessOrLessEqual + "to_double(null)",
            largerOrLargerEqual + "to_double(null)",
            equalPlusMinus + "to_double(null)",
            inEqualPlusMinus + "to_double(null)"
        );

        for (String fieldWithType : dataTypes) {
            for (String truePredicate : trueForSingleValuesPredicates) {
                String comparison = fieldWithType + truePredicate;
                var query = requestObjectBuilder().query(format(null, "from {} | where {}", testIndexName(), comparison));
                AssertWarnings assertWarnings = new AssertWarnings.ExactStrings(
                    List.of(
                        "Line 1:29: evaluation of [" + comparison + "] failed, treating result as null. Only first 20 failures recorded.",
                        "Line 1:29: java.lang.IllegalArgumentException: single-value function encountered multi-value"
                    )
                );
                var result = runEsql(query, assertWarnings, mode);

                var values = as(result.get("values"), ArrayList.class);
                assertThat(
                    format(null, "Comparison [{}] should return all rows with single values.", comparison),
                    values.size(),
                    is(NUM_SINGLE_VALUE_ROWS)
                );
            }

            for (String falsePredicate : alwaysFalsePredicates) {
                String comparison = fieldWithType + falsePredicate;
                var query = requestObjectBuilder().query(format(null, "from {} | where {}", testIndexName(), comparison));
                var result = runEsql(query);

                var values = as(result.get("values"), ArrayList.class);
                assertThat(format(null, "Comparison [{}] should return no rows.", comparison), values.size(), is(0));
            }
        }
    }

    // Test the Range created in PushFiltersToSource for qualified pushable filters on the same field
    public void testInternalRange() throws IOException {
        final int NUM_SINGLE_VALUE_ROWS = 100;
        bulkLoadTestData(NUM_SINGLE_VALUE_ROWS);
        bulkLoadTestData(10, NUM_SINGLE_VALUE_ROWS, false, RestEsqlTestCase::createDocumentWithMVs);
        bulkLoadTestData(5, NUM_SINGLE_VALUE_ROWS + 10, false, RestEsqlTestCase::createDocumentWithNulls);

        String upperBound = randomFrom(" < ", " <= ");
        String lowerBound = randomFrom(" > ", " >= ");

        String predicate = "{}" + upperBound + "{} and {}" + lowerBound + "{} and {} != {}";
        int half = NUM_SINGLE_VALUE_ROWS / 2;
        int halfPlusThree = half + 3;
        List<String> predicates = List.of(
            format(null, predicate, "integer", half, "integer", -1, "integer", half),
            format(null, predicate, "short", half, "short", -1, "short", half),
            format(null, predicate, "byte", half, "byte", -1, "byte", half),
            format(null, predicate, "long", half, "long", -1, "long", half),
            format(null, predicate, "double", half, "double", -1.0, "double", half),
            format(null, predicate, "float", half, "float", -1.0, "float", half),
            format(null, predicate, "half_float", half, "half_float", -1.0, "half_float", half),
            format(null, predicate, "scaled_float", half, "scaled_float", -1.0, "scaled_float", half),
            format(
                null,
                predicate,
                "date",
                "\"" + dateTimeToString(half) + "\"",
                "date",
                "\"1001-01-01\"",
                "date",
                "\"" + dateTimeToString(half) + "\""
            ),
            // keyword6-9 is greater than keyword53, [54,99] + [6, 9], 50 items in total
            format(
                null,
                predicate,
                "keyword",
                "\"keyword999\"",
                "keyword",
                "\"keyword" + halfPlusThree + "\"",
                "keyword",
                "\"keyword" + halfPlusThree + "\""
            ),
            format(null, predicate, "ip", "\"127.0.0." + half + "\"", "ip", "\"126.0.0.0\"", "ip", "\"127.0.0." + half + "\""),
            format(null, predicate, "version", "\"1.2." + half + "\"", "version", "\"1.2\"", "version", "\"1.2." + half + "\"")
        );

        for (String p : predicates) {
            var query = requestObjectBuilder().query(format(null, "from {} | where {}", testIndexName(), p));
            var result = runEsql(query, new AssertWarnings.NoWarnings(), mode);
            var values = as(result.get("values"), ArrayList.class);
            assertThat(
                format(null, "Comparison [{}] should return all rows with single values.", p),
                values.size(),
                is(NUM_SINGLE_VALUE_ROWS / 2)
            );
        }
    }

    public void testWarningHeadersOnFailedConversions() throws IOException {
        int count = randomFrom(10, 40, 60);
        bulkLoadTestData(count);

        Request request = prepareRequest(SYNC);
        var query = fromIndex()
            + " | sort integer asc | eval asInt = to_int(case(integer % 2 == 0, to_str(integer), keyword)) | limit 1000";
        var mediaType = attachBody(requestObjectBuilder().query(query).build(), request);

        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.setWarningsHandler(WarningsHandler.PERMISSIVE);
        options.addHeader("Content-Type", mediaType);
        options.addHeader("Accept", mediaType);

        request.setOptions(options);
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        int minExpectedWarnings = Math.min(count / 2, 20);
        var warnings = response.getWarnings();
        assertThat(warnings.size(), is(greaterThanOrEqualTo(1 + minExpectedWarnings))); // in multi-node there could be more
        var firstHeader = "Line 1:55: evaluation of [to_int(case(integer %25 2 == 0, to_str(integer), keyword))] failed, "
            + "treating result as null. Only first 20 failures recorded.";
        assertThat(warnings.get(0), containsString(firstHeader));
        for (int i = 1; i < warnings.size(); i++) {
            assertThat(
                warnings.get(i),
                containsString("org.elasticsearch.xpack.esql.core.InvalidArgumentException: Cannot parse number [keyword")
            );
        }
    }

    public void testMetadataFieldsOnMultipleIndices() throws IOException {
        var request = new Request("POST", "/" + testIndexName() + "-1/_doc/id-1");
        request.addParameter("refresh", "true");
        request.setJsonEntity("{\"a\": 1}");
        assertEquals(201, client().performRequest(request).getStatusLine().getStatusCode());
        request = new Request("POST", "/" + testIndexName() + "-1/_doc/id-1");
        request.addParameter("refresh", "true");
        request.setJsonEntity("{\"a\": 2}");
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
        request = new Request("POST", "/" + testIndexName() + "-2/_doc/id-2");
        request.addParameter("refresh", "true");
        request.setJsonEntity("{\"a\": 3}");
        assertEquals(201, client().performRequest(request).getStatusLine().getStatusCode());

        var query = fromIndex() + "* metadata _index, _version, _id | sort _version";
        Map<String, Object> result = runEsql(requestObjectBuilder().query(query));
        var columns = List.of(
            Map.of("name", "a", "type", "long"),
            Map.of("name", "_index", "type", "keyword"),
            Map.of("name", "_version", "type", "long"),
            Map.of("name", "_id", "type", "keyword")
        );
        var values = List.of(List.of(3, testIndexName() + "-2", 1, "id-2"), List.of(2, testIndexName() + "-1", 2, "id-1"));

        assertResultMap(result, columns, values);

        assertThat(deleteIndex(testIndexName() + "-1").isAcknowledged(), is(true)); // clean up
        assertThat(deleteIndex(testIndexName() + "-2").isAcknowledged(), is(true)); // clean up
    }

    public void testErrorMessageForEmptyParams() throws IOException {
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsql(requestObjectBuilder().query("row a = 1 | eval x = ?").params("[]"))
        );
        assertThat(EntityUtils.toString(re.getResponse().getEntity()), containsString("Not enough actual parameters 0"));
    }

    public void testErrorMessageForInvalidParams() throws IOException {
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsqlSync(
                requestObjectBuilder().query("row a = 1 | eval x = ?, y = ?")
                    .params(
                        "[{\"1\": \"v1\"}, {\"1-\": \"v1\"}, {\"-a\": \"v1\"}, {\"@-#\": \"v1\"}, true, 123, "
                            + "{\"type\": \"byte\", \"value\": 5}, {\"_1\": \"v1\"}, {\"_a\": \"v1\"}]"
                    )
            )
        );
        String error = EntityUtils.toString(re.getResponse().getEntity()).replaceAll("\\\\\n\s+\\\\", "");
        assertThat(error, containsString("[1] is not a valid parameter name"));
        assertThat(error, containsString("[1-] is not a valid parameter name"));
        assertThat(error, containsString("[-a] is not a valid parameter name"));
        assertThat(error, containsString("[@-#] is not a valid parameter name"));
        assertThat(error, not(containsString("[_a] is not a valid parameter name")));
        assertThat(error, not(containsString("[_1] is not a valid parameter name")));
        assertThat(error, containsString("Params cannot contain both named and unnamed parameters"));
        assertThat(error, containsString("Cannot parse more than one key:value pair as parameter"));
        re = expectThrows(
            ResponseException.class,
            () -> runEsqlSync(requestObjectBuilder().query("row a = ?0, b= ?2").params("[{\"n1\": \"v1\"}]"))
        );
        assertThat(
            EntityUtils.toString(re.getResponse().getEntity()),
            containsString("No parameter is defined for position 0, did you mean position 1")
        );
        assertThat(
            EntityUtils.toString(re.getResponse().getEntity()),
            containsString("No parameter is defined for position 2, did you mean position 1")
        );
        re = expectThrows(
            ResponseException.class,
            () -> runEsqlSync(requestObjectBuilder().query("row a = ?n0").params("[{\"n1\": \"v1\"}]"))
        );
        assertThat(EntityUtils.toString(re.getResponse().getEntity()), containsString("Unknown query parameter [n0], did you mean [n1]"));
    }

    public void testErrorMessageForInvalidIntervalParams() throws IOException {
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsqlSync(
                requestObjectBuilder().query("row x = ?n1::datetime | eval y = x + ?n2::time_duration")
                    .params("[{\"n1\": \"2024-01-01\"}, {\"n2\": \"3 days\"}]")
            )
        );

        String error = re.getMessage().replaceAll("\\\\\n\s+\\\\", "");
        assertThat(
            error,
            containsString(
                "Invalid interval value in [?n2::time_duration], expected integer followed by one of "
                    + "[MILLISECOND, MILLISECONDS, MS, SECOND, SECONDS, SEC, S, MINUTE, MINUTES, MIN, HOUR, HOURS, H] but got [3 days]"
            )
        );

        re = expectThrows(
            ResponseException.class,
            () -> runEsqlSync(
                requestObjectBuilder().query("row x = ?n1::datetime | eval y = x - ?n2::date_period")
                    .params("[{\"n1\": \"2024-01-01\"}, {\"n2\": \"3 hours\"}]")
            )
        );
        error = re.getMessage().replaceAll("\\\\\n\s+\\\\", "");
        assertThat(
            error,
            containsString(
                "Invalid interval value in [?n2::date_period], expected integer followed by one of "
                    + "[DAY, DAYS, D, WEEK, WEEKS, W, MONTH, MONTHS, MO, QUARTER, QUARTERS, Q, YEAR, YEARS, YR, Y] but got [3 hours]"
            )
        );
    }

    public void testErrorMessageForArrayValuesInParams() throws IOException {
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsql(RequestObjectBuilder.jsonBuilder().query("row a = 1 | eval x = ?").params("[{\"n1\": [5, 6, 7]}]"))
        );
        assertThat(
            EntityUtils.toString(re.getResponse().getEntity()),
            containsString("Failed to parse params: [1:45] n1=[5, 6, 7] is not supported as a parameter")
        );
    }

    public void testNamedParamsForIdentifierAndIdentifierPatterns() throws IOException {
        bulkLoadTestData(10);
        // positive
        var query = requestObjectBuilder().query(
            format(
                null,
                "from {} | eval x1 = ?n1 | where ?n2 == x1 | stats xx2 = ?fn1(?n3) by ?n4 | keep ?n4, ?n5 | sort ?n4",
                testIndexName()
            )
        )
            .params(
                "[{\"n1\" : {\"identifier\" : \"integer\"}}, {\"n2\" : {\"identifier\" : \"short\"}}, "
                    + "{\"n3\" : {\"identifier\" : \"double\"}}, {\"n4\" : {\"identifier\" : \"boolean\"}}, "
                    + "{\"n5\" : {\"pattern\" : \"xx*\"}}, {\"fn1\" : {\"identifier\" : \"max\"}}]"
            );
        Map<String, Object> result = runEsql(query);
        Map<String, String> colA = Map.of("name", "boolean", "type", "boolean");
        Map<String, String> colB = Map.of("name", "xx2", "type", "double");
        assertEquals(List.of(colA, colB), result.get("columns"));
        assertEquals(List.of(List.of(false, 9.1), List.of(true, 8.1)), result.get("values"));

        // missing params
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsqlSync(
                requestObjectBuilder().query(
                    format(
                        null,
                        "from {} | eval x1 = ?n1 | where ?n2 == x1 | stats xx2 = max(?n3) by ?n4 | keep ?n4, ?n5 | sort ?n4",
                        testIndexName()
                    )
                ).params("[]")
            )
        );
        String error = re.getMessage();
        assertThat(error, containsString("ParsingException"));
        assertThat(error, containsString("Unknown query parameter [n1]"));

        // param inside backquote is not recognized as a param
        Map<String, Integer> commandsWithLineNumber = Map.ofEntries(
            entry("eval x1 = `?n1`", 33),
            entry("where `?n1` == 1", 29),
            entry("stats x = max(n2) by `?n1`", 44),
            entry("stats x = max(`?n1`) by n2", 37),
            entry("keep `?n1`", 28),
            entry("sort `?n1`", 28)
        );
        for (Map.Entry<String, Integer> command : commandsWithLineNumber.entrySet()) {
            re = expectThrows(
                ResponseException.class,
                () -> runEsqlSync(
                    requestObjectBuilder().query(format(null, "from {} | {}", testIndexName(), command.getKey()))
                        .params("[{\"n1\" : {\"identifier\" : \"integer\"}}, {\"n2\" : {\"identifier\" : \"short\"}}]")
                )
            );
            error = re.getMessage();
            assertThat(error, containsString("VerificationException"));
            assertThat(error, containsString("line 1:" + command.getValue() + ": Unknown column [?n1]"));
        }

        commandsWithLineNumber = Map.ofEntries(
            entry("rename ?n1 as ?n2", 30),
            entry("enrich idx2 ON ?n1 WITH ?n2 = ?n3", 38),
            entry("keep ?n1", 28),
            entry("drop ?n1", 28)
        );
        for (Map.Entry<String, Integer> command : commandsWithLineNumber.entrySet()) {
            re = expectThrows(
                ResponseException.class,
                () -> runEsqlSync(
                    requestObjectBuilder().query(format(null, "from {} | {}", testIndexName(), command.getKey()))
                        .params(
                            "[{\"n1\" : {\"identifier\" : \"`n1`\"}}, {\"n2\" : {\"identifier\" : \"`n2`\"}}, "
                                + "{\"n3\" : {\"identifier\" : \"`n3`\"}}]"
                        )
                )
            );
            error = re.getMessage();
            assertThat(error, containsString("VerificationException"));
            assertThat(error, containsString("line 1:" + command.getValue() + ": Unknown column [`n1`]"));
        }

        // param cannot be used as a command name
        Map<String, String> paramsAsCommandNames = Map.ofEntries(
            entry("eval", "x = 1"),
            entry("where", "x == 1"),
            entry("stats", "x = count(*)"),
            entry("keep", "x"),
            entry("drop", "x"),
            entry("rename", "x as y"),
            entry("sort", "x"),
            entry("dissect", "x \"%{foo}\""),
            entry("grok", "x \"%{WORD:foo}\""),
            entry("enrich", "idx2 ON x"),
            entry("mvExpand", "x")
        );
        for (Map.Entry<String, String> command : paramsAsCommandNames.entrySet()) {
            re = expectThrows(
                ResponseException.class,
                () -> runEsqlSync(
                    requestObjectBuilder().query(format(null, "from {} | ?cmd {}", testIndexName(), command.getValue()))
                        .params("[{\"cmd\" : {\"identifier\" : \"" + command.getKey() + "\"}}]")
                )
            );
            error = re.getMessage();
            assertThat(error, containsString("ParsingException"));
            assertThat(error, containsString("line 1:23: mismatched input '?cmd' expecting {"));
            assertThat(error, containsString("'dissect', 'eval', 'grok', 'limit', 'sort'"));
        }
    }

    public void testErrorMessageForMissingParams() throws IOException {
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsql(requestObjectBuilder().query("from idx | where x == ?n1").params("[]"))
        );
        assertThat(
            EntityUtils.toString(re.getResponse().getEntity()).replaceAll("\\\\\n\s+\\\\", ""),
            containsString("line 1:23: Unknown query parameter [n1]")
        );

        re = expectThrows(
            ResponseException.class,
            () -> runEsql(requestObjectBuilder().query("from idx | where x == ?n1 and y == ?n2").params("[{\"n\" : \"v\"}]"))
        );
        assertThat(EntityUtils.toString(re.getResponse().getEntity()).replaceAll("\\\\\n\s+\\\\", ""), containsString("""
            line 1:23: Unknown query parameter [n1], did you mean [n]?; line 1:36: Unknown query parameter [n2], did you mean [n]?"""));

        re = expectThrows(
            ResponseException.class,
            () -> runEsql(requestObjectBuilder().query("from idx | where x == ?n1 and y == ?n2").params("[{\"n1\" : \"v1\"}]"))
        );
        assertThat(
            EntityUtils.toString(re.getResponse().getEntity()).replaceAll("\\\\\n\s+\\\\", ""),
            containsString("line 1:36: Unknown query parameter [n2], did you mean [n1]")
        );
    }

    public void testDoubleParamsForIdentifiers() throws IOException {
        assumeTrue("double parameters markers for identifiers", EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled());
        bulkLoadTestData(10);
        // positive
        // named double parameters
        var query = requestObjectBuilder().query(
            format(
                null,
                "from {} | eval x1 = ??n1 | where ??n2 == x1 | stats xx2 = ??fn1(??n3) by ??n4 | keep ??n4, ??n5 | sort ??n4",
                testIndexName()
            )
        )
            .params(
                "[{\"n1\" : \"integer\"}, {\"n2\" : \"short\"}, {\"n3\" : \"double\"}, {\"n4\" : \"boolean\"}, "
                    + "{\"n5\" : \"xx2\"}, {\"fn1\" : \"max\"}]"
            );
        validateResultsOfDoubleParametersForIdentifiers(query);

        // positional double parameters
        query = requestObjectBuilder().query(
            format(
                null,
                "from {} | eval x1 = ??1 | where ??2 == x1 | stats xx2 = ??6(??3) by ??4 | keep ??4, ??5 | sort ??4",
                testIndexName()
            )
        )
            .params(
                "[{\"n1\" : \"integer\"}, {\"n2\" : \"short\"}, {\"n3\" : \"double\"}, {\"n4\" : \"boolean\"}, "
                    + "{\"n5\" : \"xx2\"}, {\"fn1\" : \"max\"}]"
            );
        validateResultsOfDoubleParametersForIdentifiers(query);

        query = requestObjectBuilder().query(
            format(
                null,
                "from {} | eval x1 = ??1 | where ??2 == x1 | stats xx2 = ??6(??3) by ??4 | keep ??4, ??5 | sort ??4",
                testIndexName()
            )
        ).params("[\"integer\", \"short\", \"double\", \"boolean\", \"xx2\", \"max\"]");
        validateResultsOfDoubleParametersForIdentifiers(query);

        // anonymous double parameters
        query = requestObjectBuilder().query(
            format(null, "from {} | eval x1 = ?? | where ?? == x1 | stats xx2 = ??(??) by ?? | keep ??, ?? | sort ??", testIndexName())
        )
            .params(
                "[{\"n1\" : \"integer\"}, {\"n2\" : \"short\"}, {\"fn1\" : \"max\"}, {\"n3\" : \"double\"}, {\"n4\" : \"boolean\"}, "
                    + "{\"n4\" : \"boolean\"}, {\"n5\" : \"xx2\"}, {\"n4\" : \"boolean\"}]"
            );
        validateResultsOfDoubleParametersForIdentifiers(query);

        query = requestObjectBuilder().query(
            format(null, "from {} | eval x1 = ?? | where ?? == x1 | stats xx2 = ??(??) by ?? | keep ??, ?? | sort ??", testIndexName())
        ).params("[\"integer\", \"short\", \"max\", \"double\", \"boolean\", \"boolean\", \"xx2\", \"boolean\"]");
        validateResultsOfDoubleParametersForIdentifiers(query);

        // missing params
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsqlSync(
                requestObjectBuilder().query(
                    format(
                        null,
                        "from {} | eval x1 = ??n1 | where ??n2 == x1 | stats xx2 = max(??n3) by ??n4 | keep ??n4, ??n5 | sort ??n4",
                        testIndexName()
                    )
                ).params("[]")
            )
        );
        String error = re.getMessage().replaceAll("\\\\\n\s+\\\\", "");
        assertThat(error, containsString("ParsingException"));
        assertThat(error, containsString("Unknown query parameter [n1]"));

        // param inside backquote is not recognized as a param
        Map<String, Integer> commandsWithLineNumber = Map.ofEntries(
            entry("eval x1 = `??n1`", 33),
            entry("where `??n1` == 1", 29),
            entry("stats x = max(n2) by `??n1`", 44),
            entry("stats x = max(`??n1`) by n2", 37),
            entry("keep `??n1`", 28),
            entry("sort `??n1`", 28)
        );
        for (Map.Entry<String, Integer> command : commandsWithLineNumber.entrySet()) {
            re = expectThrows(
                ResponseException.class,
                () -> runEsqlSync(
                    requestObjectBuilder().query(format(null, "from {} | {}", testIndexName(), command.getKey()))
                        .params("[{\"n1\" : \"integer\"}, {\"n2\" : \"short\"}]")
                )
            );
            error = re.getMessage().replaceAll("\\\\\n\s+\\\\", "");
            assertThat(error, containsString("VerificationException"));
            assertThat(error, containsString("line 1:" + command.getValue() + ": Unknown column [??n1]"));
        }

        commandsWithLineNumber = Map.ofEntries(
            entry("rename ??n1 as ??n2", 30),
            entry("enrich idx2 ON ??n1 WITH ??n2 = ??n3", 38),
            entry("keep ??n1", 28),
            entry("drop ??n1", 28)
        );
        for (Map.Entry<String, Integer> command : commandsWithLineNumber.entrySet()) {
            re = expectThrows(
                ResponseException.class,
                () -> runEsqlSync(
                    requestObjectBuilder().query(format(null, "from {} | {}", testIndexName(), command.getKey()))
                        .params("[{\"n1\" : \"`n1`\"}, {\"n2\" : \"`n2`\"}, {\"n3\" : \"`n3`\"}]")
                )
            );
            error = re.getMessage().replaceAll("\\\\\n\s+\\\\", "");
            assertThat(error, containsString("VerificationException"));
            assertThat(error, containsString("line 1:" + command.getValue() + ": Unknown column [`n1`]"));
        }

        // param cannot be used as a command name
        Map<String, String> paramsAsCommandNames = Map.ofEntries(
            entry("eval", "x = 1"),
            entry("where", "x == 1"),
            entry("stats", "x = count(*)"),
            entry("keep", "x"),
            entry("drop", "x"),
            entry("rename", "x as y"),
            entry("sort", "x"),
            entry("dissect", "x \"%{foo}\""),
            entry("grok", "x \"%{WORD:foo}\""),
            entry("enrich", "idx2 ON x"),
            entry("mvExpand", "x")
        );
        for (Map.Entry<String, String> command : paramsAsCommandNames.entrySet()) {
            re = expectThrows(
                ResponseException.class,
                () -> runEsqlSync(
                    requestObjectBuilder().query(format(null, "from {} | ??cmd {}", testIndexName(), command.getValue()))
                        .params("[{\"cmd\" : \"" + command.getKey() + "\"}]")
                )
            );
            error = re.getMessage().replaceAll("\\\\\n\s+\\\\", "");
            assertThat(error, containsString("ParsingException"));
            assertThat(error, containsString("line 1:23: mismatched input '??cmd' expecting {"));
        }
    }

    public void testDoubleParamsWithLookupJoin() throws IOException {
        assumeTrue("double parameters markers for identifiers", EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled());
        bulkLoadTestDataLookupMode(10);
        var query = requestObjectBuilder().query(
            format(
                null,
                "from {} | eval x1 = ??n1 | where ??n2 == x1 | lookup join {} on ??n3 | keep ??n4 | sort ??n4",
                testIndexName(),
                testIndexName()
            )
        ).params("[{\"n1\" : \"integer\"}, {\"n2\" : \"short\"}, {\"n3\" : \"double\"}, {\"n4\" : \"boolean\"}]");
        Map<String, Object> result = runEsql(query);
        Map<String, String> colA = Map.of("name", "boolean", "type", "boolean");
        assertEquals(List.of(colA), result.get("columns"));
        assertEquals(
            List.of(
                List.of(false),
                List.of(false),
                List.of(false),
                List.of(false),
                List.of(false),
                List.of(true),
                List.of(true),
                List.of(true),
                List.of(true),
                List.of(true)
            ),
            result.get("values")
        );
    }

    private void validateResultsOfDoubleParametersForIdentifiers(RequestObjectBuilder query) throws IOException {
        Map<String, Object> result = runEsql(query);
        Map<String, String> colA = Map.of("name", "boolean", "type", "boolean");
        Map<String, String> colB = Map.of("name", "xx2", "type", "double");
        assertEquals(List.of(colA, colB), result.get("columns"));
        assertEquals(List.of(List.of(false, 9.1), List.of(true, 8.1)), result.get("values"));
    }

    public void testMultipleBatchesWithLookupJoin() throws IOException {
        assumeTrue(
            "Makes numberOfChannels consistent with layout map for join with multiple batches",
            EsqlCapabilities.Cap.MAKE_NUMBER_OF_CHANNELS_CONSISTENT_WITH_LAYOUT.isEnabled()
        );
        // Create more than 10 indices to trigger multiple batches of data node execution.
        // The sort field should be missing on some indices to reproduce NullPointerException caused by duplicated items in layout
        for (int i = 1; i <= 20; i++) {
            createIndex("idx" + i, randomBoolean(), "\"mappings\": {\"properties\" : {\"a\" : {\"type\" : \"keyword\"}}}");
        }
        bulkLoadTestDataLookupMode(10);
        // lookup join with and without sort
        for (String sort : List.of("", "| sort integer")) {
            var query = requestObjectBuilder().query(format(null, "from * | lookup join {} on integer {}", testIndexName(), sort));
            Map<String, Object> result = runEsql(query);
            var columns = as(result.get("columns"), List.class);
            assertEquals(21, columns.size());
            var values = as(result.get("values"), List.class);
            assertEquals(10, values.size());
        }
        // clean up
        for (int i = 1; i <= 20; i++) {
            assertThat(deleteIndex("idx" + i).isAcknowledged(), is(true));
        }
    }

    public void testErrorMessageForLiteralDateMathOverflow() throws IOException {
        List<String> dateMathOverflowExpressions = List.of(
            "2147483647 day + 1 day",
            "306783378 week + 1 week",
            "2147483647 month + 1 month",
            "2147483647 year + 1 year",
            // We cannot easily force an overflow using just milliseconds, since these are divided by 1000 and then the resulting seconds
            // are stored in a long. But combining with seconds works.
            "9223372036854775807 second + 1000 millisecond",
            "9223372036854775807 second + 1 second",
            "153722867280912930 minute + 1 minute",
            "2562047788015215 hour + 1 hour"

        );

        for (String overflowExp : dateMathOverflowExpressions) {
            assertExceptionForDateMath(overflowExp, "overflow");
        }

    }

    public void testErrorMessageForLiteralDateMathOverflowOnNegation() throws IOException {
        assertExceptionForDateMath("-(-2147483647 year - 1 year)", "overflow");
        assertExceptionForDateMath("-(-9223372036854775807 second - 1 second)", "Exceeds capacity of Duration");
    }

    private void assertExceptionForDateMath(String dateMathString, String errorSubstring) throws IOException {
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsql(requestObjectBuilder().query("row a = 1 | eval x = now() + (" + dateMathString + ")"))
        );

        String responseMessage = EntityUtils.toString(re.getResponse().getEntity());
        // the error in the response message might be chopped up by newlines, but finding "overflow" should suffice.
        assertThat(responseMessage, containsString(errorSubstring));

        assertThat(re.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testComplexFieldNames() throws IOException {
        bulkLoadTestData(1);
        // catch verification exception, field names not found
        int fieldNumber = 5000;
        String q1 = fromIndex() + queryWithComplexFieldNames(fieldNumber);
        ResponseException e = expectThrows(ResponseException.class, () -> runEsql(requestObjectBuilder().query(q1)));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("verification_exception"));

        // catch automaton's TooComplexToDeterminizeException
        fieldNumber = 6000;
        final String q2 = fromIndex() + queryWithComplexFieldNames(fieldNumber);
        e = expectThrows(ResponseException.class, () -> runEsql(requestObjectBuilder().query(q2)));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("The field names are too complex to process"));
    }

    /**
     * INLINESTATS <strong>can</strong> group on {@code NOW()}. It's a little silly, but
     * doing something like {@code DATE_TRUNC(1 YEAR, NOW() - 1970-01-01T00:00:00Z)} is
     * much more sensible. But just grouping on {@code NOW()} is enough to test this.
     * <p>
     *     This works because {@code NOW()} locks it's value at the start of the entire
     *     query. It's part of the "configuration" of the query.
     * </p>
     */
    @AwaitsFix(bugUrl = "Disabled temporarily until JOIN implementation is completed")
    public void testInlineStatsNow() throws IOException {
        assumeTrue("INLINESTATS only available on snapshots", Build.current().isSnapshot());
        indexTimestampData(1);

        RequestObjectBuilder builder = requestObjectBuilder().query(
            fromIndex() + " | EVAL now=NOW() | INLINESTATS AVG(value) BY now | SORT value ASC"
        );
        Map<String, Object> result = runEsql(builder);
        ListMatcher values = matchesList();
        for (int i = 0; i < 1000; i++) {
            values = values.item(
                matchesList().item("2020-12-12T00:00:00.000Z")
                    .item("value" + i)
                    .item("value" + i)
                    .item(i)
                    .item(any(String.class))
                    .item(499.5)
            );
        }
        assertResultMap(
            result,
            matchesList().item(matchesMap().entry("name", "@timestamp").entry("type", "date"))
                .item(matchesMap().entry("name", "test").entry("type", "text"))
                .item(matchesMap().entry("name", "test.keyword").entry("type", "keyword"))
                .item(matchesMap().entry("name", "value").entry("type", "long"))
                .item(matchesMap().entry("name", "now").entry("type", "date"))
                .item(matchesMap().entry("name", "AVG(value)").entry("type", "double")),
            values
        );
    }

    public void testTopLevelFilter() throws IOException {
        indexTimestampData(3); // Multiple shards has caused a bug in the past with the merging case below

        RequestObjectBuilder builder = requestObjectBuilder().filter(b -> {
            b.startObject("range");
            {
                b.startObject("@timestamp").field("gte", "2020-12-12").endObject();
            }
            b.endObject();
        }).query(fromIndex() + " | STATS SUM(value)");

        Map<String, Object> result = runEsql(builder);
        assertResultMap(
            result,
            matchesList().item(matchesMap().entry("name", "SUM(value)").entry("type", "long")),
            List.of(List.of(499500))
        );
    }

    public void testTopLevelFilterMerged() throws IOException {
        indexTimestampData(3); // Multiple shards has caused a bug in the past with the merging case below

        RequestObjectBuilder builder = requestObjectBuilder().filter(b -> {
            b.startObject("range");
            {
                b.startObject("@timestamp").field("gte", "2020-12-12").endObject();
            }
            b.endObject();
        }).query(fromIndex() + " | WHERE value == 12 | STATS SUM(value)");
        Map<String, Object> result = runEsql(builder);
        assertResultMap(result, matchesList().item(matchesMap().entry("name", "SUM(value)").entry("type", "long")), List.of(List.of(12)));
    }

    public void testTopLevelFilterBoolMerged() throws IOException {
        indexTimestampData(3); // Multiple shards has caused a bug in the past

        for (int i = 0; i < 100; i++) {
            // Run the query many times so we're more likely to bump into any sort of modification problems
            RequestObjectBuilder builder = requestObjectBuilder().filter(b -> {
                b.startObject("bool");
                {
                    b.startArray("filter");
                    {
                        b.startObject().startObject("range");
                        {
                            b.startObject("@timestamp").field("gte", "2020-12-12").endObject();
                        }
                        b.endObject().endObject();
                        b.startObject().startObject("match");
                        {
                            b.field("test", "value12");
                        }
                        b.endObject().endObject();
                    }
                    b.endArray();
                }
                b.endObject();
            }).query(fromIndex() + " | WHERE @timestamp > \"2010-01-01\" | STATS SUM(value)");
            Map<String, Object> result = runEsql(builder);
            assertResultMap(
                result,
                matchesList().item(matchesMap().entry("name", "SUM(value)").entry("type", "long")),
                List.of(List.of(12))
            );
        }
    }

    private static String queryWithComplexFieldNames(int field) {
        StringBuilder query = new StringBuilder();
        query.append(" | keep ").append(randomAlphaOfLength(10)).append(1);
        for (int i = 2; i <= field; i++) {
            query.append(", ").append(randomAlphaOfLength(10)).append(i);
        }
        return query.toString();
    }

    private static String expectedTextBody(String format, int count, @Nullable Character csvDelimiter) {
        StringBuilder sb = new StringBuilder();
        switch (format) {
            case "txt" -> {
                sb.append("    keyword    |    integer    \n");
                sb.append("---------------+---------------\n");
            }
            case "csv" -> sb.append("keyword").append(csvDelimiter).append("integer\r\n");
            case "tsv" -> sb.append("keyword\tinteger\n");
            default -> {
                assert false : "unexpected format type [" + format + "]";
            }
        }
        for (int i = 0; i < count; i++) {
            sb.append("keyword").append(i);
            int iLen = String.valueOf(i).length();
            switch (format) {
                case "txt" -> sb.append(" ".repeat(8 - iLen)).append("|");
                case "csv" -> sb.append(csvDelimiter);
                case "tsv" -> sb.append('\t');
            }
            sb.append(i);
            if (format.equals("txt")) {
                sb.append(" ".repeat(15 - iLen));
            }
            sb.append(format.equals("csv") ? "\r\n" : "\n");
        }
        return sb.toString();
    }

    public Map<String, Object> runEsql(RequestObjectBuilder requestObject) throws IOException {
        return runEsql(requestObject, new AssertWarnings.NoWarnings(), mode);
    }

    public static Map<String, Object> runEsqlSync(RequestObjectBuilder requestObject) throws IOException {
        return runEsqlSync(requestObject, new AssertWarnings.NoWarnings());
    }

    public static Map<String, Object> runEsqlAsync(RequestObjectBuilder requestObject) throws IOException {
        return runEsqlAsync(requestObject, randomBoolean(), new AssertWarnings.NoWarnings());
    }

    public static Map<String, Object> runEsql(RequestObjectBuilder requestObject, AssertWarnings assertWarnings, Mode mode)
        throws IOException {
        if (mode == ASYNC) {
            return runEsqlAsync(requestObject, randomBoolean(), assertWarnings);
        } else {
            return runEsqlSync(requestObject, assertWarnings);
        }
    }

    public static Map<String, Object> runEsqlSync(RequestObjectBuilder requestObject, AssertWarnings assertWarnings) throws IOException {
        Request request = prepareRequestWithOptions(requestObject, SYNC);

        HttpEntity entity = performRequest(request, assertWarnings);
        return entityToMap(entity, requestObject.contentType());
    }

    public static Map<String, Object> runEsqlAsync(RequestObjectBuilder requestObject, AssertWarnings assertWarnings) throws IOException {
        return runEsqlAsync(requestObject, randomBoolean(), assertWarnings);
    }

    public static Map<String, Object> runEsqlAsync(
        RequestObjectBuilder requestObject,
        boolean keepOnCompletion,
        AssertWarnings assertWarnings
    ) throws IOException {
        addAsyncParameters(requestObject, keepOnCompletion);
        Request request = prepareRequestWithOptions(requestObject, ASYNC);

        if (shouldLog()) {
            LOGGER.info("REQUEST={}", request);
        }

        Response response = performRequest(request);
        HttpEntity entity = response.getEntity();

        Object initialColumns = null;
        Object initialValues = null;
        var json = entityToMap(entity, requestObject.contentType());
        checkKeepOnCompletion(requestObject, json, keepOnCompletion);
        String id = (String) json.get("id");

        var supportsAsyncHeaders = clusterHasCapability("POST", "/_query", List.of(), List.of("async_query_status_headers")).orElse(false);
        var supportsSuggestedCast = clusterHasCapability("POST", "/_query", List.of(), List.of("suggested_cast")).orElse(false);

        if (id == null) {
            // no id returned from an async call, must have completed immediately and without keep_on_completion
            assertThat(requestObject.keepOnCompletion(), either(nullValue()).or(is(false)));
            assertThat((boolean) json.get("is_running"), is(false));
            if (supportsAsyncHeaders) {
                assertThat(response.getHeader("X-Elasticsearch-Async-Id"), nullValue());
                assertThat(response.getHeader("X-Elasticsearch-Async-Is-Running"), is("?0"));
            }
            assertWarnings(response, assertWarnings);
            json.remove("is_running"); // remove this to not mess up later map assertions
            return Collections.unmodifiableMap(json);
        } else {
            // async may not return results immediately, so may need an async get
            assertThat(id, is(not(emptyOrNullString())));
            boolean isRunning = (boolean) json.get("is_running");
            if (isRunning == false) {
                // must have completed immediately so keep_on_completion must be true
                assertThat(requestObject.keepOnCompletion(), is(true));
                assertWarnings(response, assertWarnings);
                // we already have the results, but let's remember them so that we can compare to async get
                initialColumns = json.get("columns");
                initialValues = json.get("values");
            } else {
                // did not return results immediately, so we will need an async get
                assertThat(json.get("columns"), is(equalTo(List.<Map<String, String>>of()))); // no partial results
                assertThat(json.get("pages"), nullValue());
            }

            if (supportsAsyncHeaders) {
                assertThat(response.getHeader("X-Elasticsearch-Async-Id"), is(id));
                assertThat(response.getHeader("X-Elasticsearch-Async-Is-Running"), is(isRunning ? "?1" : "?0"));
            }

            // issue a second request to "async get" the results
            Request getRequest = prepareAsyncGetRequest(id);
            getRequest.setOptions(request.getOptions());
            response = performRequest(getRequest);
            entity = response.getEntity();
        }

        var result = entityToMap(entity, requestObject.contentType());

        // assert initial contents, if any, are the same as async get contents
        if (initialColumns != null) {
            if (supportsSuggestedCast == false) {
                assertEquals(
                    removeOriginalTypesAndSuggestedCast(initialColumns),
                    removeOriginalTypesAndSuggestedCast(result.get("columns"))
                );
            } else {
                assertEquals(initialColumns, result.get("columns"));
            }
            assertEquals(initialValues, result.get("values"));
        }

        assertWarnings(response, assertWarnings);
        assertDeletable(id);
        return removeAsyncProperties(result);
    }

    private static Object removeOriginalTypesAndSuggestedCast(Object response) {
        if (response instanceof ArrayList<?> columns) {
            var newColumns = new ArrayList<>();
            for (var column : columns) {
                if (column instanceof Map<?, ?> columnMap) {
                    var newMap = new HashMap<>(columnMap);
                    newMap.remove("original_types");
                    newMap.remove("suggested_cast");
                    newColumns.add(newMap);
                } else {
                    newColumns.add(column);
                }
            }
            return newColumns;
        } else {
            return response;
        }
    }

    public void testAsyncGetWithoutContentType() throws IOException {
        int count = randomIntBetween(0, 100);
        bulkLoadTestData(count);
        var requestObject = requestObjectBuilder().query(fromIndex() + " | keep keyword, integer | sort integer asc | limit 100");

        addAsyncParameters(requestObject, true);
        Request request = prepareRequestWithOptions(requestObject, ASYNC);

        if (shouldLog()) {
            LOGGER.info("REQUEST={}", request);
        }

        Response response = performRequest(request);
        HttpEntity entity = response.getEntity();

        var json = entityToMap(entity, requestObject.contentType());
        checkKeepOnCompletion(requestObject, json, true);
        String id = (String) json.get("id");
        // results won't be returned because wait_for_completion is provided a very small interval
        assertThat(id, is(not(emptyOrNullString())));

        // issue an "async get" request with no Content-Type
        Request getRequest = prepareAsyncGetRequest(id);
        response = performRequest(getRequest);
        entity = response.getEntity();
        var result = entityToMap(entity, XContentType.JSON);

        ListMatcher values = matchesList();
        for (int i = 0; i < count; i++) {
            values = values.item(matchesList().item("keyword" + i).item(i));
        }
        assertResultMap(
            result,
            getResultMatcher(result).entry("id", id).entry("is_running", false),
            matchesList().item(matchesMap().entry("name", "keyword").entry("type", "keyword"))
                .item(matchesMap().entry("name", "integer").entry("type", "integer")),
            values
        );

    }

    protected static Request prepareRequestWithOptions(RequestObjectBuilder requestObject, Mode mode) throws IOException {
        requestObject.build();
        Request request = prepareRequest(mode);
        String mediaType = attachBody(requestObject, request);
        if (requestObject.allowPartialResults != null) {
            request.addParameter("allow_partial_results", String.valueOf(requestObject.allowPartialResults));
        }

        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.setWarningsHandler(WarningsHandler.PERMISSIVE); // We assert the warnings ourselves
        options.addHeader("Content-Type", mediaType);

        if (randomBoolean()) {
            options.addHeader("Accept", mediaType);
        } else {
            request.addParameter("format", requestObject.contentType().queryParameter());
        }
        request.setOptions(options);
        return request;
    }

    // Removes async properties, otherwise consuming assertions would need to handle sync and async differences
    static Map<String, Object> removeAsyncProperties(Map<String, Object> map) {
        Map<String, Object> copy = new HashMap<>(map);
        assertFalse((boolean) copy.remove("is_running"));
        copy.remove("id"); // id is optional, do not assert its removal
        return Collections.unmodifiableMap(copy);
    }

    protected static Map<String, Object> entityToMap(HttpEntity entity, XContentType expectedContentType) throws IOException {
        var result = EsqlTestUtils.entityToMap(entity, expectedContentType);
        if (shouldLog()) {
            LOGGER.info("entity={}", result);
        }
        return result;
    }

    static void addAsyncParameters(RequestObjectBuilder requestObject, boolean keepOnCompletion) throws IOException {
        // deliberately short in order to frequently trigger return without results
        requestObject.waitForCompletion(TimeValue.timeValueNanos(randomIntBetween(1, 100)));
        requestObject.keepOnCompletion(keepOnCompletion);
        requestObject.keepAlive(TimeValue.timeValueDays(randomIntBetween(1, 10)));
    }

    // If keep_on_completion is set then an id must always be present, regardless of the value of any other property.
    static void checkKeepOnCompletion(RequestObjectBuilder requestObject, Map<String, Object> json, boolean keepOnCompletion) {
        if (requestObject.keepOnCompletion()) {
            assertTrue(keepOnCompletion);
            assertThat((String) json.get("id"), not(emptyOrNullString()));
        } else {
            assertFalse(keepOnCompletion);
        }
    }

    static void assertDeletable(String id) throws IOException {
        var request = prepareAsyncDeleteRequest(id);
        performRequest(request);

        // the stored response should no longer be retrievable
        ResponseException re = expectThrows(ResponseException.class, () -> deleteNonExistent(request));
        assertThat(EntityUtils.toString(re.getResponse().getEntity()), containsString(id));
    }

    static void deleteNonExistent(Request request) throws IOException {
        Response response = client().performRequest(request);
        assertEquals(404, response.getStatusLine().getStatusCode());
    }

    static String runEsqlAsTextWithFormat(RequestObjectBuilder builder, String format, @Nullable Character delimiter, Mode mode)
        throws IOException {
        Request request = prepareRequest(mode);
        if (mode == ASYNC) {
            addAsyncParameters(builder, randomBoolean());
        }
        String mediaType = attachBody(builder.build(), request);

        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Content-Type", mediaType);

        boolean addParam = randomBoolean();
        if (addParam) {
            request.addParameter("format", format);
        } else {
            switch (format) {
                case "txt" -> options.addHeader("Accept", "text/plain");
                case "csv" -> options.addHeader("Accept", "text/csv");
                case "tsv" -> options.addHeader("Accept", "text/tab-separated-values");
            }
        }
        if (delimiter != null) {
            request.addParameter("delimiter", String.valueOf(delimiter));
        }
        request.setOptions(options);

        if (shouldLog()) {
            LOGGER.info("REQUEST={}", request);
        }

        Response response = performRequest(request);
        HttpEntity entity = assertWarnings(response, new AssertWarnings.NoWarnings());

        // get the content, it could be empty because the request might have not completed
        String initialValue = Streams.copyToString(new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8));
        String id = response.getHeader("X-Elasticsearch-Async-Id");

        if (mode == SYNC) {
            assertThat(id, is(emptyOrNullString()));
            return initialValue;
        }

        if (id == null) {
            // no id returned from an async call, must have completed immediately and without keep_on_completion
            assertThat(builder.keepOnCompletion(), either(nullValue()).or(is(false)));
            assertNull(response.getHeader("is_running"));
            // the content cant be empty
            assertThat(initialValue, not(emptyOrNullString()));
            return initialValue;
        } else {
            // async may not return results immediately, so may need an async get
            assertThat(id, is(not(emptyOrNullString())));
            String isRunning = response.getHeader("X-Elasticsearch-Async-Is-Running");
            if ("?0".equals(isRunning)) {
                // must have completed immediately so keep_on_completion must be true
                assertThat(builder.keepOnCompletion(), is(true));
            } else {
                // did not return results immediately, so we will need an async get
                // Also, different format modes return different results.
                switch (format) {
                    case "txt" -> assertThat(initialValue, emptyOrNullString());
                    case "csv" -> {
                        assertEquals("\r\n", initialValue);
                        initialValue = "";
                    }
                    case "tsv" -> {
                        assertEquals("\n", initialValue);
                        initialValue = "";
                    }
                }
            }
            // issue a second request to "async get" the results
            Request getRequest = prepareAsyncGetRequest(id);
            if (delimiter != null) {
                getRequest.addParameter("delimiter", String.valueOf(delimiter));
            }
            // If the `format` parameter is not added, the GET request will return a response
            // with the `Content-Type` type due to the lack of an `Accept` header.
            if (addParam) {
                getRequest.addParameter("format", format);
            }
            // if `addParam` is false, `options` will already have an `Accept` header
            getRequest.setOptions(options);
            response = performRequest(getRequest);
            entity = assertWarnings(response, new AssertWarnings.NoWarnings());
        }
        String newValue = Streams.copyToString(new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8));

        // assert initial contents, if any, are the same as async get contents
        if (initialValue != null && initialValue.isEmpty() == false) {
            assertEquals(initialValue, newValue);
        }

        assertDeletable(id);
        return newValue;
    }

    private static Request prepareRequest(Mode mode) {
        return finishRequest(new Request("POST", "/_query" + (mode == ASYNC ? "/async" : "")));
    }

    private static Request prepareAsyncGetRequest(String id) {
        return finishRequest(new Request("GET", "/_query/async/" + id + "?wait_for_completion_timeout=60s"));
    }

    private static Request prepareAsyncDeleteRequest(String id) {
        return finishRequest(new Request("DELETE", "/_query/async/" + id));
    }

    private static Request finishRequest(Request request) {
        request.addParameter("error_trace", "true");   // Helps with debugging in case something crazy happens on the server.
        request.addParameter("pretty", "true");        // Improves error reporting readability
        return request;
    }

    private static String attachBody(RequestObjectBuilder requestObject, Request request) throws IOException {
        String mediaType = requestObject.contentType().mediaTypeWithoutParameters();
        try (ByteArrayOutputStream bos = (ByteArrayOutputStream) requestObject.getOutputStream()) {
            request.setEntity(new NByteArrayEntity(bos.toByteArray(), ContentType.getByMimeType(mediaType)));
        }
        return mediaType;
    }

    private static HttpEntity performRequest(Request request, AssertWarnings assertWarnings) throws IOException {
        return assertWarnings(performRequest(request), assertWarnings);
    }

    protected static Response performRequest(Request request) throws IOException {
        Response response = client().performRequest(request);
        if (shouldLog()) {
            LOGGER.info("RESPONSE={}", response);
            LOGGER.info("RESPONSE headers={}", Arrays.toString(response.getHeaders()));
        }
        assertEquals(200, response.getStatusLine().getStatusCode());
        return response;
    }

    private static HttpEntity assertWarnings(Response response, AssertWarnings assertWarnings) {
        List<String> warnings = new ArrayList<>(response.getWarnings());
        warnings.removeAll(mutedWarnings());
        if (shouldLog()) {
            LOGGER.info("RESPONSE warnings (after muted)={}", warnings);
        }
        assertWarnings.assertWarnings(warnings);
        return response.getEntity();
    }

    private static Set<String> mutedWarnings() {
        return Set.of(
            "No limit defined, adding default limit of [1000]",
            "No limit defined, adding default limit of [500]" // this is for bwc tests, the limit in v 8.12.x is 500
        );
    }

    private static void bulkLoadTestData(int count) throws IOException {
        bulkLoadTestData(count, 0, true, RestEsqlTestCase::createDocument);
    }

    private static void bulkLoadTestDataLookupMode(int count) throws IOException {
        createIndex(testIndexName(), true);
        bulkLoadTestData(count, 0, false, RestEsqlTestCase::createDocument);
    }

    private static void createIndex(String indexName, boolean lookupMode) throws IOException {
        Request request = new Request("PUT", "/" + indexName);
        request.setJsonEntity(lookupMode ? MAPPING_ALL_TYPES_LOOKUP : MAPPING_ALL_TYPES);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
    }

    private static void bulkLoadTestData(int count, int firstIndex, boolean createIndex, IntFunction<String> createDocument)
        throws IOException {
        Request request;
        if (createIndex) {
            createIndex(testIndexName(), false);
        }

        if (count > 0) {
            request = new Request("POST", "/" + testIndexName() + "/_bulk");
            request.addParameter("refresh", "true");

            StringBuilder bulk = new StringBuilder();
            for (int i = 0; i < count; i++) {
                bulk.append(createDocument.apply(i + firstIndex));
            }
            request.setJsonEntity(bulk.toString());
            assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
        }
    }

    private static String createDocument(int i) {
        return format(
            null,
            DOCUMENT_TEMPLATE,
            i,
            ((i & 1) == 0),
            (i % 256),
            i,
            (i + 0.1),
            (i + 0.1),
            (i + 0.1),
            (i + 0.1),
            i,
            "\"127.0.0." + (i % 256) + "\"",
            "\"keyword" + i + "\"",
            i,
            i,
            (i % Short.MAX_VALUE),
            "\"text" + i + "\"",
            "\"1.2." + i + "\"",
            "\"wildcard" + i + "\""
        );
    }

    private static String createDocumentWithMVs(int i) {
        return format(
            null,
            DOCUMENT_TEMPLATE,
            i,
            repeatValueAsMV((i & 1) == 0),
            repeatValueAsMV(i % 256),
            repeatValueAsMV(i),
            repeatValueAsMV(i + 0.1),
            repeatValueAsMV(i + 0.1),
            repeatValueAsMV(i + 0.1),
            repeatValueAsMV(i + 0.1),
            repeatValueAsMV(i),
            repeatValueAsMV("\"127.0.0." + (i % 256) + "\""),
            repeatValueAsMV("\"keyword" + i + "\""),
            repeatValueAsMV(i),
            repeatValueAsMV(i),
            repeatValueAsMV(i % Short.MAX_VALUE),
            repeatValueAsMV("\"text" + i + "\""),
            repeatValueAsMV("\"1.2." + i + "\""),
            repeatValueAsMV("\"wildcard" + i + "\"")
        );
    }

    private static String createDocumentWithNulls(int i) {
        return format(null, """
                {"index":{"_id":"{}"}}
                {}
            """, i);
    }

    private static String repeatValueAsMV(Object value) {
        return "[" + value + ", " + value + "]";
    }

    private static void createIndex(String indexName, boolean lookupMode, String mapping) throws IOException {
        Request request = new Request("PUT", "/" + indexName);
        String settings = "\"settings\" : {\"mode\" : \"lookup\"}, ";
        request.setJsonEntity("{" + (lookupMode ? settings : "") + mapping + "}");
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
    }

    public static RequestObjectBuilder requestObjectBuilder() throws IOException {
        return new RequestObjectBuilder();
    }

    @After
    public void wipeTestData() throws IOException {
        try {
            var response = client().performRequest(new Request("DELETE", "/" + testIndexName()));
            assertEquals(200, response.getStatusLine().getStatusCode());
        } catch (ResponseException re) {
            assertEquals(404, re.getResponse().getStatusLine().getStatusCode());
        }
    }

    protected static String testIndexName() {
        return TEST_INDEX_NAME;
    }

    protected static String fromIndex() {
        return "from " + testIndexName();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Before
    @After
    public void assertRequestBreakerEmpty() throws Exception {
        EsqlSpecTestCase.assertRequestBreakerEmpty();
    }

    protected void indexTimestampData(int shards) throws IOException {
        Request createIndex = new Request("PUT", testIndexName());
        createIndex.setJsonEntity("""
            {
              "settings": {
                "index": {
                  "number_of_shards": %shards%
                }
              }
            }""".replace("%shards%", Integer.toString(shards)));
        Response response = client().performRequest(createIndex);
        assertThat(
            entityToMap(response.getEntity(), XContentType.JSON),
            matchesMap().entry("shards_acknowledged", true).entry("index", testIndexName()).entry("acknowledged", true)
        );

        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            b.append(String.format(Locale.ROOT, """
                {"create":{"_index":"%s"}}
                {"@timestamp":"2020-12-12","test":"value%s","value":%d}
                """, testIndexName(), i, i));
        }
        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.addParameter("filter_path", "errors");
        bulk.setJsonEntity(b.toString());
        response = client().performRequest(bulk);
        Assert.assertEquals("{\"errors\":false}", EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
    }
}
