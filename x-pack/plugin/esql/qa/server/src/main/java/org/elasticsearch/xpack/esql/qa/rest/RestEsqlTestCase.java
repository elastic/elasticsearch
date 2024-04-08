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
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.Mode.ASYNC;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.Mode.SYNC;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public abstract class RestEsqlTestCase extends ESRestTestCase {

    // Test runner will run multiple suites in parallel, with some of them requiring preserving state between
    // tests (like EsqlSpecTestCase), so test data (like index name) needs not collide and cleanup must be done locally.
    private static final String TEST_INDEX_NAME = "rest-esql-test";

    private static final Logger LOGGER = LogManager.getLogger(RestEsqlTestCase.class);

    private static final List<String> NO_WARNINGS = List.of();

    private static final String MAPPING_ALL_TYPES;

    static {
        String properties = EsqlTestUtils.loadUtf8TextFile("/mapping-all-types.json");
        MAPPING_ALL_TYPES = "{\"mappings\": " + properties + "}";
    }

    private static final String DOCUMENT_TEMPLATE = """
        {"index":{"_id":"{}"}}
        {"boolean": {}, "byte": {}, "date": {}, "double": {}, "float": {}, "half_float": {}, "scaled_float": {}, "integer": {},""" + """
        "ip": {}, "keyword": {}, "long": {}, "unsigned_long": {}, "short": {}, "text": {},""" + """
         "version": {}, "wildcard": {}}
        """;

    // larger than any (unsigned) long
    private static final String HUMONGOUS_DOUBLE = "1E300";
    private static final String INFINITY = "1.0/0.0";
    private static final String NAN = "0.0/0.0";

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

    public static class RequestObjectBuilder {
        private final XContentBuilder builder;
        private boolean isBuilt = false;

        private Boolean keepOnCompletion = null;

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

        public RequestObjectBuilder build() throws IOException {
            if (isBuilt == false) {
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
        Map<String, Object> answer = runEsql(builder().query("row a = 1, b = 2"));
        assertEquals(2, answer.size());
        Map<String, String> colA = Map.of("name", "a", "type", "integer");
        Map<String, String> colB = Map.of("name", "b", "type", "integer");
        assertEquals(List.of(colA, colB), answer.get("columns"));
        assertEquals(List.of(List.of(1, 2)), answer.get("values"));
    }

    public void testUseUnknownIndex() throws IOException {
        ResponseException e = expectThrows(ResponseException.class, () -> runEsql(builder().query("from doesNotExist")));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("verification_exception"));
        assertThat(e.getMessage(), containsString("Unknown index [doesNotExist]"));
    }

    public void testUseKnownIndexWithUnknownIndex() throws IOException {
        // to ignore a concrete non-existent index, we need to opt in (which is not the default)
        useKnownIndexWithOther("noSuchIndex", "ignore_unavailable");
    }

    public void testUseKnownIndexWithUnknownPattern() throws IOException {
        // to not ignore a non-existing index, we need to opt in (which is the default)
        useKnownIndexWithOther("noSuchPattern*", "allow_no_indices");
    }

    private void useKnownIndexWithOther(String other, String option) throws IOException {
        final int count = randomIntBetween(1, 10);
        bulkLoadTestData(count);

        CheckedFunction<Boolean, RequestObjectBuilder, IOException> builder = o -> {
            String q = fromIndex() + ',' + other;
            q += " OPTIONS \"" + option + "\"=\"" + o + "\"";
            q += " | KEEP keyword, integer | SORT integer asc | LIMIT 10";
            return builder().query(q);
        };

        // test failure
        ResponseException e = expectThrows(ResponseException.class, () -> runEsql(builder.apply(false)));
        assertEquals(404, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("no such index [" + other + "]"));

        // test success
        assertEquals(expectedTextBody("txt", count, null), runEsqlAsTextWithFormat(builder.apply(true), "txt", null));
    }

    // https://github.com/elastic/elasticsearch/issues/106805
    public void testUseUnknownIndexOnly() {
        useUnknownIndex("ignore_unavailable");
        useUnknownIndex("allow_no_indices");
    }

    private void useUnknownIndex(String option) {
        CheckedFunction<Boolean, RequestObjectBuilder, IOException> builder = o -> {
            String q = "FROM doesnotexist OPTIONS \"" + option + "\"=\"" + o + "\"";
            q += " | KEEP keyword, integer | SORT integer asc | LIMIT 10";
            return builder().query(q);
        };

        // test failure 404 from resolver
        ResponseException e = expectThrows(ResponseException.class, () -> runEsql(builder.apply(false)));
        assertEquals(404, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("index_not_found_exception"));
        assertThat(e.getMessage(), containsString("no such index [doesnotexist]"));

        // test failure 400 from verifier
        e = expectThrows(ResponseException.class, () -> runEsql(builder.apply(true)));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("verification_exception"));
        assertThat(e.getMessage(), containsString("Unknown index [doesnotexist]"));

    }

    public void testSearchPreference() throws IOException {
        final int count = randomIntBetween(1, 10);
        bulkLoadTestData(count);

        CheckedFunction<String, RequestObjectBuilder, IOException> builder = o -> {
            String q = fromIndex();
            if (Strings.hasText(o)) {
                q += " OPTIONS " + o;
            }
            q += " | KEEP keyword, integer | SORT integer asc | LIMIT 10";
            return builder().query(q);
        };

        // verify that it returns as expected
        assertEquals(expectedTextBody("txt", count, null), runEsqlAsTextWithFormat(builder.apply(null), "txt", null));

        // returns nothing (0 for count), given the non-existing shard as preference
        String option = "\"preference\"=\"_shards:666\"";
        assertEquals(expectedTextBody("txt", 0, null), runEsqlAsTextWithFormat(builder.apply(option), "txt", null));
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

        RequestObjectBuilder builder = new RequestObjectBuilder().query(fromIndex() + " | stats min(value)");
        Map<String, Object> result = runEsql(builder);
        assertMap(
            result,
            matchesMap().entry("values", List.of(List.of(1))).entry("columns", List.of(Map.of("name", "min(value)", "type", "long")))
        );

        builder = new RequestObjectBuilder().query(fromIndex() + " | stats min(value) by group");
        result = runEsql(builder);
        assertMap(
            result,
            matchesMap().entry("values", List.of(List.of(2, 0), List.of(1, 1)))
                .entry("columns", List.of(Map.of("name", "min(value)", "type", "long"), Map.of("name", "group", "type", "long")))
        );
    }

    public void testColumnarMode() throws IOException {
        int docCount = randomIntBetween(3, 10);
        bulkLoadTestData(docCount);

        boolean columnar = randomBoolean();
        var query = builder().query(fromIndex() + " | keep keyword, integer | sort integer asc");
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
        var builder = builder().query(fromIndex() + " | keep keyword, integer | sort integer asc | limit 100");
        assertEquals(expectedTextBody("txt", count, null), runEsqlAsTextWithFormat(builder, "txt", null));
    }

    public void testCSVMode() throws IOException {
        int count = randomIntBetween(0, 100);
        bulkLoadTestData(count);
        var builder = builder().query(fromIndex() + " | keep keyword, integer | sort integer asc | limit 100");
        assertEquals(expectedTextBody("csv", count, '|'), runEsqlAsTextWithFormat(builder, "csv", '|'));
    }

    public void testTSVMode() throws IOException {
        int count = randomIntBetween(0, 100);
        bulkLoadTestData(count);
        var builder = builder().query(fromIndex() + " | keep keyword, integer | sort integer asc | limit 100");
        assertEquals(expectedTextBody("tsv", count, null), runEsqlAsTextWithFormat(builder, "tsv", null));
    }

    public void testCSVNoHeaderMode() throws IOException {
        bulkLoadTestData(1);
        var builder = builder().query(fromIndex() + " | keep keyword, integer | sort integer asc | limit 100");
        Request request = prepareRequest(SYNC);
        String mediaType = attachBody(builder.build(), request);
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Content-Type", mediaType);
        options.addHeader("Accept", "text/csv; header=absent");
        request.setOptions(options);
        HttpEntity entity = performRequest(request, List.of());
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
            // TODO: https://github.com/elastic/elasticsearch/issues/100130
            // "half_float",
            // "float",
            "double",
            "scaled_float"
        );

        String lessOrLessEqual = randomFrom(" < ", " <= ");
        String largerOrLargerEqual = randomFrom(" > ", " >= ");
        String inEqualPlusMinus = randomFrom(" != ", " != -");
        String equalPlusMinus = randomFrom(" == ", " == -");
        // TODO: once we do not support infinity and NaN anymore, remove INFINITY/NAN cases.
        // https://github.com/elastic/elasticsearch/issues/98698#issuecomment-1847423390
        String humongousPositiveLiteral = randomFrom(HUMONGOUS_DOUBLE, INFINITY);
        String nanOrNull = randomFrom(NAN, "to_double(null)");

        List<String> trueForSingleValuesPredicates = List.of(
            lessOrLessEqual + humongousPositiveLiteral,
            largerOrLargerEqual + " -" + humongousPositiveLiteral,
            inEqualPlusMinus + humongousPositiveLiteral,
            inEqualPlusMinus + NAN
        );
        List<String> alwaysFalsePredicates = List.of(
            lessOrLessEqual + " -" + humongousPositiveLiteral,
            largerOrLargerEqual + humongousPositiveLiteral,
            equalPlusMinus + humongousPositiveLiteral,
            lessOrLessEqual + nanOrNull,
            largerOrLargerEqual + nanOrNull,
            equalPlusMinus + nanOrNull,
            inEqualPlusMinus + "to_double(null)"
        );

        for (String fieldWithType : dataTypes) {
            for (String truePredicate : trueForSingleValuesPredicates) {
                String comparison = fieldWithType + truePredicate;
                var query = builder().query(format(null, "from {} | where {}", testIndexName(), comparison));
                List<String> expectedWarnings = List.of(
                    "Line 1:29: evaluation of [" + comparison + "] failed, treating result as null. Only first 20 failures recorded.",
                    "Line 1:29: java.lang.IllegalArgumentException: single-value function encountered multi-value"
                );
                var result = runEsql(query, expectedWarnings, mode);

                var values = as(result.get("values"), ArrayList.class);
                assertThat(
                    format(null, "Comparison [{}] should return all rows with single values.", comparison),
                    values.size(),
                    is(NUM_SINGLE_VALUE_ROWS)
                );
            }

            for (String falsePredicate : alwaysFalsePredicates) {
                String comparison = fieldWithType + falsePredicate;
                var query = builder().query(format(null, "from {} | where {}", testIndexName(), comparison));
                var result = runEsql(query);

                var values = as(result.get("values"), ArrayList.class);
                assertThat(format(null, "Comparison [{}] should return no rows.", comparison), values.size(), is(0));
            }
        }
    }

    public void testWarningHeadersOnFailedConversions() throws IOException {
        int count = randomFrom(10, 40, 60);
        bulkLoadTestData(count);

        Request request = prepareRequest(SYNC);
        var query = fromIndex() + " | eval asInt = to_int(case(integer % 2 == 0, to_str(integer), keyword)) | limit 1000";
        var mediaType = attachBody(new RequestObjectBuilder().query(query).build(), request);

        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.setWarningsHandler(WarningsHandler.PERMISSIVE);
        options.addHeader("Content-Type", mediaType);
        options.addHeader("Accept", mediaType);

        request.setOptions(options);
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        int expectedWarnings = Math.min(count / 2, 20);
        var warnings = response.getWarnings();
        assertThat(warnings.size(), is(1 + expectedWarnings));
        var firstHeader = "Line 1:36: evaluation of [to_int(case(integer %25 2 == 0, to_str(integer), keyword))] failed, "
            + "treating result as null. Only first 20 failures recorded.";
        assertThat(warnings.get(0), containsString(firstHeader));
        for (int i = 1; i <= expectedWarnings; i++) {
            assertThat(
                warnings.get(i),
                containsString("org.elasticsearch.xpack.ql.InvalidArgumentException: Cannot parse number [keyword" + (2 * i - 1) + "]")
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
        Map<String, Object> result = runEsql(new RequestObjectBuilder().query(query));
        var columns = List.of(
            Map.of("name", "a", "type", "long"),
            Map.of("name", "_index", "type", "keyword"),
            Map.of("name", "_version", "type", "long"),
            Map.of("name", "_id", "type", "keyword")
        );
        var values = List.of(List.of(3, testIndexName() + "-2", 1, "id-2"), List.of(2, testIndexName() + "-1", 2, "id-1"));

        assertMap(result, matchesMap().entry("columns", columns).entry("values", values));

        assertThat(deleteIndex(testIndexName() + "-1").isAcknowledged(), is(true)); // clean up
        assertThat(deleteIndex(testIndexName() + "-2").isAcknowledged(), is(true)); // clean up
    }

    public void testErrorMessageForEmptyParams() throws IOException {
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsql(new RequestObjectBuilder().query("row a = 1 | eval x = ?").params("[]"))
        );
        assertThat(EntityUtils.toString(re.getResponse().getEntity()), containsString("Not enough actual parameters 0"));
    }

    public void testErrorMessageForInvalidParams() throws IOException {
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsql(new RequestObjectBuilder().query("row a = 1").params("[{\"x\":\"y\"}]"))
        );
        assertThat(EntityUtils.toString(re.getResponse().getEntity()), containsString("Required [value, type]"));
    }

    public void testErrorMessageForMissingTypeInParams() throws IOException {
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsql(new RequestObjectBuilder().query("row a = 1").params("[\"x\", 123, true, {\"value\": \"y\"}]"))
        );
        assertThat(EntityUtils.toString(re.getResponse().getEntity()), containsString("Required [type]"));
    }

    public void testErrorMessageForMissingValueInParams() throws IOException {
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsql(new RequestObjectBuilder().query("row a = 1").params("[\"x\", 123, true, {\"type\": \"y\"}]"))
        );
        assertThat(EntityUtils.toString(re.getResponse().getEntity()), containsString("Required [value]"));
    }

    public void testErrorMessageForInvalidTypeInParams() throws IOException {
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsqlSync(new RequestObjectBuilder().query("row a = 1 | eval x = ?").params("[{\"type\": \"byte\", \"value\": 5}]"))
        );
        assertThat(
            EntityUtils.toString(re.getResponse().getEntity()),
            containsString("EVAL does not support type [byte] in expression [?]")
        );
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
            () -> runEsql(new RequestObjectBuilder().query("row a = 1 | eval x = now() + (" + dateMathString + ")"))
        );

        String responseMessage = EntityUtils.toString(re.getResponse().getEntity());
        // the error in the response message might be chopped up by newlines, but finding "overflow" should suffice.
        assertThat(responseMessage, containsString(errorSubstring));

        assertThat(re.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testErrorMessageForArrayValuesInParams() throws IOException {
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsql(
                new RequestObjectBuilder().query("row a = 1 | eval x = ?").params("[{\"type\": \"integer\", \"value\": [5, 6, 7]}]")
            )
        );
        assertThat(
            EntityUtils.toString(re.getResponse().getEntity()),
            containsString("[params] value doesn't support values of type: START_ARRAY")
        );
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
        return runEsql(requestObject, NO_WARNINGS, mode);
    }

    public static Map<String, Object> runEsqlSync(RequestObjectBuilder requestObject) throws IOException {
        return runEsqlSync(requestObject, NO_WARNINGS);
    }

    static Map<String, Object> runEsql(RequestObjectBuilder requestObject, List<String> expectedWarnings, Mode mode) throws IOException {
        if (mode == ASYNC) {
            return runEsqlAsync(requestObject, expectedWarnings);
        } else {
            return runEsqlSync(requestObject, expectedWarnings);
        }
    }

    public static Map<String, Object> runEsqlSync(RequestObjectBuilder requestObject, List<String> expectedWarnings) throws IOException {
        requestObject.build();
        Request request = prepareRequest(SYNC);
        String mediaType = attachBody(requestObject, request);

        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.setWarningsHandler(WarningsHandler.PERMISSIVE); // We assert the warnings ourselves
        options.addHeader("Content-Type", mediaType);

        if (randomBoolean()) {
            options.addHeader("Accept", mediaType);
        } else {
            request.addParameter("format", requestObject.contentType().queryParameter());
        }
        request.setOptions(options);

        HttpEntity entity = performRequest(request, expectedWarnings);
        return entityToMap(entity, requestObject.contentType());
    }

    public static Map<String, Object> runEsqlAsync(RequestObjectBuilder requestObject, List<String> expectedWarnings) throws IOException {
        addAsyncParameters(requestObject);
        requestObject.build();
        Request request = prepareRequest(ASYNC);
        String mediaType = attachBody(requestObject, request);

        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.setWarningsHandler(WarningsHandler.PERMISSIVE); // We assert the warnings ourselves
        options.addHeader("Content-Type", mediaType);

        if (randomBoolean()) {
            options.addHeader("Accept", mediaType);
        } else {
            request.addParameter("format", requestObject.contentType().queryParameter());
        }
        request.setOptions(options);

        if (shouldLog()) {
            LOGGER.info("REQUEST={}", request);
        }

        Response response = performRequest(request);
        HttpEntity entity = response.getEntity();

        Object initialColumns = null;
        Object initialValues = null;
        var json = entityToMap(entity, requestObject.contentType());
        checkKeepOnCompletion(requestObject, json);
        String id = (String) json.get("id");

        if (id == null) {
            // no id returned from an async call, must have completed immediately and without keep_on_completion
            assertThat(requestObject.keepOnCompletion(), either(nullValue()).or(is(false)));
            assertThat((boolean) json.get("is_running"), is(false));
            assertWarnings(response, expectedWarnings);
            json.remove("is_running"); // remove this to not mess up later map assertions
            return Collections.unmodifiableMap(json);
        } else {
            // async may not return results immediately, so may need an async get
            assertThat(id, is(not(emptyOrNullString())));
            if ((boolean) json.get("is_running") == false) {
                // must have completed immediately so keep_on_completion must be true
                assertThat(requestObject.keepOnCompletion(), is(true));
                assertWarnings(response, expectedWarnings);
                // we already have the results, but let's remember them so that we can compare to async get
                initialColumns = json.get("columns");
                initialValues = json.get("values");
            } else {
                // did not return results immediately, so we will need an async get
                assertThat(json.get("columns"), is(equalTo(List.<Map<String, String>>of()))); // no partial results
                assertThat(json.get("pages"), nullValue());
            }
            // issue a second request to "async get" the results
            Request getRequest = prepareAsyncGetRequest(id);
            getRequest.setOptions(options);
            response = performRequest(getRequest);
            entity = response.getEntity();
        }

        var result = entityToMap(entity, requestObject.contentType());

        // assert initial contents, if any, are the same as async get contents
        if (initialColumns != null) {
            assertEquals(initialColumns, result.get("columns"));
            assertEquals(initialValues, result.get("values"));
        }

        assertWarnings(response, expectedWarnings);
        assertDeletable(id);
        return removeAsyncProperties(result);
    }

    // Removes async properties, otherwise consuming assertions would need to handle sync and async differences
    static Map<String, Object> removeAsyncProperties(Map<String, Object> map) {
        Map<String, Object> copy = new HashMap<>(map);
        assertFalse((boolean) copy.remove("is_running"));
        copy.remove("id"); // id is optional, do not assert its removal
        return Collections.unmodifiableMap(copy);
    }

    static Map<String, Object> entityToMap(HttpEntity entity, XContentType expectedContentType) throws IOException {
        try (InputStream content = entity.getContent()) {
            XContentType xContentType = XContentType.fromMediaType(entity.getContentType().getValue());
            assertEquals(expectedContentType, xContentType);
            var map = XContentHelper.convertToMap(xContentType.xContent(), content, false);
            if (shouldLog()) {
                LOGGER.info("entity={}", map);
            }
            return map;
        }
    }

    static void addAsyncParameters(RequestObjectBuilder requestObject) throws IOException {
        // deliberately short in order to frequently trigger return without results
        requestObject.waitForCompletion(TimeValue.timeValueNanos(randomIntBetween(1, 100)));
        requestObject.keepOnCompletion(randomBoolean());
        requestObject.keepAlive(TimeValue.timeValueDays(randomIntBetween(1, 10)));
    }

    // If keep_on_completion is set then an id must always be present, regardless of the value of any other property.
    static void checkKeepOnCompletion(RequestObjectBuilder requestObject, Map<String, Object> json) {
        if (requestObject.keepOnCompletion()) {
            assertThat((String) json.get("id"), not(emptyOrNullString()));
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

    static String runEsqlAsTextWithFormat(RequestObjectBuilder builder, String format, @Nullable Character delimiter) throws IOException {
        Request request = prepareRequest(SYNC);
        String mediaType = attachBody(builder.build(), request);

        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Content-Type", mediaType);

        if (randomBoolean()) {
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

        HttpEntity entity = performRequest(request, List.of());
        return Streams.copyToString(new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8));
    }

    private static Request prepareRequest(Mode mode) {
        Request request = new Request("POST", "/_query" + (mode == ASYNC ? "/async" : ""));
        request.addParameter("error_trace", "true");   // Helps with debugging in case something crazy happens on the server.
        request.addParameter("pretty", "true");        // Improves error reporting readability
        return request;
    }

    private static Request prepareAsyncGetRequest(String id) {
        Request request = new Request("GET", "/_query/async/" + id + "?wait_for_completion_timeout=60s");
        request.addParameter("error_trace", "true");   // Helps with debugging in case something crazy happens on the server.
        request.addParameter("pretty", "true");        // Improves error reporting readability
        return request;
    }

    private static Request prepareAsyncDeleteRequest(String id) {
        Request request = new Request("DELETE", "/_query/async/" + id);
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

    private static HttpEntity performRequest(Request request, List<String> allowedWarnings) throws IOException {
        return assertWarnings(performRequest(request), allowedWarnings);
    }

    private static Response performRequest(Request request) throws IOException {
        Response response = client().performRequest(request);
        if (shouldLog()) {
            LOGGER.info("RESPONSE={}", response);
            LOGGER.info("RESPONSE headers={}", Arrays.toString(response.getHeaders()));
        }
        assertEquals(200, response.getStatusLine().getStatusCode());
        return response;
    }

    private static HttpEntity assertWarnings(Response response, List<String> allowedWarnings) {
        List<String> warnings = new ArrayList<>(response.getWarnings());
        warnings.removeAll(mutedWarnings());
        if (shouldLog()) {
            LOGGER.info("RESPONSE warnings (after muted)={}", warnings);
        }
        assertMap(warnings, matchesList(allowedWarnings));
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

    private static void bulkLoadTestData(int count, int firstIndex, boolean createIndex, IntFunction<String> createDocument)
        throws IOException {
        Request request;
        if (createIndex) {
            request = new Request("PUT", "/" + testIndexName());
            request.setJsonEntity(MAPPING_ALL_TYPES);
            assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
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

    private static RequestObjectBuilder builder() throws IOException {
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
}
