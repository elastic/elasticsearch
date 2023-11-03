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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RestEsqlTestCase extends ESRestTestCase {

    // Test runner will run multiple suites in parallel, with some of them requiring preserving state between
    // tests (like EsqlSpecTestCase), so test data (like index name) needs not collide and cleanup must be done locally.
    private static final String TEST_INDEX_NAME = "rest-esql-test";

    public static class RequestObjectBuilder {
        private final XContentBuilder builder;
        private boolean isBuilt = false;

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
        Map<String, Object> answer = runEsql(builder().query("row a = 1, b = 2").build());
        assertEquals(2, answer.size());
        Map<String, String> colA = Map.of("name", "a", "type", "integer");
        Map<String, String> colB = Map.of("name", "b", "type", "integer");
        assertEquals(List.of(colA, colB), answer.get("columns"));
        assertEquals(List.of(List.of(1, 2)), answer.get("values"));
    }

    public void testUseUnknownIndex() throws IOException {
        ResponseException e = expectThrows(ResponseException.class, () -> runEsql(builder().query("from doesNotExist").build()));
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

        RequestObjectBuilder builder = new RequestObjectBuilder().query(fromIndex() + " | stats min(value)");
        Map<String, Object> result = runEsql(builder.build());
        assertMap(
            result,
            matchesMap().entry("values", List.of(List.of(1))).entry("columns", List.of(Map.of("name", "min(value)", "type", "long")))
        );

        builder = new RequestObjectBuilder().query(fromIndex() + " | stats min(value) by group");
        result = runEsql(builder.build());
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
        var query = builder().query(fromIndex() + " | keep keyword, integer");
        if (columnar || randomBoolean()) {
            query.columnar(columnar);
        }
        Map<String, Object> answer = runEsql(query.build());

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
        var builder = builder().query(fromIndex() + " | keep keyword, integer | limit 100").build();
        assertEquals(expectedTextBody("txt", count, null), runEsqlAsTextWithFormat(builder, "txt", null));
    }

    public void testCSVMode() throws IOException {
        int count = randomIntBetween(0, 100);
        bulkLoadTestData(count);
        var builder = builder().query(fromIndex() + " | keep keyword, integer | limit 100").build();
        assertEquals(expectedTextBody("csv", count, '|'), runEsqlAsTextWithFormat(builder, "csv", '|'));
    }

    public void testTSVMode() throws IOException {
        int count = randomIntBetween(0, 100);
        bulkLoadTestData(count);
        var builder = builder().query(fromIndex() + " | keep keyword, integer | limit 100").build();
        assertEquals(expectedTextBody("tsv", count, null), runEsqlAsTextWithFormat(builder, "tsv", null));
    }

    public void testCSVNoHeaderMode() throws IOException {
        bulkLoadTestData(1);
        var builder = builder().query(fromIndex() + " | keep keyword, integer | limit 100").build();
        Request request = prepareRequest();
        String mediaType = attachBody(builder, request);
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Content-Type", mediaType);
        options.addHeader("Accept", "text/csv; header=absent");
        request.setOptions(options);
        HttpEntity entity = performRequest(request, List.of());
        String actual = Streams.copyToString(new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8));
        assertEquals("keyword0,0\r\n", actual);
    }

    public void testWarningHeadersOnFailedConversions() throws IOException {
        int count = randomFrom(10, 40, 60);
        bulkLoadTestData(count);

        Request request = prepareRequest();
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
                containsString("java.lang.NumberFormatException: For input string: \\\"keyword" + (2 * i - 1) + "\\\"")
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

        var query = fromIndex() + "* [metadata _index, _version, _id] | sort _version";
        Map<String, Object> result = runEsql(new RequestObjectBuilder().query(query).build());
        var columns = List.of(
            Map.of("name", "a", "type", "long"),
            Map.of("name", "_index", "type", "keyword"),
            Map.of("name", "_version", "type", "long"),
            Map.of("name", "_id", "type", "keyword")
        );
        var values = List.of(List.of(3, testIndexName() + "-2", 1, "id-2"), List.of(2, testIndexName() + "-1", 2, "id-1"));

        assertMap(result, matchesMap().entry("columns", columns).entry("values", values));
    }

    public void testErrorMessageForEmptyParams() throws IOException {
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsql(new RequestObjectBuilder().query("row a = 1 | eval x = ?").params("[]").build())
        );
        assertThat(EntityUtils.toString(re.getResponse().getEntity()), containsString("Not enough actual parameters 0"));
    }

    public void testErrorMessageForInvalidParams() throws IOException {
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsql(new RequestObjectBuilder().query("row a = 1").params("[{\"x\":\"y\"}]").build())
        );
        assertThat(EntityUtils.toString(re.getResponse().getEntity()), containsString("Required [value, type]"));
    }

    public void testErrorMessageForMissingTypeInParams() throws IOException {
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsql(new RequestObjectBuilder().query("row a = 1").params("[\"x\", 123, true, {\"value\": \"y\"}]").build())
        );
        assertThat(EntityUtils.toString(re.getResponse().getEntity()), containsString("Required [type]"));
    }

    public void testErrorMessageForMissingValueInParams() throws IOException {
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsql(new RequestObjectBuilder().query("row a = 1").params("[\"x\", 123, true, {\"type\": \"y\"}]").build())
        );
        assertThat(EntityUtils.toString(re.getResponse().getEntity()), containsString("Required [value]"));
    }

    public void testErrorMessageForInvalidTypeInParams() throws IOException {
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsql(new RequestObjectBuilder().query("row a = 1 | eval x = ?").params("[{\"type\": \"byte\", \"value\": 5}]").build())
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

    private static void assertExceptionForDateMath(String dateMathString, String errorSubstring) throws IOException {
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsql(new RequestObjectBuilder().query("row a = 1 | eval x = now() + (" + dateMathString + ")").build())
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
                new RequestObjectBuilder().query("row a = 1 | eval x = ?").params("[{\"type\": \"integer\", \"value\": [5, 6, 7]}]").build()
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

    public static Map<String, Object> runEsql(RequestObjectBuilder requestObject) throws IOException {
        return runEsql(requestObject, List.of());
    }

    public static Map<String, Object> runEsql(RequestObjectBuilder requestObject, List<String> expectedWarnings) throws IOException {
        Request request = prepareRequest();
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
        try (InputStream content = entity.getContent()) {
            XContentType xContentType = XContentType.fromMediaType(entity.getContentType().getValue());
            assertEquals(requestObject.contentType(), xContentType);
            return XContentHelper.convertToMap(xContentType.xContent(), content, false);
        }
    }

    static String runEsqlAsTextWithFormat(RequestObjectBuilder builder, String format, @Nullable Character delimiter) throws IOException {
        Request request = prepareRequest();
        String mediaType = attachBody(builder, request);

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

    private static Request prepareRequest() {
        Request request = new Request("POST", "/_query");
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
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
        List<String> warnings = new ArrayList<>(response.getWarnings());
        warnings.removeAll(mutedWarnings());
        assertMap(warnings, matchesList(allowedWarnings));
        return response.getEntity();
    }

    private static Set<String> mutedWarnings() {
        return Set.of("No limit defined, adding default limit of [500]");
    }

    private static void bulkLoadTestData(int count) throws IOException {
        Request request = new Request("PUT", "/" + testIndexName());
        request.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "keyword": {
                    "type": "keyword"
                  },
                  "integer": {
                    "type": "integer"
                  }
                }
              }
            }""");
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        if (count > 0) {
            request = new Request("POST", "/" + testIndexName() + "/_bulk");
            request.addParameter("refresh", "true");
            StringBuilder bulk = new StringBuilder();
            for (int i = 0; i < count; i++) {
                bulk.append(org.elasticsearch.core.Strings.format("""
                    {"index":{"_id":"%s"}}
                    {"keyword":"keyword%s", "integer":%s}
                    """, i, i, i));
            }
            request.setJsonEntity(bulk.toString());
            assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
        }
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
