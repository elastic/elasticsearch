/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.logs;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.action.DocWriteRequest.OpType.CREATE;
import static org.mockito.Mockito.when;

public class RestLogsActionTests extends RestActionTestCase {

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestLogsAction());
    }

    public void testIngestJsonLogs() {
        RestRequest req = createLogsRequest("/_logs", Map.of("message", "Hello World"), Map.of("foo", "bar"));

        verifyingClient.setExecuteVerifier((BiFunction<ActionType<BulkResponse>, BulkRequest, BulkResponse>) (actionType, request) -> {
            assertEquals(2, request.requests().size());
            IndexRequest indexRequest = (IndexRequest) request.requests().get(0);
            assertDataStreamFields("generic", "default", indexRequest);
            assertEquals(CREATE, indexRequest.opType());
            assertEquals("Hello World", ((IndexRequest) request.requests().get(0)).sourceAsMap().get("message"));
            assertEquals("bar", ((IndexRequest) request.requests().get(1)).sourceAsMap().get("foo"));
            return Mockito.mock(BulkResponse.class);
        });
        assertEquals(0, dispatchRequest(req).errors().get());
    }

    public void testMultilineJsonLogs() {
        RestRequest req = createLogsRequest("/_logs", """
            {
              "message": "Hello World"
            }
            {
              "foo": "bar"
            }""");

        verifyingClient.setExecuteVerifier((BiFunction<ActionType<BulkResponse>, BulkRequest, BulkResponse>) (actionType, request) -> {
            assertEquals(2, request.requests().size());
            IndexRequest indexRequest = (IndexRequest) request.requests().get(0);
            assertDataStreamFields("generic", "default", indexRequest);
            assertEquals(CREATE, indexRequest.opType());
            assertEquals("Hello World", ((IndexRequest) request.requests().get(0)).sourceAsMap().get("message"));
            assertEquals("bar", ((IndexRequest) request.requests().get(1)).sourceAsMap().get("foo"));
            return Mockito.mock(BulkResponse.class);
        });
        assertEquals(0, dispatchRequest(req).errors().get());
    }

    public void testMultilineRawLogs_first_line_pattern() {
        testRawEvents(Map.of("_multiline.first_line_pattern", "^[A-Z]"), "INFO foo\n  bar", "INFO baz", "INFO qux");
    }

    public void testMultilineRawLogs_first_line_pattern_negate() {
        testRawEvents(
            Map.of("_multiline.first_line_pattern", "^\\s", "_multiline.negate", "true"),
            "INFO foo\n  bar",
            "INFO baz",
            "INFO qux"
        );
    }

    public void testMultilineRawLogs_last_line_pattern() {
        testRawEvents(Map.of("_multiline.last_line_pattern", ">$"), "<INFO foo\n  bar>", "<INFO baz>", "<INFO qux>");
    }

    public void testMultilineRawLogs_first_line_pattern_and_last_line_pattern() {
        testRawEvents(
            Map.of("_multiline.first_line_pattern", "^<", "_multiline.last_line_pattern", ">$"),
            "<INFO foo\n  bar>",
            "INFO baz\nqux",
            "<INFO quux>"
        );
    }

    public void testMultilineRawLogs_last_line_pattern_negate() {
        testRawEvents(
            Map.of("_multiline.last_line_pattern", "\\\\$", "_multiline.negate", "true"),
            "INFO foo\\\n  bar",
            "INFO baz",
            "INFO qux"
        );
    }

    public void testRawEvents(Map<String, String> params, String... events) {
        RestRequest req = createLogsRequest("/_logs", String.join("\n", events), params);

        verifyingClient.setExecuteVerifier((BiFunction<ActionType<BulkResponse>, BulkRequest, BulkResponse>) (actionType, request) -> {
            assertEquals(events.length, request.requests().size());
            IndexRequest indexRequest = (IndexRequest) request.requests().get(0);
            assertDataStreamFields("generic", "default", indexRequest);
            assertEquals(CREATE, indexRequest.opType());
            for (int i = 0; i < events.length; i++) {
                assertEquals(events[i], ((IndexRequest) request.requests().get(i)).sourceAsMap().get("message"));
            }
            return Mockito.mock(BulkResponse.class);
        });
        assertEquals(0, dispatchRequest(req).errors().get());
    }

    public void testInvalidJson() {
        RestRequest req = createLogsRequest("/_logs/foo", "{\"message\": \"missing end quote}");
        verifyingClient.setExecuteVerifier((BiFunction<ActionType<BulkResponse>, BulkRequest, BulkResponse>) (actionType, request) -> {
            assertEquals(1, request.requests().size());
            IndexRequest indexRequest = (IndexRequest) request.requests().get(0);
            assertDataStreamFields("foo", "default", indexRequest);
            Map<String, Object> doc = ((IndexRequest) request.requests().get(0)).sourceAsMap();
            assertEquals("{\"message\": \"missing end quote}", getPath(doc, "event.original"));
            assertEquals("json_e_o_f_exception", getPath(doc, "ingest.error.type"));
            return Mockito.mock(BulkResponse.class);
        });
        assertEquals(0, dispatchRequest(req).errors().get());
    }

    public void testMetadata() {
        RestRequest req = createLogsRequest(
            "/_logs",
            Map.of("_metadata", Map.of("global", true)),
            Map.of("_metadata", Map.of("local1", true)),
            Map.of("foo", "bar"),
            Map.of("_metadata", Map.of("local2", true)),
            Map.of("foo", "bar")
        );
        verifyingClient.setExecuteVerifier((BiFunction<ActionType<BulkResponse>, BulkRequest, BulkResponse>) (actionType, request) -> {
            assertEquals(2, request.requests().size());
            Map<String, Object> doc1 = ((IndexRequest) request.requests().get(0)).sourceAsMap();
            Map<String, Object> doc2 = ((IndexRequest) request.requests().get(1)).sourceAsMap();
            assertEquals(true, doc1.get("global"));
            assertEquals(true, doc1.get("local1"));
            assertEquals(true, doc1.get("global"));
            assertEquals(true, doc2.get("local2"));
            return Mockito.mock(BulkResponse.class);
        });
        assertEquals(0, dispatchRequest(req).errors().get());
    }

    public void testMergeGlobalAndEventMetadata() {
        RestRequest req = createLogsRequest("/_logs/foo", Map.of("data_stream.namespace", "bar"));
        verifyingClient.setExecuteVerifier((BiFunction<ActionType<BulkResponse>, BulkRequest, BulkResponse>) (actionType, request) -> {
            assertEquals(1, request.requests().size());
            Map<String, Object> doc = ((IndexRequest) request.requests().get(0)).sourceAsMap();
            assertEquals("foo", getPath(doc, "data_stream.dataset"));
            assertEquals("bar", getPath(doc, "data_stream.namespace"));
            return Mockito.mock(BulkResponse.class);
        });
        assertEquals(0, dispatchRequest(req).errors().get());
    }

    private void assertDataStreamFields(String dataset, String namespace, DocWriteRequest<?> docWriteRequest) {
        IndexRequest indexRequest = (IndexRequest) docWriteRequest;
        assertEquals("logs-" + dataset + "-" + namespace, indexRequest.index());
        assertEquals(Map.of("type", "logs", "dataset", dataset, "namespace", namespace), indexRequest.sourceAsMap().get("data_stream"));
    }

    public void testPathMetadata() {
        RestRequest req = createLogsRequest("/_logs/foo/bar", Map.of("message", "Hello World"));
        verifyingClient.setExecuteVerifier((BiFunction<ActionType<BulkResponse>, BulkRequest, BulkResponse>) (actionType, request) -> {
            assertEquals(1, request.requests().size());
            assertDataStreamFields("foo", "bar", request.requests().get(0));
            return Mockito.mock(BulkResponse.class);
        });
        assertEquals(0, dispatchRequest(req).errors().get());
    }

    public void testRetryInGenericDataStreamOnMappingError() {
        RestRequest req = createLogsRequest("/_logs/foo/bar", Map.of("message", "Hello World"));
        AtomicBoolean firstRequest = new AtomicBoolean(true);
        verifyingClient.setExecuteVerifier((BiFunction<ActionType<BulkResponse>, BulkRequest, BulkResponse>) (actionType, request) -> {
            if (firstRequest.get()) {
                firstRequest.set(false);
                assertEquals(1, request.requests().size());
                assertDataStreamFields("foo", "bar", request.requests().get(0));
                return createMockBulkFailureResponse(new MapperParsingException("bad foo"));
            } else {
                assertEquals(1, request.requests().size());
                IndexRequest indexRequest = (IndexRequest) request.requests().get(0);
                assertDataStreamFields("generic", "bar", indexRequest);
                Map<String, Object> doc = indexRequest.sourceAsMap();
                assertEquals("mapper_parsing_exception", getPath(doc, "ingest.error.type"));
                assertEquals("bad foo", getPath(doc, "ingest.error.message"));
                assertEquals("logs", getPath(doc, "ingest.error.data_stream.type"));
                assertEquals("foo", getPath(doc, "ingest.error.data_stream.dataset"));
                assertEquals("bar", getPath(doc, "ingest.error.data_stream.namespace"));
                return Mockito.mock(BulkResponse.class);
            }
        });
        assertEquals(0, dispatchRequest(req).errors().get());
    }

    public void testRetryInSameDataStreamOnTransientError() {
        RestRequest req = createLogsRequest("/_logs/foo", Map.of("message", "Hello World"));
        AtomicBoolean firstRequest = new AtomicBoolean(true);
        verifyingClient.setExecuteVerifier((BiFunction<ActionType<BulkResponse>, BulkRequest, BulkResponse>) (actionType, request) -> {
            if (firstRequest.get()) {
                firstRequest.set(false);
                assertEquals(1, request.requests().size());
                assertDataStreamFields("foo", "default", request.requests().get(0));
                return createMockBulkFailureResponse(new EsRejectedExecutionException());
            } else {
                assertEquals(1, request.requests().size());
                IndexRequest indexRequest = (IndexRequest) request.requests().get(0);
                assertDataStreamFields("foo", "default", indexRequest);
                Map<String, Object> doc = indexRequest.sourceAsMap();
                assertNull(doc.get("_logs"));
                return Mockito.mock(BulkResponse.class);
            }
        });
        assertEquals(0, dispatchRequest(req).errors().get());
    }

    public void testReturnErrorWhenRetryFails() {
        RestRequest req = createLogsRequest("/_logs/foo", Map.of("message", "Hello World"));
        verifyingClient.setExecuteVerifier(
            (BiFunction<ActionType<BulkResponse>, BulkRequest, BulkResponse>) (actionType, request) -> createMockBulkFailureResponse(
                new EsRejectedExecutionException()
            )
        );
        assertEquals(1, dispatchRequest(req).errors().get());
    }

    private BulkResponse createMockBulkFailureResponse(Exception exception) {
        BulkResponse bulkResponse = Mockito.mock(BulkResponse.class);
        when(bulkResponse.hasFailures()).thenReturn(true);

        BulkItemResponse bulkItemResponse = Mockito.mock(BulkItemResponse.class);
        when(bulkItemResponse.getItemId()).thenReturn(0);
        when(bulkItemResponse.isFailed()).thenReturn(true);
        when(bulkResponse.getItems()).thenReturn(new BulkItemResponse[] { bulkItemResponse });

        BulkItemResponse.Failure failure = Mockito.mock(BulkItemResponse.Failure.class);
        when(failure.getCause()).thenReturn(exception);
        when(failure.getStatus()).thenReturn(ExceptionsHelper.status(exception));
        when(bulkItemResponse.getFailure()).thenReturn(failure);

        return bulkResponse;
    }

    public void testIngestPlainTextLog() {
        RestRequest req = createLogsRequest("/_logs", """
            Hello World
            """);
        verifyingClient.setExecuteVerifier((BiFunction<ActionType<BulkResponse>, BulkRequest, BulkResponse>) (actionType, request) -> {
            assertEquals(1, request.requests().size());
            IndexRequest indexRequest = (IndexRequest) request.requests().get(0);
            assertDataStreamFields("generic", "default", indexRequest);
            assertEquals(CREATE, indexRequest.opType());
            assertEquals("Hello World", indexRequest.sourceAsMap().get("message"));
            return Mockito.mock(BulkResponse.class);
        });
        assertEquals(0, dispatchRequest(req).errors().get());
    }

    public void testExpandDots() throws IOException {
        List<String> testScenarios = List.of("""
            {"foo.bar":"baz"}
            {"foo":{"bar":"baz"}}
            """, """
            {"foo":"bar","foo.bar":"baz"}
            {"foo":"bar","foo.bar":"baz"}
            """, """
            {"foo":{"bar":"baz"},"foo.baz":"qux"}
            {"foo":{"baz":"qux","bar":"baz"}}
            """);
        for (String testScenario : testScenarios) {
            String[] split = testScenario.split("\n");
            Map<String, Object> withExpandedDots = jsonToMap(split[0]);
            RestLogsAction.expandDots(withExpandedDots);
            assertEquals(jsonToMap(split[1]), withExpandedDots);
        }
    }

    @SuppressWarnings("unchecked")
    private RestRequest createLogsRequest(String path, Map<?, ?>... ndJson) {
        return createLogsRequest(
            path,
            Arrays.stream(ndJson).map(j -> (Map<String, ?>) j).map(this::json).collect(Collectors.joining("\n"))
        );
    }

    private RestRequest createLogsRequest(String path, String content) {
        return createLogsRequest(path, content, Map.of());
    }

    private RestRequest createLogsRequest(String path, String content, Map<String, String> params) {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(path)
            .withParams(new HashMap<>(params))
            .withContent(BytesReference.fromByteBuffer(ByteBuffer.wrap(content.getBytes(UTF_8))), null)
            .build();
    }

    public String json(Map<String, ?> map) {
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.map(map);
            return XContentHelper.convertToJson(BytesReference.bytes(builder), false, XContentType.JSON);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> jsonToMap(String json) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            return parser.map();
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T getPath(Map<String, Object> doc, String path) {
        Map<String, Object> parent = doc;
        String[] pathElements = path.split("\\.");
        for (int i = 0; i < pathElements.length - 1; i++) {
            parent = (Map<String, Object>) parent.get(pathElements[i]);
            if (parent == null) {
                return null;
            }
        }
        return (T) parent.get(pathElements[pathElements.length - 1]);
    }

}
