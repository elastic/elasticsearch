/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.logs;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesReference;
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
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.action.DocWriteRequest.OpType.CREATE;
import static org.mockito.Mockito.when;

public class RestLogsActionTest extends RestActionTestCase {

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestLogsAction());
    }

    @Test
    public void testIngestJsonLogs() {
        RestRequest req = createLogsRequest("/_logs", Map.of("message", "Hello World"), Map.of("foo", "bar"));
        setBulkRequestVerifier((actionType, request) -> {
            assertEquals(2, request.requests().size());
            IndexRequest indexRequest = (IndexRequest) request.requests().get(0);
            assertDataStreamFields("generic", "default", indexRequest);
            assertEquals(CREATE, indexRequest.opType());
            assertEquals("Hello World", ((IndexRequest) request.requests().get(0)).sourceAsMap().get("message"));
            assertEquals("bar", ((IndexRequest) request.requests().get(1)).sourceAsMap().get("foo"));
            return Mockito.mock(BulkResponse.class);
        });
        dispatchRequest(req);
    }

    @Test
    public void testMetadata() {
        RestRequest req = createLogsRequest(
            "/_logs",
            Map.of("_metadata", Map.of("global", true)),
            Map.of("_metadata", Map.of("local1", true)),
            Map.of("foo", "bar"),
            Map.of("_metadata", Map.of("local2", true)),
            Map.of("foo", "bar")
        );
        setBulkRequestVerifier((actionType, request) -> {
            assertEquals(2, request.requests().size());
            Map<String, Object> doc1 = ((IndexRequest) request.requests().get(0)).sourceAsMap();
            Map<String, Object> doc2 = ((IndexRequest) request.requests().get(1)).sourceAsMap();
            assertEquals(true, doc1.get("global"));
            assertEquals(true, doc1.get("local1"));
            assertEquals(true, doc1.get("global"));
            assertEquals(true, doc2.get("local2"));
            return Mockito.mock(BulkResponse.class);
        });
        dispatchRequest(req);
    }

    private void assertDataStreamFields(String dataset, String namespace, DocWriteRequest<?> docWriteRequest) {
        IndexRequest indexRequest = (IndexRequest) docWriteRequest;
        assertEquals("logs-" + dataset + "-" + namespace, indexRequest.index());
        assertEquals(Map.of("type", "logs", "dataset", dataset, "namespace", namespace), indexRequest.sourceAsMap().get("data_stream"));
    }

    @Test
    public void testPathMetadata() {
        RestRequest req = createLogsRequest("/_logs/foo/bar", Map.of("message", "Hello World"));
        setBulkRequestVerifier((actionType, request) -> {
            assertEquals(1, request.requests().size());
            assertDataStreamFields("foo", "bar", request.requests().get(0));
            return Mockito.mock(BulkResponse.class);
        });
        dispatchRequest(req);
    }

    @Test
    public void testRetryOnIngestError() {
        RestRequest req = createLogsRequest("/_logs/foo", Map.of("message", "Hello World"));
        AtomicBoolean firstRequest = new AtomicBoolean(true);
        setBulkRequestVerifier((actionType, request) -> {
            if (firstRequest.get()) {
                firstRequest.set(false);
                assertEquals(1, request.requests().size());
                assertDataStreamFields("foo", "default", request.requests().get(0));
                BulkResponse bulkResponse = Mockito.mock(BulkResponse.class);
                BulkItemResponse bulkItemResponse = Mockito.mock(BulkItemResponse.class);
                when(bulkResponse.hasFailures()).thenReturn(true);
                when(bulkItemResponse.getItemId()).thenReturn(0);
                when(bulkItemResponse.isFailed()).thenReturn(true);
                BulkItemResponse.Failure failure = Mockito.mock(BulkItemResponse.Failure.class);
                when(failure.getCause()).thenReturn(new MapperParsingException("bad foo"));
                when(bulkItemResponse.getFailure()).thenReturn(failure);
                when(bulkResponse.getItems()).thenReturn(new BulkItemResponse[] { bulkItemResponse });
                return bulkResponse;
            } else {
                assertEquals(1, request.requests().size());
                IndexRequest indexRequest = (IndexRequest) request.requests().get(0);
                assertDataStreamFields("generic", "default", indexRequest);
                assertEquals(Map.of("type", "mapper_parsing_exception", "message", "bad foo"), indexRequest.sourceAsMap().get("error"));
                return Mockito.mock(BulkResponse.class);
            }
        });
        dispatchRequest(req);
    }

    @Test
    public void testIngestPlainTextLog() {
        RestRequest req = createLogsRequest("/_logs", """
            Hello World
            """);
        setBulkRequestVerifier((actionType, request) -> {
            assertEquals(1, request.requests().size());
            IndexRequest indexRequest = (IndexRequest) request.requests().get(0);
            assertDataStreamFields("generic", "default", indexRequest);
            assertEquals(CREATE, indexRequest.opType());
            assertEquals("Hello World", indexRequest.sourceAsMap().get("message"));
            return Mockito.mock(BulkResponse.class);
        });
        dispatchRequest(req);
    }

    @Test
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

    private RestRequest createLogsRequest(String path, Map<?, ?>... ndJson) {
        return createLogsRequest(
            path,
            Arrays.stream(ndJson).map(j -> (Map<String, ?>) j).map(this::json).collect(Collectors.joining("\n"))
        );
    }

    private RestRequest createLogsRequest(String path, String content) {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(path)
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

    private void setBulkRequestVerifier(BiFunction<ActionType<BulkResponse>, BulkRequest, BulkResponse> verifier) {
        verifyingClient.setExecuteVerifier(verifier);
        BiFunction<?, ?, ?> dropTypeInfo = (BiFunction<?, ?, ?>) verifier;
        @SuppressWarnings("unchecked")
        BiFunction<ActionType<?>, ActionRequest, ActionResponse> pasteGenerics = (BiFunction<
            ActionType<?>,
            ActionRequest,
            ActionResponse>) dropTypeInfo;
        verifyingClient.setExecuteLocallyVerifier(pasteGenerics);
    }
}
