/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.otel;

import org.elasticsearch.common.Strings;
import org.elasticsearch.ingest.CompoundProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestPipelineFieldAccessPattern;
import org.elasticsearch.ingest.IngestProcessorException;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Map.entry;
import static org.hamcrest.CoreMatchers.is;

public class NormalizeForStreamProcessorTests extends ESTestCase {

    private final NormalizeForStreamProcessor processor = new NormalizeForStreamProcessor("test", "test processor");

    /**
     * The processor uses a static map of field paths to use for transforming from one format to another. These field paths
     * must be able to work on both classic and flexible access patterns, which means they cannot use any syntax that is exclusive
     * to one or the other, nor should they use features that work differently between the access patterns.
     */
    public void testRenameKeysHaveUniversalSyntax() {
        NormalizeForStreamProcessor.RENAME_KEYS.forEach((key, value) -> {
            var keyParts = key.split("\\.");
            for (String keyPart : keyParts) {
                assertThat("Cannot use open bracket in rename keys", keyPart.contains("]"), is(false));
                assertThat("Cannot use close bracket in rename keys", keyPart.contains("["), is(false));
                expectThrows(NumberFormatException.class, "Cannot use numeric field name in rename keys", () -> Integer.parseInt(keyPart));
            }
            var valueParts = value.split("\\.");
            for (String valuePart : valueParts) {
                assertThat("Cannot use open bracket in rename keys", valuePart.contains("]"), is(false));
                assertThat("Cannot use close bracket in rename keys", valuePart.contains("["), is(false));
                expectThrows(
                    NumberFormatException.class,
                    "Cannot use numeric field name in rename keys",
                    () -> Integer.parseInt(valuePart)
                );
            }
        });
    }

    public void testIsOTelDocument_validMinimalOTelDocument() {
        Map<String, Object> source = new HashMap<>();
        source.put("resource", new HashMap<>());
        assertTrue(NormalizeForStreamProcessor.isOTelDocument(source));
    }

    public void testIsOTelDocument_validOTelDocumentWithScopeAndAttributes() {
        Map<String, Object> source = new HashMap<>();
        source.put("attributes", new HashMap<>());
        source.put("resource", new HashMap<>());
        source.put("scope", new HashMap<>());
        assertTrue(NormalizeForStreamProcessor.isOTelDocument(source));
    }

    public void testIsOTelDocument_missingResource() {
        Map<String, Object> source = new HashMap<>();
        source.put("scope", new HashMap<>());
        assertFalse(NormalizeForStreamProcessor.isOTelDocument(source));
    }

    public void testIsOTelDocument_resourceNotMap() {
        Map<String, Object> source = new HashMap<>();
        source.put("resource", "not a map");
        source.put("scope", new HashMap<>());
        assertFalse(NormalizeForStreamProcessor.isOTelDocument(source));
    }

    public void testIsOTelDocument_invalidResourceAttributes() {
        Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", "not a map");
        Map<String, Object> source = new HashMap<>();
        source.put("resource", resource);
        source.put("scope", new HashMap<>());
        assertFalse(NormalizeForStreamProcessor.isOTelDocument(source));
    }

    public void testIsOTelDocument_scopeNotMap() {
        Map<String, Object> source = new HashMap<>();
        source.put("resource", new HashMap<>());
        source.put("scope", "not a map");
        assertFalse(NormalizeForStreamProcessor.isOTelDocument(source));
    }

    public void testIsOTelDocument_invalidAttributes() {
        Map<String, Object> source = new HashMap<>();
        source.put("resource", new HashMap<>());
        source.put("scope", new HashMap<>());
        source.put("attributes", "not a map");
        assertFalse(NormalizeForStreamProcessor.isOTelDocument(source));
    }

    public void testIsOTelDocument_invalidBody() {
        Map<String, Object> source = new HashMap<>();
        source.put("resource", new HashMap<>());
        source.put("scope", new HashMap<>());
        source.put("body", "not a map");
        assertFalse(NormalizeForStreamProcessor.isOTelDocument(source));
    }

    public void testIsOTelDocument_invalidBodyText() {
        Map<String, Object> body = new HashMap<>();
        body.put("text", 123);
        Map<String, Object> source = new HashMap<>();
        source.put("resource", new HashMap<>());
        source.put("scope", new HashMap<>());
        source.put("body", body);
        assertFalse(NormalizeForStreamProcessor.isOTelDocument(source));
    }

    public void testIsOTelDocument_invalidBodyStructured() {
        Map<String, Object> body = new HashMap<>();
        body.put("structured", "a string");
        Map<String, Object> source = new HashMap<>();
        source.put("resource", new HashMap<>());
        source.put("scope", new HashMap<>());
        source.put("body", body);
        assertFalse(NormalizeForStreamProcessor.isOTelDocument(source));
    }

    public void testIsOTelDocument_validBody() {
        Map<String, Object> body = new HashMap<>();
        body.put("text", "a string");
        body.put("structured", new HashMap<>());
        Map<String, Object> source = new HashMap<>();
        source.put("resource", new HashMap<>());
        source.put("scope", new HashMap<>());
        source.put("body", body);
        assertTrue(NormalizeForStreamProcessor.isOTelDocument(source));
    }

    public void testExecute_validOTelDocument() {
        Map<String, Object> source = Map.ofEntries(
            entry("resource", Map.of()),
            entry("scope", Map.of()),
            entry("body", Map.of("text", "a string", "structured", Map.of())),
            entry("key1", "value1")
        );
        IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);
        Map<String, Object> shallowCopy = new HashMap<>(source);
        runWithRandomAccessPattern(document);
        // verify that top level keys are not moved when processing a valid OTel document
        assertEquals(shallowCopy, document.getSource());
    }

    public void testExecute_nonOTelDocument() {
        Map<String, Object> source = new HashMap<>();
        source.put("key1", "value1");
        source.put("key2", "value2");
        IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);

        runWithRandomAccessPattern(document);

        Map<String, Object> result = document.getSource();
        assertTrue(result.containsKey("attributes"));
        assertTrue(result.containsKey("resource"));

        Map<String, Object> attributes = get(result, "attributes");
        assertEquals("value1", attributes.get("key1"));
        assertEquals("value2", attributes.get("key2"));
        assertFalse(source.containsKey("key1"));
        assertFalse(source.containsKey("key2"));

        Map<String, Object> resource = get(result, "resource");
        assertTrue(resource.containsKey("attributes"));
        Map<String, Object> resourceAttributes = get(resource, "attributes");
        assertTrue(resourceAttributes.isEmpty());
    }

    public void testExecute_nonOTelDocument_withExistingAttributes() {
        Map<String, Object> source = new HashMap<>();
        Map<String, Object> existingAttributes = new HashMap<>();
        existingAttributes.put("existingKey", "existingValue");
        source.put("attributes", existingAttributes);
        source.put("key1", "value1");
        IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);

        runWithRandomAccessPattern(document);

        Map<String, Object> result = document.getSource();
        assertTrue(result.containsKey("attributes"));
        assertTrue(result.containsKey("resource"));

        Map<String, Object> attributes = get(result, "attributes");
        assertEquals("existingValue", attributes.get("attributes.existingKey"));
        assertEquals("value1", attributes.get("key1"));

        Map<String, Object> resource = get(result, "resource");
        assertTrue(resource.containsKey("attributes"));
        Map<String, Object> resourceAttributes = get(resource, "attributes");
        assertTrue(resourceAttributes.isEmpty());
    }

    public void testExecute_nonOTelDocument_withExistingResource() {
        Map<String, Object> source = new HashMap<>();
        Map<String, Object> existingResource = new HashMap<>();
        existingResource.put("existingKey", "existingValue");
        source.put("resource", existingResource);
        source.put("scope", "invalid scope");
        source.put("key1", "value1");
        IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);

        runWithRandomAccessPattern(document);

        Map<String, Object> result = document.getSource();
        assertTrue(result.containsKey("attributes"));
        assertTrue(result.containsKey("resource"));

        Map<String, Object> attributes = get(result, "attributes");
        assertEquals("value1", attributes.get("key1"));
        assertEquals("existingValue", attributes.get("resource.existingKey"));
        assertEquals("invalid scope", attributes.get("scope"));

        Map<String, Object> resource = get(result, "resource");
        assertTrue(resource.containsKey("attributes"));
        Map<String, Object> resourceAttributes = get(resource, "attributes");
        assertTrue(resourceAttributes.isEmpty());
    }

    public void testRenameSpecialKeys_nestedForm() {
        Map<String, Object> source = new HashMap<>();
        Map<String, Object> span = new HashMap<>();
        span.put("id", "spanIdValue");
        source.put("span", span);
        Map<String, Object> log = new HashMap<>();
        log.put("level", "logLevelValue");
        source.put("log", log);
        Map<String, Object> trace = new HashMap<>();
        trace.put("id", "traceIdValue");
        source.put("trace", trace);
        IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);

        doWithRandomAccessPattern(document, NormalizeForStreamProcessor::renameSpecialKeys);

        Map<String, Object> result = document.getSource();
        assertEquals("spanIdValue", result.get("span_id"));
        assertFalse(result.containsKey("span"));
        assertEquals("logLevelValue", result.get("severity_text"));
        assertFalse(result.containsKey("log"));
        assertEquals("traceIdValue", result.get("trace_id"));
        assertFalse(result.containsKey("trace"));
    }

    public void testRenameSpecialKeys_topLevelDottedField() {
        Map<String, Object> source = new HashMap<>();
        source.put("span.id", "spanIdValue");
        source.put("log.level", "logLevelValue");
        source.put("trace.id", "traceIdValue");
        source.put("message", "this is a message");
        IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);

        doWithRandomAccessPattern(document, NormalizeForStreamProcessor::renameSpecialKeys);

        Map<String, Object> result = document.getSource();
        assertEquals("spanIdValue", result.get("span_id"));
        assertEquals("logLevelValue", result.get("severity_text"));
        assertEquals("traceIdValue", result.get("trace_id"));
        Map<String, Object> body = get(result, "body");
        String text = get(body, "text");
        assertEquals("this is a message", text);
        assertFalse(source.containsKey("span.id"));
        assertFalse(source.containsKey("log.level"));
        assertFalse(source.containsKey("trace.id"));
        assertFalse(source.containsKey("message"));
    }

    public void testRenameSpecialKeys_mixedForm() {
        Map<String, Object> source = new HashMap<>();
        Map<String, Object> span = new HashMap<>();
        span.put("id", "nestedSpanIdValue");
        source.put("span", span);
        source.put("span.id", "topLevelSpanIdValue");
        IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);

        doWithRandomAccessPattern(document, NormalizeForStreamProcessor::renameSpecialKeys);

        Map<String, Object> result = document.getSource();
        // nested form should take precedence
        assertEquals("nestedSpanIdValue", result.get("span_id"));
    }

    public void testExecute_moveFlatAttributes() {
        Map<String, Object> source = new HashMap<>();
        Map<String, Object> expectedResourceAttributes = new HashMap<>();
        EcsOTelResourceAttributes.LATEST.forEach(attribute -> {
            String value = randomAlphaOfLength(10);
            source.put(attribute, value);
            expectedResourceAttributes.put(attribute, value);
        });
        Map<String, Object> expectedAttributes = Map.of("agent.non-resource", "value", "service.non-resource", "value", "foo", "bar");
        source.putAll(expectedAttributes);
        IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);

        runWithRandomAccessPattern(document);

        assertTrue(source.containsKey("resource"));
        Map<String, Object> resource = get(source, "resource");
        Map<String, Object> resourceAttributes = get(resource, "attributes");
        assertEquals(expectedResourceAttributes, resourceAttributes);
        EcsOTelResourceAttributes.LATEST.forEach(attribute -> assertFalse(source.containsKey(attribute)));

        assertTrue(source.containsKey("attributes"));
        Map<String, Object> attributes = get(source, "attributes");
        assertEquals(expectedAttributes, attributes);
        assertFalse(source.containsKey("foo"));
        assertFalse(source.containsKey("agent.non-resource"));
        assertFalse(source.containsKey("service.non-resource"));
    }

    public void testExecute_moveNestedAttributes() {
        IngestDocument document = new IngestDocument("index", "id", 1, null, null, new HashMap<>());

        Map<String, Object> expectedResourceAttributes = new HashMap<>();
        EcsOTelResourceAttributes.LATEST.forEach(attribute -> {
            String value = randomAlphaOfLength(10);
            // parses dots as object notations
            document.setFieldValue(attribute, value);
            expectedResourceAttributes.put(attribute, value);
        });
        Map<String, Object> expectedAttributes = Map.of("agent.non-resource", "value", "service.non-resource", "value", "foo", "bar");
        expectedAttributes.forEach(document::setFieldValue);

        runWithRandomAccessPattern(document);

        Map<String, Object> source = document.getSource();

        assertTrue(source.containsKey("resource"));
        Map<String, Object> resource = get(source, "resource");
        Map<String, Object> resourceAttributes = get(resource, "attributes");
        assertEquals(expectedResourceAttributes, resourceAttributes);
        EcsOTelResourceAttributes.LATEST.forEach(attribute -> {
            // parse first part of the key
            String namespace = attribute.substring(0, attribute.indexOf('.'));
            assertFalse(source.containsKey(namespace));
        });
        assertTrue(source.containsKey("attributes"));
        Map<String, Object> attributes = get(source, "attributes");
        assertEquals(expectedAttributes, attributes);
        assertFalse(source.containsKey("foo"));
        assertFalse(source.containsKey("agent.non-resource"));
        assertFalse(source.containsKey("service.non-resource"));
    }

    public void testKeepNullValues() {
        Map<String, Object> source = new HashMap<>();
        Map<String, Object> span = new HashMap<>();
        span.put("id", null);
        source.put("span", span);
        source.put("log.level", null);
        source.put("trace_id", null);
        source.put("foo", null);
        source.put("agent.name", null);
        IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);

        runWithRandomAccessPattern(document);

        assertFalse(source.containsKey("span"));
        assertTrue(source.containsKey("span_id"));
        assertNull(source.get("span_id"));
        assertFalse(source.containsKey("log"));
        assertTrue(source.containsKey("severity_text"));
        assertNull(source.get("severity_text"));
        assertFalse(source.containsKey("trace_id"));
        Map<String, Object> expectedAttributes = new HashMap<>();
        expectedAttributes.put("foo", null);
        expectedAttributes.put("trace_id", null);
        assertEquals(expectedAttributes, get(source, "attributes"));
        Map<String, Object> expectedResourceAttributes = new HashMap<>();
        expectedResourceAttributes.put("agent.name", null);
        assertEquals(expectedResourceAttributes, get(get(source, "resource"), "attributes"));
    }

    public void testExecute_deepFlattening() {
        Map<String, Object> source = new HashMap<>();
        Map<String, Object> service = new HashMap<>();
        service.put("name", "serviceNameValue");
        Map<String, Object> node = new HashMap<>();
        node.put("name", "serviceNodeNameValue");
        node.put("type", "serviceNodeTypeValue");
        service.put("node", node);
        source.put("service", service);

        Map<String, Object> top = new HashMap<>();
        top.put("child", "childValue");
        Map<String, Object> nestedChild = new HashMap<>();
        nestedChild.put("grandchild", "grandchildValue");
        top.put("nested-child", nestedChild);
        source.put("top", top);

        IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);

        runWithRandomAccessPattern(document);

        Map<String, Object> result = document.getSource();

        Map<String, Object> expectedResourceAttributes = Map.of(
            "service.name",
            "serviceNameValue",
            "service.node.name",
            "serviceNodeNameValue"
        );

        assertTrue(result.containsKey("resource"));
        Map<String, Object> resource = get(result, "resource");
        Map<String, Object> resourceAttributes = get(resource, "attributes");
        assertEquals(expectedResourceAttributes, resourceAttributes);
        assertNull(resource.get("service"));

        Map<String, Object> expectedAttributes = Map.of(
            "service.node.type",
            "serviceNodeTypeValue",
            "top.child",
            "childValue",
            "top.nested-child.grandchild",
            "grandchildValue"
        );

        assertTrue(result.containsKey("attributes"));
        Map<String, Object> attributes = get(result, "attributes");
        assertEquals(expectedAttributes, attributes);
        assertNull(attributes.get("top"));
    }

    public void testExecute_arraysNotFlattened() {
        Map<String, Object> source = new HashMap<>();
        Map<String, Object> nestedAgent = new HashMap<>();
        nestedAgent.put("name", "agentNameValue");
        List<String> agentArray = List.of("value1", "value2");
        nestedAgent.put("array", agentArray);
        source.put("agent", nestedAgent);

        Map<String, Object> nestedService = new HashMap<>();
        List<String> serviceNameArray = List.of("value1", "value2");
        nestedService.put("name", serviceNameArray);
        source.put("service", nestedService);

        IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);

        runWithRandomAccessPattern(document);

        Map<String, Object> result = document.getSource();

        Map<String, Object> expectedResourceAttributes = Map.of("agent.name", "agentNameValue", "service.name", serviceNameArray);

        assertTrue(result.containsKey("resource"));
        Map<String, Object> resource = get(result, "resource");
        Map<String, Object> resourceAttributes = get(resource, "attributes");
        assertEquals(expectedResourceAttributes, resourceAttributes);

        assertTrue(result.containsKey("attributes"));
        Map<String, Object> attributes = get(result, "attributes");
        assertEquals(Map.of("agent.array", agentArray), attributes);

        assertNull(resource.get("agent"));
        assertNull(attributes.get("service"));
    }

    /**
     * Test for ECS-JSON {@code message} field normalization.
     * <p>
     * Input document:
     * <pre>
     * {
     *   "@timestamp": "2023-10-01T12:00:00Z",
     *   "message": "{
     *     \"@timestamp\": \"2023-10-02T12:00:00Z\",
     *     \"log.level\": \"INFO\",
     *     \"service.name\": \"my-service\",
     *     \"message\": \"The actual log message\",
     *     \"http\": {
     *       \"method\": \"GET\",
     *       \"url\": {
     *         \"path\": \"/api/v1/resource\"
     *       }
     *     }
     *   }"
     * }
     * </pre>
     * <p>
     * Expected output document:
     * <pre>
     * {
     *   "@timestamp": "2023-10-02T12:00:00Z",
     *   "severity_text": "INFO",
     *   "body": {
     *     "text": "The actual log message"
     *   },
     *   "resource": {
     *     "attributes": {
     *       "service.name": "my-service"
     *     }
     *   },
     *   "attributes": {
     *     "http.method": "GET",
     *     "http.url.path": "/api/v1/resource"
     *   }
     * }
     * </pre>
     */
    public void testExecute_ecsJsonMessageNormalization() throws IOException {
        Map<String, Object> httpUrl = new HashMap<>();
        httpUrl.put("path", "/api/v1/resource");

        Map<String, Object> http = new HashMap<>();
        http.put("method", "GET");
        http.put("url", httpUrl);

        Map<String, Object> message = new HashMap<>();
        message.put("@timestamp", "2023-10-02T12:00:00Z");
        message.put("log.level", "INFO");
        message.put("service.name", "my-service");
        message.put("message", "The actual log message");
        message.put("http", http);

        Map<String, Object> source = new HashMap<>();
        source.put("@timestamp", "2023-10-01T12:00:00Z");
        source.put("message", representJsonAsString(message));

        IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);
        runWithRandomAccessPattern(document);

        Map<String, Object> result = document.getSource();

        assertEquals("2023-10-02T12:00:00Z", result.get("@timestamp"));
        assertEquals("INFO", result.get("severity_text"));
        assertEquals("The actual log message", get(get(result, "body"), "text"));
        assertEquals(Map.of("service.name", "my-service"), get(get(result, "resource"), "attributes"));
        assertEquals(Map.of("http.method", "GET", "http.url.path", "/api/v1/resource"), get(result, "attributes"));
    }

    /**
     * Test for non-ECS-JSON {@code message} field normalization.
     * <p>
     * Input document:
     * <pre>
     * {
     *   "@timestamp": "2023-10-01T12:00:00Z",
     *   "log": {
     *     "level": "INFO"
     *   },
     *   "service": {
     *     "name": "my-service"
     *   },
     *   "tags": ["user-action", "api-call"],
     *   "message": "{
     *     \"root_cause\": \"Network error\",
     *     \"http\": {
     *       \"method\": \"GET\",
     *       \"url\": {
     *         \"path\": \"/api/v1/resource\"
     *       }
     *     }
     *   }"
     * }
     * </pre>
     * <p>
     * Expected output document:
     * <pre>
     * {
     *   "@timestamp": "2023-10-01T12:00:00Z",
     *   "severity_text": "INFO",
     *   "resource": {
     *     "attributes": {
     *       "service.name": "my-service"
     *     }
     *   },
     *   "attributes": {
     *     "tags": ["user-action", "api-call"]
     *   },
     *   "body": {
     *     "structured": {
     *       "root_cause": "Network error",
     *       "http": {
     *         "method": "GET",
     *         "url": {
     *           "path": "/api/v1/resource"
     *         }
     *       }
     *     }
     *   }
     * }
     * </pre>
     */
    public void testExecute_nonEcsJsonMessageNormalization() throws IOException {
        Map<String, Object> httpUrl = new HashMap<>();
        httpUrl.put("path", "/api/v1/resource");

        Map<String, Object> http = new HashMap<>();
        http.put("method", "GET");
        http.put("url", httpUrl);

        Map<String, Object> message = new HashMap<>();
        message.put("root_cause", "Network error");
        message.put("http", http);

        Map<String, Object> log = new HashMap<>();
        log.put("level", "INFO");

        Map<String, Object> service = new HashMap<>();
        service.put("name", "my-service");

        Map<String, Object> source = new HashMap<>();
        source.put("@timestamp", "2023-10-01T12:00:00Z");
        source.put("log", log);
        source.put("service", service);
        source.put("tags", new ArrayList<>(List.of("user-action", "api-call")));
        source.put("message", representJsonAsString(message));

        IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);
        runWithRandomAccessPattern(document);

        Map<String, Object> result = document.getSource();

        assertEquals("2023-10-01T12:00:00Z", result.get("@timestamp"));
        assertEquals("INFO", result.get("severity_text"));
        assertEquals(Map.of("service.name", "my-service"), get(get(result, "resource"), "attributes"));
        assertEquals(Map.of("tags", List.of("user-action", "api-call")), get(result, "attributes"));
        assertEquals(message, get(get(result, "body"), "structured"));
    }

    @SuppressWarnings("unchecked")
    public void testOtherPrimitiveMessage() {
        Map<String, Object> source = new HashMap<>();
        source.put("message", 42);
        IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);

        runWithRandomAccessPattern(document);

        Map<String, Object> result = document.getSource();
        assertEquals(42, ((Map<String, Object>) result.get("body")).get("text"));
    }

    @SuppressWarnings("unchecked")
    public void testObjectMessage() {
        Map<String, Object> source = new HashMap<>();
        Map<String, Object> message = new HashMap<>();
        message.put("key1", "value1");
        message.put("key2", "value2");
        source.put("message", message);
        IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);

        runWithRandomAccessPattern(document);

        Map<String, Object> result = document.getSource();
        assertEquals(message, ((Map<String, Object>) result.get("body")).get("text"));
    }

    private static String representJsonAsString(Map<String, Object> json) throws IOException {
        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            return Strings.toString(xContentBuilder.map(json));
        }
    }

    /**
     * A utility function for getting a key from a map and casting the result.
     */
    @SuppressWarnings("unchecked")
    private static <T> T get(Map<String, Object> context, String key) {
        return (T) context.get(key);
    }

    private void runWithRandomAccessPattern(IngestDocument document) {
        runWithAccessPattern(randomFrom(IngestPipelineFieldAccessPattern.values()), document);
    }

    private void runWithAccessPattern(IngestPipelineFieldAccessPattern accessPattern, IngestDocument document) {
        runProcessorWithAccessPattern(accessPattern, document, processor);
    }

    private void doWithRandomAccessPattern(IngestDocument document, Consumer<IngestDocument> action) {
        doWithAccessPattern(randomFrom(IngestPipelineFieldAccessPattern.values()), document, action);
    }

    private void doWithAccessPattern(
        IngestPipelineFieldAccessPattern accessPattern,
        IngestDocument document,
        Consumer<IngestDocument> action
    ) {
        runProcessorWithAccessPattern(accessPattern, document, new TestProcessor(action));
    }

    private void runProcessorWithAccessPattern(
        IngestPipelineFieldAccessPattern accessPattern,
        IngestDocument document,
        Processor processor
    ) {
        AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>(null);
        document.executePipeline(
            new Pipeline(
                randomAlphanumericOfLength(10),
                null,
                null,
                null,
                new CompoundProcessor(processor),
                accessPattern,
                null,
                null,
                null
            ),
            (ignored, ex) -> {
                if (ex != null) {
                    if (ex instanceof IngestProcessorException ingestProcessorException) {
                        exceptionAtomicReference.set((Exception) ingestProcessorException.getCause());
                    } else {
                        exceptionAtomicReference.set(ex);
                    }
                }
            }
        );
        Exception exception = exceptionAtomicReference.get();
        if (exception != null) {
            fail(exception);
        }
    }
}
