/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.ecs;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EcsNamespaceProcessorTests extends ESTestCase {

    private final EcsNamespaceProcessor processor = new EcsNamespaceProcessor("test", "test processor");

    public void testIsOTelDocument_validMinimalOTelDocument() {
        Map<String, Object> source = new HashMap<>();
        source.put("resource", new HashMap<>());
        assertTrue(EcsNamespaceProcessor.isOTelDocument(source));
    }

    public void testIsOTelDocument_validOTelDocumentWithScopeAndAttributes() {
        Map<String, Object> source = new HashMap<>();
        source.put("attributes", new HashMap<>());
        source.put("resource", new HashMap<>());
        source.put("scope", new HashMap<>());
        assertTrue(EcsNamespaceProcessor.isOTelDocument(source));
    }

    public void testIsOTelDocument_missingResource() {
        Map<String, Object> source = new HashMap<>();
        source.put("scope", new HashMap<>());
        assertFalse(EcsNamespaceProcessor.isOTelDocument(source));
    }

    public void testIsOTelDocument_resourceNotMap() {
        Map<String, Object> source = new HashMap<>();
        source.put("resource", "not a map");
        source.put("scope", new HashMap<>());
        assertFalse(EcsNamespaceProcessor.isOTelDocument(source));
    }

    public void testIsOTelDocument_invalidResourceAttributes() {
        Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", "not a map");
        Map<String, Object> source = new HashMap<>();
        source.put("resource", resource);
        source.put("scope", new HashMap<>());
        assertFalse(EcsNamespaceProcessor.isOTelDocument(source));
    }

    public void testIsOTelDocument_scopeNotMap() {
        Map<String, Object> source = new HashMap<>();
        source.put("resource", new HashMap<>());
        source.put("scope", "not a map");
        assertFalse(EcsNamespaceProcessor.isOTelDocument(source));
    }

    public void testIsOTelDocument_invalidAttributes() {
        Map<String, Object> source = new HashMap<>();
        source.put("resource", new HashMap<>());
        source.put("scope", new HashMap<>());
        source.put("attributes", "not a map");
        assertFalse(EcsNamespaceProcessor.isOTelDocument(source));
    }

    public void testIsOTelDocument_invalidBody() {
        Map<String, Object> source = new HashMap<>();
        source.put("resource", new HashMap<>());
        source.put("scope", new HashMap<>());
        source.put("body", "not a map");
        assertFalse(EcsNamespaceProcessor.isOTelDocument(source));
    }

    public void testIsOTelDocument_invalidBodyText() {
        Map<String, Object> body = new HashMap<>();
        body.put("text", 123);
        Map<String, Object> source = new HashMap<>();
        source.put("resource", new HashMap<>());
        source.put("scope", new HashMap<>());
        source.put("body", body);
        assertFalse(EcsNamespaceProcessor.isOTelDocument(source));
    }

    public void testIsOTelDocument_invalidBodyStructured() {
        Map<String, Object> body = new HashMap<>();
        body.put("structured", "a string");
        Map<String, Object> source = new HashMap<>();
        source.put("resource", new HashMap<>());
        source.put("scope", new HashMap<>());
        source.put("body", body);
        assertFalse(EcsNamespaceProcessor.isOTelDocument(source));
    }

    public void testIsOTelDocument_validBody() {
        Map<String, Object> body = new HashMap<>();
        body.put("text", "a string");
        body.put("structured", new HashMap<>());
        Map<String, Object> source = new HashMap<>();
        source.put("resource", new HashMap<>());
        source.put("scope", new HashMap<>());
        source.put("body", body);
        assertTrue(EcsNamespaceProcessor.isOTelDocument(source));
    }

    public void testExecute_validOTelDocument() {
        Map<String, Object> source = new HashMap<>();
        source.put("resource", new HashMap<>());
        source.put("scope", new HashMap<>());
        Map<String, Object> body = new HashMap<>();
        body.put("text", "a string");
        body.put("structured", new HashMap<>());
        source.put("body", body);
        source.put("key1", "value1");
        Map<String, Object> before = new HashMap<>(source);
        IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);
        processor.execute(document);
        assertEquals(before, document.getSource());
    }

    public void testExecute_nonOTelDocument() {
        Map<String, Object> source = new HashMap<>();
        source.put("key1", "value1");
        source.put("key2", "value2");
        IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);

        processor.execute(document);

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

        processor.execute(document);

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

        processor.execute(document);

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

        EcsNamespaceProcessor.renameSpecialKeys(document);

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

        EcsNamespaceProcessor.renameSpecialKeys(document);

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

        EcsNamespaceProcessor.renameSpecialKeys(document);

        Map<String, Object> result = document.getSource();
        // nested form should take precedence
        assertEquals("nestedSpanIdValue", result.get("span_id"));
    }

    public void testExecute_moveToAttributeMaps() {
        Map<String, Object> source = new HashMap<>();
        source.put("agent.name", "agentNameValue");
        Map<String, Object> agent = new HashMap<>();
        agent.put("type", "agentTypeValue");
        source.put("agent", agent);
        source.put("cloud.provider", "cloudProviderValue");
        Map<String, Object> cloud = new HashMap<>();
        cloud.put("type", "cloudTypeValue");
        source.put("cloud", cloud);
        source.put("host.name", "hostNameValue");
        Map<String, Object> host = new HashMap<>();
        host.put("type", "hostTypeValue");
        source.put("host", host);
        source.put("service.name", "serviceNameValue");
        Map<String, Object> service = new HashMap<>();
        service.put("type", "serviceTypeValue");
        source.put("service", service);
        IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);

        processor.execute(document);

        Map<String, Object> result = document.getSource();

        // all attributes should be flattened
        Map<String, Object> expectedResourceAttributes = Map.of(
            "agent.name",
            "agentNameValue",
            "agent.type",
            "agentTypeValue",
            "cloud.provider",
            "cloudProviderValue",
            "cloud.type",
            "cloudTypeValue",
            "host.name",
            "hostNameValue",
            "host.type",
            "hostTypeValue"
        );

        assertTrue(result.containsKey("resource"));
        Map<String, Object> resource = get(result, "resource");
        Map<String, Object> resourceAttributes = get(resource, "attributes");
        assertEquals(expectedResourceAttributes, resourceAttributes);
        assertNull(resourceAttributes.get("agent"));
        assertNull(resourceAttributes.get("cloud"));
        assertNull(resourceAttributes.get("host"));
        assertFalse(source.containsKey("agent.name"));
        assertFalse(source.containsKey("agent"));
        assertFalse(source.containsKey("cloud.provider"));
        assertFalse(source.containsKey("cloud"));
        assertFalse(source.containsKey("host.name"));
        assertFalse(source.containsKey("host"));

        Map<String, Object> expectedAttributes = Map.of("service.name", "serviceNameValue", "service.type", "serviceTypeValue");

        assertTrue(result.containsKey("attributes"));
        Map<String, Object> attributes = get(result, "attributes");
        assertEquals(expectedAttributes, attributes);
        assertNull(attributes.get("service"));
        assertFalse(source.containsKey("service.name"));
        assertFalse(source.containsKey("service"));
    }

    public void testExecute_deepFlattening() {
        Map<String, Object> source = new HashMap<>();
        Map<String, Object> nestedAgent = new HashMap<>();
        nestedAgent.put("name", "agentNameValue");
        Map<String, Object> deeperAgent = new HashMap<>();
        deeperAgent.put("type", "agentTypeValue");
        nestedAgent.put("details", deeperAgent);
        source.put("agent", nestedAgent);

        Map<String, Object> nestedService = new HashMap<>();
        nestedService.put("name", "serviceNameValue");
        Map<String, Object> deeperService = new HashMap<>();
        deeperService.put("type", "serviceTypeValue");
        nestedService.put("details", deeperService);
        source.put("service", nestedService);

        IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);

        processor.execute(document);

        Map<String, Object> result = document.getSource();

        Map<String, Object> expectedResourceAttributes = Map.of("agent.name", "agentNameValue", "agent.details.type", "agentTypeValue");

        assertTrue(result.containsKey("resource"));
        Map<String, Object> resource = get(result, "resource");
        Map<String, Object> resourceAttributes = get(resource, "attributes");
        assertEquals(expectedResourceAttributes, resourceAttributes);
        assertNull(resource.get("agent"));

        Map<String, Object> expectedAttributes = Map.of("service.name", "serviceNameValue", "service.details.type", "serviceTypeValue");

        assertTrue(result.containsKey("attributes"));
        Map<String, Object> attributes = get(result, "attributes");
        assertEquals(expectedAttributes, attributes);
        assertNull(attributes.get("service"));
    }

    public void testExecute_arraysNotFlattened() {
        Map<String, Object> source = new HashMap<>();
        Map<String, Object> nestedAgent = new HashMap<>();
        nestedAgent.put("name", "agentNameValue");
        List<String> agentArray = List.of("value1", "value2");
        nestedAgent.put("array", agentArray);
        source.put("agent", nestedAgent);

        Map<String, Object> nestedService = new HashMap<>();
        nestedService.put("name", "serviceNameValue");
        List<String> serviceArray = List.of("value1", "value2");
        nestedService.put("array", serviceArray);
        source.put("service", nestedService);

        IngestDocument document = new IngestDocument("index", "id", 1, null, null, source);

        processor.execute(document);

        Map<String, Object> result = document.getSource();

        Map<String, Object> expectedResourceAttributes = Map.of("agent.name", "agentNameValue", "agent.array", agentArray);

        assertTrue(result.containsKey("resource"));
        Map<String, Object> resource = get(result, "resource");
        Map<String, Object> resourceAttributes = get(resource, "attributes");
        assertEquals(expectedResourceAttributes, resourceAttributes);
        assertNull(resource.get("agent"));

        Map<String, Object> expectedAttributes = Map.of("service.name", "serviceNameValue", "service.array", serviceArray);

        assertTrue(result.containsKey("attributes"));
        Map<String, Object> attributes = get(result, "attributes");
        assertEquals(expectedAttributes, attributes);
        assertNull(attributes.get("service"));
    }

    /**
     * A utility function for getting a key from a map and casting the result.
     */
    @SuppressWarnings("unchecked")
    private static <T> T get(Map<String, Object> context, String key) {
        return (T) context.get(key);
    }
}
