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

@SuppressWarnings("unchecked")
public class EcsNamespacingProcessorTests extends ESTestCase {

    private final EcsNamespacingProcessor processor = new EcsNamespacingProcessor("test", "test processor");

    public void testIsOTelDocument_validMinimalOTelDocument() {
        Map<String, Object> document = new HashMap<>();
        document.put("resource", new HashMap<>());
        document.put("scope", new HashMap<>());
        assertTrue(processor.isOTelDocument(document));
    }

    public void testIsOTelDocument_validOTelDocumentWithAttributes() {
        Map<String, Object> document = new HashMap<>();
        document.put("attributes", new HashMap<>());
        document.put("resource", new HashMap<>());
        document.put("scope", new HashMap<>());
        assertTrue(processor.isOTelDocument(document));
    }

    public void testIsOTelDocument_missingResource() {
        Map<String, Object> document = new HashMap<>();
        document.put("scope", new HashMap<>());
        assertFalse(processor.isOTelDocument(document));
    }

    public void testIsOTelDocument_missingScope() {
        Map<String, Object> document = new HashMap<>();
        document.put("resource", new HashMap<>());
        assertFalse(processor.isOTelDocument(document));
    }

    public void testIsOTelDocument_resourceNotMap() {
        Map<String, Object> document = new HashMap<>();
        document.put("resource", "not a map");
        document.put("scope", new HashMap<>());
        assertFalse(processor.isOTelDocument(document));
    }

    public void testIsOTelDocument_invalidResourceAttributes() {
        Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", "not a map");
        Map<String, Object> document = new HashMap<>();
        document.put("resource", resource);
        document.put("scope", new HashMap<>());
        assertFalse(processor.isOTelDocument(document));
    }

    public void testIsOTelDocument_scopeNotMap() {
        Map<String, Object> document = new HashMap<>();
        document.put("resource", new HashMap<>());
        document.put("scope", "not a map");
        assertFalse(processor.isOTelDocument(document));
    }

    public void testIsOTelDocument_invalidAttributes() {
        Map<String, Object> document = new HashMap<>();
        document.put("resource", new HashMap<>());
        document.put("scope", new HashMap<>());
        document.put("attributes", "not a map");
        assertFalse(processor.isOTelDocument(document));
    }

    public void testIsOTelDocument_invalidBody() {
        Map<String, Object> document = new HashMap<>();
        document.put("resource", new HashMap<>());
        document.put("scope", new HashMap<>());
        document.put("body", "not a map");
        assertFalse(processor.isOTelDocument(document));
    }

    public void testIsOTelDocument_invalidBodyText() {
        Map<String, Object> body = new HashMap<>();
        body.put("text", 123);
        Map<String, Object> document = new HashMap<>();
        document.put("resource", new HashMap<>());
        document.put("scope", new HashMap<>());
        document.put("body", body);
        assertFalse(processor.isOTelDocument(document));
    }

    public void testIsOTelDocument_invalidBodyStructured() {
        Map<String, Object> body = new HashMap<>();
        body.put("structured", "a string");
        Map<String, Object> document = new HashMap<>();
        document.put("resource", new HashMap<>());
        document.put("scope", new HashMap<>());
        document.put("body", body);
        assertFalse(processor.isOTelDocument(document));
    }

    public void testIsOTelDocument_validBody() {
        Map<String, Object> body = new HashMap<>();
        body.put("text", "a string");
        body.put("structured", new HashMap<>());
        Map<String, Object> document = new HashMap<>();
        document.put("resource", new HashMap<>());
        document.put("scope", new HashMap<>());
        document.put("body", body);
        assertTrue(processor.isOTelDocument(document));
    }

    public void testExecute_validOTelDocument() {
        Map<String, Object> document = new HashMap<>();
        document.put("resource", new HashMap<>());
        document.put("scope", new HashMap<>());
        Map<String, Object> body = new HashMap<>();
        body.put("text", "a string");
        body.put("structured", new HashMap<>());
        document.put("body", body);
        document.put("key1", "value1");
        Map<String, Object> before = new HashMap<>(document);
        IngestDocument ingestDocument = new IngestDocument("index", "id", 1, null, null, document);
        IngestDocument result = processor.execute(ingestDocument);
        assertEquals(before, result.getSource());
    }

    public void testExecute_nonOTelDocument() {
        Map<String, Object> document = new HashMap<>();
        document.put("key1", "value1");
        document.put("key2", "value2");
        IngestDocument ingestDocument = new IngestDocument("index", "id", 1, null, null, document);

        IngestDocument result = processor.execute(ingestDocument);

        Map<String, Object> source = result.getSource();
        assertTrue(source.containsKey("attributes"));
        assertTrue(source.containsKey("resource"));

        Map<String, Object> attributes = (Map<String, Object>) source.get("attributes");
        assertEquals("value1", attributes.get("key1"));
        assertEquals("value2", attributes.get("key2"));
        assertFalse(document.containsKey("key1"));
        assertFalse(document.containsKey("key2"));

        Map<String, Object> resource = (Map<String, Object>) source.get("resource");
        assertTrue(resource.containsKey("attributes"));
        assertTrue(((Map<String, Object>) resource.get("attributes")).isEmpty());
    }

    public void testExecute_nonOTelDocument_withExistingAttributes() {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> existingAttributes = new HashMap<>();
        existingAttributes.put("existingKey", "existingValue");
        document.put("attributes", existingAttributes);
        document.put("key1", "value1");
        IngestDocument ingestDocument = new IngestDocument("index", "id", 1, null, null, document);

        IngestDocument result = processor.execute(ingestDocument);

        Map<String, Object> source = result.getSource();
        assertTrue(source.containsKey("attributes"));
        assertTrue(source.containsKey("resource"));

        Map<String, Object> attributes = (Map<String, Object>) source.get("attributes");
        assertEquals("existingValue", attributes.get("attributes.existingKey"));
        assertEquals("value1", attributes.get("key1"));

        Map<String, Object> resource = (Map<String, Object>) source.get("resource");
        assertTrue(resource.containsKey("attributes"));
        assertTrue(((Map<String, Object>) resource.get("attributes")).isEmpty());
    }

    public void testExecute_nonOTelDocument_withExistingResource() {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> existingResource = new HashMap<>();
        existingResource.put("existingKey", "existingValue");
        document.put("resource", existingResource);
        document.put("key1", "value1");
        IngestDocument ingestDocument = new IngestDocument("index", "id", 1, null, null, document);

        IngestDocument result = processor.execute(ingestDocument);

        Map<String, Object> source = result.getSource();
        assertTrue(source.containsKey("attributes"));
        assertTrue(source.containsKey("resource"));

        Map<String, Object> attributes = (Map<String, Object>) source.get("attributes");
        assertEquals("value1", attributes.get("key1"));
        assertEquals("existingValue", attributes.get("resource.existingKey"));

        Map<String, Object> resource = (Map<String, Object>) source.get("resource");
        assertTrue(resource.containsKey("attributes"));
        assertTrue(((Map<String, Object>) resource.get("attributes")).isEmpty());
    }

    public void testRenameSpecialKeys_nestedForm() {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> span = new HashMap<>();
        span.put("id", "spanIdValue");
        document.put("span", span);
        Map<String, Object> log = new HashMap<>();
        log.put("level", "logLevelValue");
        document.put("log", log);
        Map<String, Object> trace = new HashMap<>();
        trace.put("id", "traceIdValue");
        document.put("trace", trace);
        IngestDocument ingestDocument = new IngestDocument("index", "id", 1, null, null, document);

        processor.execute(ingestDocument);

        Map<String, Object> source = ingestDocument.getSource();
        assertEquals("spanIdValue", source.get("span_id"));
        assertFalse(source.containsKey("span"));
        assertEquals("logLevelValue", source.get("severity_text"));
        assertFalse(source.containsKey("log"));
        assertEquals("traceIdValue", source.get("trace_id"));
        assertFalse(source.containsKey("trace"));
        assertTrue(source.containsKey("attributes"));
        assertTrue(((Map<String, Object>) source.get("attributes")).isEmpty());
    }

    public void testRenameSpecialKeys_topLevelDottedField() {
        Map<String, Object> document = new HashMap<>();
        document.put("span.id", "spanIdValue");
        document.put("log.level", "logLevelValue");
        document.put("trace.id", "traceIdValue");
        document.put("message", "this is a message");
        IngestDocument ingestDocument = new IngestDocument("index", "id", 1, null, null, document);

        processor.execute(ingestDocument);

        Map<String, Object> source = ingestDocument.getSource();
        assertEquals("spanIdValue", source.get("span_id"));
        assertEquals("logLevelValue", source.get("severity_text"));
        assertEquals("traceIdValue", source.get("trace_id"));
        Object body = source.get("body");
        assertTrue(body instanceof Map);
        assertEquals("this is a message", ((Map<String, Object>) body).get("text"));
        assertTrue(source.containsKey("attributes"));
        assertTrue(((Map<String, Object>) source.get("attributes")).isEmpty());
        assertFalse(document.containsKey("span.id"));
        assertFalse(document.containsKey("log.level"));
        assertFalse(document.containsKey("trace.id"));
        assertFalse(document.containsKey("message"));
    }

    public void testRenameSpecialKeys_mixedForm() {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> span = new HashMap<>();
        span.put("id", "nestedSpanIdValue");
        document.put("span", span);
        document.put("span.id", "topLevelSpanIdValue");
        IngestDocument ingestDocument = new IngestDocument("index", "id", 1, null, null, document);

        processor.execute(ingestDocument);

        Map<String, Object> source = ingestDocument.getSource();
        // nested form should take precedence
        assertEquals("nestedSpanIdValue", source.get("span_id"));
    }

    public void testExecute_moveToAttributeMaps() {
        Map<String, Object> document = new HashMap<>();
        document.put("agent.name", "agentNameValue");
        Map<String, Object> agent = new HashMap<>();
        agent.put("type", "agentTypeValue");
        document.put("agent", agent);
        document.put("cloud.provider", "cloudProviderValue");
        Map<String, Object> cloud = new HashMap<>();
        cloud.put("type", "cloudTypeValue");
        document.put("cloud", cloud);
        document.put("host.name", "hostNameValue");
        Map<String, Object> host = new HashMap<>();
        host.put("type", "hostTypeValue");
        document.put("host", host);
        document.put("service.name", "serviceNameValue");
        Map<String, Object> service = new HashMap<>();
        service.put("type", "serviceTypeValue");
        document.put("service", service);
        IngestDocument ingestDocument = new IngestDocument("index", "id", 1, null, null, document);

        IngestDocument result = processor.execute(ingestDocument);

        Map<String, Object> source = result.getSource();

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

        assertTrue(source.containsKey("resource"));
        Map<String, Object> resource = (Map<String, Object>) source.get("resource");
        Map<String, Object> resourceAttributes = (Map<String, Object>) resource.get("attributes");
        assertEquals(expectedResourceAttributes, resourceAttributes);
        assertNull(resourceAttributes.get("agent"));
        assertNull(resourceAttributes.get("cloud"));
        assertNull(resourceAttributes.get("host"));
        assertFalse(document.containsKey("agent.name"));
        assertFalse(document.containsKey("agent"));
        assertFalse(document.containsKey("cloud.provider"));
        assertFalse(document.containsKey("cloud"));
        assertFalse(document.containsKey("host.name"));
        assertFalse(document.containsKey("host"));

        Map<String, Object> expectedAttributes = Map.of("service.name", "serviceNameValue", "service.type", "serviceTypeValue");

        assertTrue(source.containsKey("attributes"));
        Map<String, Object> attributes = (Map<String, Object>) source.get("attributes");
        assertEquals(expectedAttributes, attributes);
        assertNull(attributes.get("service"));
        assertFalse(document.containsKey("service.name"));
        assertFalse(document.containsKey("service"));
    }

    public void testExecute_deepFlattening() {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> nestedAgent = new HashMap<>();
        nestedAgent.put("name", "agentNameValue");
        Map<String, Object> deeperAgent = new HashMap<>();
        deeperAgent.put("type", "agentTypeValue");
        nestedAgent.put("details", deeperAgent);
        document.put("agent", nestedAgent);

        Map<String, Object> nestedService = new HashMap<>();
        nestedService.put("name", "serviceNameValue");
        Map<String, Object> deeperService = new HashMap<>();
        deeperService.put("type", "serviceTypeValue");
        nestedService.put("details", deeperService);
        document.put("service", nestedService);

        IngestDocument ingestDocument = new IngestDocument("index", "id", 1, null, null, document);

        IngestDocument result = processor.execute(ingestDocument);

        Map<String, Object> source = result.getSource();

        Map<String, Object> expectedResourceAttributes = Map.of("agent.name", "agentNameValue", "agent.details.type", "agentTypeValue");

        assertTrue(source.containsKey("resource"));
        Map<String, Object> resource = (Map<String, Object>) source.get("resource");
        Map<String, Object> resourceAttributes = (Map<String, Object>) resource.get("attributes");
        assertEquals(expectedResourceAttributes, resourceAttributes);
        assertNull(resource.get("agent"));

        Map<String, Object> expectedAttributes = Map.of("service.name", "serviceNameValue", "service.details.type", "serviceTypeValue");

        assertTrue(source.containsKey("attributes"));
        Map<String, Object> attributes = (Map<String, Object>) source.get("attributes");
        assertEquals(expectedAttributes, attributes);
        assertNull(attributes.get("service"));
    }

    public void testExecute_arraysNotFlattened() {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> nestedAgent = new HashMap<>();
        nestedAgent.put("name", "agentNameValue");
        List<String> agentArray = List.of("value1", "value2");
        nestedAgent.put("array", agentArray);
        document.put("agent", nestedAgent);

        Map<String, Object> nestedService = new HashMap<>();
        nestedService.put("name", "serviceNameValue");
        List<String> serviceArray = List.of("value1", "value2");
        nestedService.put("array", serviceArray);
        document.put("service", nestedService);

        IngestDocument ingestDocument = new IngestDocument("index", "id", 1, null, null, document);

        IngestDocument result = processor.execute(ingestDocument);

        Map<String, Object> source = result.getSource();

        Map<String, Object> expectedResourceAttributes = Map.of("agent.name", "agentNameValue", "agent.array", agentArray);

        assertTrue(source.containsKey("resource"));
        Map<String, Object> resource = (Map<String, Object>) source.get("resource");
        Map<String, Object> resourceAttributes = (Map<String, Object>) resource.get("attributes");
        assertEquals(expectedResourceAttributes, resourceAttributes);
        assertNull(resource.get("agent"));

        Map<String, Object> expectedAttributes = Map.of("service.name", "serviceNameValue", "service.array", serviceArray);

        assertTrue(source.containsKey("attributes"));
        Map<String, Object> attributes = (Map<String, Object>) source.get("attributes");
        assertEquals(expectedAttributes, attributes);
        assertNull(attributes.get("service"));
    }
}
