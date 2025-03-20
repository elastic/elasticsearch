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
import java.util.Optional;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;

public class EcsNamespacingProcessorTests extends ESTestCase {

    public void testInitializeAttributes() {
        IngestDocument doc = createSampleDocument();
        EcsNamespacingProcessor processor = new EcsNamespacingProcessor("testTag", "testDescription");
        processor.initializeAttributes(doc);
        assertNotNull(doc.getFieldValue("attributes", Map.class));
        assertNotNull(doc.getFieldValue("resource.attributes", Map.class));
    }

    public void testInitializeAttributes_MaintainInitialAttributeMaps() {
        EcsNamespacingProcessor processor = new EcsNamespacingProcessor("testTag", "testDescription");
        IngestDocument doc = createSampleDocument();
        doc.setTopLevelFieldValue("resource.attributes", Map.of("foo", "bar"), false, false);
        Map<String, Object> initialAttributes = new HashMap<>();
        doc.setFieldValue("attributes", initialAttributes);
        initialAttributes.put("resource", "initial-resource");
        initialAttributes.put("resource.attributes", "initial-resource-attributes");
        initialAttributes.put("resource.attributes.foo", "initial-resource-attributes-foo");
        initialAttributes.put("foo", "bar");
        Map<String, Object> initialResource = new HashMap<>();
        addOneChildMap("foo", "bar", initialResource, "attributes");
        doc.setFieldValue("resource", initialResource);

        processor.initializeAttributes(doc);

        Object attributes = doc.getFieldValue("attributes", Map.class);
        assertNotNull(attributes);
        assertEquals("bar", doc.getDirectChildFieldValue(attributes, "foo"));
        assertEquals("initial-resource", doc.getDirectChildFieldValue(attributes, "resource"));
        assertEquals(
            List.of("initial-resource-attributes", Map.of("foo", "bar")),
            doc.getDirectChildFieldValue(attributes, "resource.attributes")
        );
        assertEquals("initial-resource-attributes-foo", doc.getDirectChildFieldValue(attributes, "resource.attributes.foo"));
        Object resource = doc.getFieldValue("resource", Map.class);
        assertNotNull(resource);
        Object resourceAttributes = doc.getDirectChildFieldValue(resource, "attributes");
        assertNotNull(resourceAttributes);
        assertEquals("bar", doc.getDirectChildFieldValue(resourceAttributes, "foo"));

        assertFalse(doc.hasTopLevelField("resource.attributes"));
    }

    public void testInitializeAttributes_WithInitialNonMapAttributes() {
        IngestDocument doc = createSampleDocument();
        EcsNamespacingProcessor processor = new EcsNamespacingProcessor("testTag", "testDescription");
        doc.setFieldValue("attributes", 42);
        doc.setFieldValue("resource", "forty-two");
        doc.setTopLevelFieldValue("resource.attributes", 42.0, false, true);
        processor.initializeAttributes(doc);
        Object attributes = doc.getFieldValue("attributes", Map.class);
        assertNotNull(attributes);
        assertEquals(42, doc.getDirectChildFieldValue(attributes, "attributes"));
        // todo: at the moment we always set `attributes.resource` and `attributes.resource.attributes` as lists,
        // even if they contain only one element - is this the desired behavior?
        assertEquals("forty-two", doc.getDirectChildFieldValue(attributes, "resource"));
        assertEquals(42.0, doc.getDirectChildFieldValue(attributes, "resource.attributes"));
        assertNotNull(doc.getFieldValue("resource", Map.class));
        assertNotNull(doc.getFieldValue("resource.attributes", Map.class));
    }

    public void testInitializeAttributes_WithMapsAndNonMapsAttributes() {
        IngestDocument doc = createSampleDocument();
        EcsNamespacingProcessor processor = new EcsNamespacingProcessor("testTag", "testDescription");
        Map<String, Object> initialAttributes = new HashMap<>();
        initialAttributes.put("resource", "value1");
        doc.setFieldValue("attributes", initialAttributes);
        doc.setTopLevelFieldValue("resource", 1, false, true);
        processor.initializeAttributes(doc);
        Object attributes = doc.getFieldValue("attributes", Map.class);
        assertSame(initialAttributes, attributes);
        assertEquals(List.of("value1", 1), doc.getDirectChildFieldValue(attributes, "resource"));
    }

    @SuppressWarnings("unchecked")
    public void testNormalizeSpecialKeys() {
        IngestDocument doc = createSampleDocument();
        EcsNamespacingProcessor processor = new EcsNamespacingProcessor("testTag", "testDescription");

        Object error = doc.getFieldValue("error", Object.class);
        assertNotNull(error);
        Object errorException = doc.getTopLevelFieldValue("error.exception");
        assertNotNull(errorException);
        Object errorExceptionType = doc.getTopLevelFieldValue("error.exception.type");
        assertThat(errorExceptionType, instanceOf(String.class));

        // this resolve all dots as nested fields
        assertEquals("value1", doc.getFieldValue("error.exception.type", String.class));
        assertEquals("value2", doc.getDirectChildFieldValue(error, "exception.type"));
        assertEquals("value3", doc.getDirectChildFieldValue(errorException, "type"));
        assertEquals("value4", doc.getTopLevelFieldValue("error.exception.type"));
        assertEquals("span-id", doc.getFieldValue("span.id", String.class));
        assertFalse(doc.hasTopLevelField("span.id"));

        processor.normalizeSpecialKeys(doc);

        // this resolve all dots as nested fields
        assertFalse(doc.hasField("error.exception.type"));
        assertFalse(doc.hasDirectChildField(error, "exception.type"));
        assertFalse(doc.hasDirectChildField(errorException, "type"));
        errorExceptionType = doc.getTopLevelFieldValue("error.exception.type");
        assertThat(errorExceptionType, instanceOf(List.class));
        assertThat((List<Object>) errorExceptionType, containsInAnyOrder("value1", "value2", "value3", "value4"));
        assertFalse(doc.hasField("span.id"));
        assertEquals("span-id", doc.getTopLevelFieldValue("span.id"));
        assertFalse(doc.hasField("log.level"));
        assertEquals("log-level", doc.getTopLevelFieldValue("log.level"));
    }

    /**
     * Given the sample document below (created through {@link #createSampleDocument()}, the processor should produce the
     * following document:
     * <pre>
     *  {
     *      "@timestamp": 1742375907943,
     *      "body.text": "test message",
     *      "attributes": {
     *          "error": {
     *              "exception": {
     *                  "foo": "bar"
     *              },
     *              "foo": "bar"
     *          },
     *          "error.exception": {
     *              "foo": "bar"
     *          },
     *          "error.exception.type": ["value1", "value2", "value3", "value4"],
     *           "service": {
     *              "name": "service-name"
     *           },
     *          "service.type": "service-type"
     *          "user.name": "user-name",
     *          "user.id": "user-id",
     *          "attributes.foo": "bar",
     *          "resource.attributes.foo": "bar"
     *      },
     *      "resource": {
     *          "attributes": {
     *              "agent": {
     *                  "name": "agent-name"
     *              },
     *              "agent.type": "agent-type",
     *              "host": {
     *                  "name": "host-name"
     *              },
     *              "host.type": "host-type",
     *              "cloud": {
     *                  "region": "cloud-region"
     *              },
     *              "cloud.provider": "cloud-provider",
     *          }
     *      },
     *      "span_id": "span-id",
     *      "severity_text": "log-level",
     *      "trace_id": "trace-id"
     *  }
     *  </pre>
     */
    @SuppressWarnings("unchecked")
    public void testProcessor() {
        IngestDocument doc = createSampleDocument();
        EcsNamespacingProcessor processor = new EcsNamespacingProcessor("testTag", "testDescription");
        processor.execute(doc);

        assertEquals(Optional.of(1742375907943L).get(), doc.getFieldValue("@timestamp", Long.class));
        assertEquals("test message", doc.getTopLevelFieldValue("body.text"));

        Map<String, Object> attributes = doc.getFieldValue("attributes", Map.class);
        assertNotNull(attributes);

        Map<String, Object> error = (Map<String, Object>) attributes.get("error");
        assertNotNull(error);
        Map<String, Object> errorException = (Map<String, Object>) error.get("exception");
        assertNotNull(errorException);
        assertEquals("bar", errorException.get("foo"));
        assertEquals("bar", error.get("foo"));

        Map<String, Object> errorExceptionMap = (Map<String, Object>) attributes.get("error.exception");
        assertNotNull(errorExceptionMap);
        assertEquals("bar", errorExceptionMap.get("foo"));

        List<String> errorExceptionType = (List<String>) attributes.get("error.exception.type");
        assertNotNull(errorExceptionType);
        assertThat(errorExceptionType, containsInAnyOrder("value1", "value2", "value3", "value4"));

        Map<String, Object> service = (Map<String, Object>) attributes.get("service");
        assertNotNull(service);
        assertEquals("service-name", service.get("name"));
        assertEquals("service-type", attributes.get("service.type"));

        assertEquals("user-name", attributes.get("user.name"));
        assertEquals("user-id", attributes.get("user.id"));
        assertEquals("bar", attributes.get("attributes.foo"));
        assertEquals("bar", attributes.get("resource.attributes.foo"));

        Map<String, Object> resource = doc.getFieldValue("resource", Map.class);
        assertNotNull(resource);
        Map<String, Object> resourceAttributes = (Map<String, Object>) resource.get("attributes");
        assertNotNull(resourceAttributes);

        Map<String, Object> agent = (Map<String, Object>) resourceAttributes.get("agent");
        assertNotNull(agent);
        assertEquals("agent-name", agent.get("name"));
        assertEquals("agent-type", resourceAttributes.get("agent.type"));

        Map<String, Object> host = (Map<String, Object>) resourceAttributes.get("host");
        assertNotNull(host);
        assertEquals("host-name", host.get("name"));
        assertEquals("host-type", resourceAttributes.get("host.type"));

        Map<String, Object> cloud = (Map<String, Object>) resourceAttributes.get("cloud");
        assertNotNull(cloud);
        assertEquals("cloud-region", cloud.get("region"));
        assertEquals("cloud-provider", resourceAttributes.get("cloud.provider"));

        assertEquals("span-id", doc.getFieldValue("span_id", String.class));
        assertEquals("log-level", doc.getFieldValue("severity_text", String.class));
        assertEquals("trace-id", doc.getFieldValue("trace_id", String.class));

        // verify that all other fields are removed
        assertFalse(doc.hasField("message"));
        assertFalse(doc.hasField("body"));
        assertFalse(doc.hasField("error"));
        assertFalse(doc.hasField("error.exception"));
        assertFalse(doc.hasField("error.exception.type"));
        assertFalse(doc.hasField("log.level"));
        assertFalse(doc.hasField("span"));
        assertFalse(doc.hasField("agent"));
        assertFalse(doc.hasField("agent.type"));
        assertFalse(doc.hasField("host"));
        assertFalse(doc.hasField("host.type"));
        assertFalse(doc.hasField("cloud"));
        assertFalse(doc.hasField("cloud.provider"));
        assertFalse(doc.hasField("service"));
        assertFalse(doc.hasField("service.type"));
        assertFalse(doc.hasField("user.name"));
        assertFalse(doc.hasField("user.id"));
        assertFalse(doc.hasField("attributes.foo"));
        assertFalse(doc.hasField("resource.attributes.foo"));
    }

    /**
     * Constructs a test document with the following structure:
     * <pre>
     * {
     *     "@timestamp": 1742375907943,
     *     "message": "test message",
     *     "body": {
     *         "sub-message": "text sub message"
     *     },
     *     "error": {
     *         "exception": {
     *             "type": "value1",
     *             "foo": "bar"
     *         },
     *         "exception.type": "value2",
     *         "foo": "bar"
     *     },
     *     "error.exception": {
     *         "type": "value3",
     *         "foo": "bar"
     *     },
     *     "error.exception.type": "value4",
     *     "span": {
     *         "id": "span-id"
     *     },
     *     "log.level": "log-level",
     *     "trace_id": "trace-id",
     *     "agent": {
     *         "name": "agent-name"
     *     },
     *     "agent.type": "agent-type",
     *     "host": {
     *         "name": "host-name"
     *     },
     *     "host.type": "host-type",
     *     "cloud": {
     *         "region": "cloud-region"
     *     },
     *     "cloud.provider": "cloud-provider",
     *     "service": {
     *         "name": "service-name"
     *     },
     *     "service.type": "service-type",
     *     "user.name": "user-name",
     *     "user.id": "user-id",
     *     "attributes.foo": "bar",
     *     "resource.attributes.foo": "bar"
     * }
     * </pre>
     * @return the test document
     */
    private static IngestDocument createSampleDocument() {
        Map<String, Object> document = new HashMap<>();
        document.put("@timestamp", 1742375907943L);
        document.put("message", "test message");
        Map<String, Object> error = new HashMap<>();
        Map<String, Object> exception = new HashMap<>();
        exception.put("type", "value1");
        exception.put("foo", "bar");
        error.put("exception", exception);
        error.put("exception.type", "value2");
        error.put("foo", "bar");
        document.put("error", error);
        Map<String, Object> errorException = new HashMap<>();
        errorException.put("type", "value3");
        errorException.put("foo", "bar");
        document.put("error.exception", errorException);
        document.put("error.exception.type", "value4");
        addOneChildMap("id", "span-id", document, "span");
        document.put("log.level", "log-level");
        document.put("trace_id", "trace-id");
        addOneChildMap("name", "agent-name", document, "agent");
        document.put("agent.type", "agent-type");
        addOneChildMap("name", "host-name", document, "host");
        document.put("host.type", "host-type");
        addOneChildMap("region", "cloud-region", document, "cloud");
        document.put("cloud.provider", "cloud-provider");
        addOneChildMap("name", "service-name", document, "service");
        document.put("service.type", "service-type");
        document.put("user.name", "user-name");
        document.put("user.id", "user-id");
        document.put("attributes.foo", "bar");
        document.put("resource.attributes.foo", "bar");
        return new IngestDocument("index", "id", 1, null, null, document);
    }

    private static void addOneChildMap(String childName, String value, Map<String, Object> document, String parentName) {
        Map<String, Object> parent = new HashMap<>();
        parent.put(childName, value);
        document.put(parentName, parent);
    }
}
