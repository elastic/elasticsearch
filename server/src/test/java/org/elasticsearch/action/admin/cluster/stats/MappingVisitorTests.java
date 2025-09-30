/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MappingVisitorTests extends ESTestCase {

    private static void collectTypes(Map<String, ?> mapping, Set<String> types) {
        MappingVisitor.visitMapping(mapping, (f, m) -> {
            if (m.containsKey("type")) {
                types.add(m.get("type").toString());
            } else {
                types.add("object");
            }
        });
    }

    public void testCountTopLevelFields() {
        Map<String, Object> mapping = new HashMap<>();
        Set<String> fields = new HashSet<>();
        collectTypes(mapping, fields);
        assertEquals(Collections.emptySet(), fields);

        Map<String, Object> properties = new HashMap<>();
        mapping.put("properties", properties);

        Map<String, Object> keywordField = new HashMap<>();
        keywordField.put("type", "keyword");
        properties.put("foo", keywordField);
        collectTypes(mapping, fields);
        assertEquals(Collections.singleton("keyword"), fields);

        Map<String, Object> IndexField = new HashMap<>();
        IndexField.put("type", "integer");
        properties.put("bar", IndexField);
        fields = new HashSet<>();
        collectTypes(mapping, fields);
        assertEquals(new HashSet<>(Arrays.asList("keyword", "integer")), fields);

        properties.put("baz", IndexField);
        fields = new HashSet<>();
        collectTypes(mapping, fields);
        assertEquals(new HashSet<>(Arrays.asList("keyword", "integer")), fields);
    }

    public void testCountMultiFields() {
        Map<String, Object> keywordField = new HashMap<>();
        keywordField.put("type", "keyword");

        Map<String, Object> textField = new HashMap<>();
        textField.put("type", "text");

        Map<String, Object> fields = new HashMap<>();
        fields.put("keyword", keywordField);
        textField.put("fields", fields);

        Map<String, Object> properties = new HashMap<>();
        properties.put("foo", textField);

        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", properties);

        Set<String> usedFields = new HashSet<>();
        collectTypes(mapping, usedFields);
        assertEquals(new HashSet<>(Arrays.asList("keyword", "text")), usedFields);
    }

    public void testCountInnerFields() {
        Map<String, Object> keywordField = new HashMap<>();
        keywordField.put("type", "keyword");

        Map<String, Object> properties = new HashMap<>();
        properties.put("foo", keywordField);

        Map<String, Object> objectMapping = new HashMap<>();
        objectMapping.put("properties", properties);

        Map<String, Object> mapping = new HashMap<>();

        properties = new HashMap<>();
        properties.put("obj", objectMapping);
        mapping.put("properties", properties);
        Set<String> fields = new HashSet<>();
        collectTypes(mapping, fields);
        assertEquals(new HashSet<>(Arrays.asList("keyword", "object")), fields);

        properties.put("bar", keywordField);
        fields = new HashSet<>();
        collectTypes(mapping, fields);
        assertEquals(new HashSet<>(Arrays.asList("keyword", "object")), fields);
    }

    public void testCountRuntimeFields() {
        Map<String, Object> mapping = new HashMap<>();
        Set<String> fields = new HashSet<>();
        collectRuntimeTypes(mapping, fields);
        assertEquals(Collections.emptySet(), fields);

        Map<String, Object> properties = new HashMap<>();
        mapping.put("runtime", properties);

        Map<String, Object> keywordField = new HashMap<>();
        keywordField.put("type", "keyword");
        properties.put("foo", keywordField);
        collectRuntimeTypes(mapping, fields);
        assertEquals(Collections.singleton("keyword"), fields);

        Map<String, Object> runtimeField = new HashMap<>();
        runtimeField.put("type", "long");
        properties.put("bar", runtimeField);
        fields = new HashSet<>();
        collectRuntimeTypes(mapping, fields);
        assertEquals(new HashSet<>(Arrays.asList("keyword", "long")), fields);

        properties.put("baz", runtimeField);
        fields = new HashSet<>();
        collectRuntimeTypes(mapping, fields);
        assertEquals(new HashSet<>(Arrays.asList("keyword", "long")), fields);
    }

    private static void collectRuntimeTypes(Map<String, ?> mapping, Set<String> types) {
        MappingVisitor.visitRuntimeMapping(mapping, (f, m) -> types.add(m.get("type").toString()));
    }

    @SuppressWarnings("unchecked")
    public void testConvertLongToKeyword() {
        Map<String, Object> longType = Map.of("type", "long");
        Map<String, Object> textType = Map.of("type", "text");
        Map<String, Object> floatType = Map.of("type", "float", "scaling_factor", 1000);
        Map<String, Object> multiField = Map.of("type", "keyword", "fields", Map.of("my-long", longType, "my-float", floatType));
        Map<String, Object> objectField = Map.of("type", "keyword", "properties", Map.of("my-text", textType, "my-long", longType));
        Map<String, Object> expectedProperties = Map.of(
            "properties",
            Map.of("my-long", longType, "my-float", floatType, "my-multi-field", multiField, "my-object", objectField)
        );

        HashMap<String, Object> result = new HashMap<>();
        MappingVisitor.visitAndCopyMapping(expectedProperties, result, (fieldName, source, dest) -> {
            for (String key : source.keySet()) {
                if (key.equals("type") && source.get(key).equals("long")) {
                    dest.put(key, "keyword");
                } else {
                    dest.put(key, source.get(key));
                }
            }
        });

        assertTrue(result.containsKey("properties"));
        Map<String, Object> properties = (Map<String, Object>) result.get("properties");

        assertTrue(properties.containsKey("my-long"));
        Map<String, Object> myLong = (Map<String, Object>) properties.get("my-long");
        assertEquals("keyword", myLong.get("type"));

        assertTrue(properties.containsKey("my-float"));
        Map<String, Object> myFloat = (Map<String, Object>) properties.get("my-float");
        assertEquals("float", myFloat.get("type"));
        assertEquals(1000, myFloat.get("scaling_factor"));

        assertTrue(properties.containsKey("my-multi-field"));
        Map<String, Object> myMultiField = (Map<String, Object>) properties.get("my-multi-field");
        assertEquals("keyword", myMultiField.get("type"));
        assertTrue(myMultiField.containsKey("fields"));
        Map<String, Object> foundFields = (Map<String, Object>) myMultiField.get("fields");
        assertTrue(foundFields.containsKey("my-long"));
        assertEquals("keyword", ((Map<String, Object>) foundFields.get("my-long")).get("type"));
        assertTrue(foundFields.containsKey("my-float"));
        assertEquals("float", ((Map<String, Object>) foundFields.get("my-float")).get("type"));
        assertEquals(1000, ((Map<String, Object>) foundFields.get("my-float")).get("scaling_factor"));

        assertTrue(properties.containsKey("my-object"));
        Map<String, Object> myObject = (Map<String, Object>) properties.get("my-object");
        assertEquals("keyword", myObject.get("type"));
        assertTrue(myObject.containsKey("properties"));
        Map<String, Object> foundSubObjects = (Map<String, Object>) myObject.get("properties");
        assertTrue(foundSubObjects.containsKey("my-long"));
        assertEquals("keyword", ((Map<String, Object>) foundSubObjects.get("my-long")).get("type"));
        assertTrue(foundSubObjects.containsKey("my-text"));
        assertEquals("text", ((Map<String, Object>) foundSubObjects.get("my-text")).get("type"));
    }
}
