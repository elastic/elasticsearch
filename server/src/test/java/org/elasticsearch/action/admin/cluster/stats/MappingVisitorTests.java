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

    private static void collectFieldsAndSubFields(Map<String, ?> mapping, Set<String> fields, Set<String> subFields) {
        MappingVisitor.visitMapping(mapping, (f, m) -> fields.add(f), (f, m) -> subFields.add(f));
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

        Map<String, Object> indexField = new HashMap<>();
        indexField.put("type", "integer");
        properties.put("bar", indexField);
        fields = new HashSet<>();
        collectTypes(mapping, fields);
        assertEquals(new HashSet<>(Arrays.asList("keyword", "integer")), fields);

        properties.put("baz", indexField);
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

    public void testFieldsAndMultiFields() {
        Map<String, Object> keywordType = new HashMap<>();
        keywordType.put("type", "keyword");

        Map<String, Object> textType = new HashMap<>();
        textType.put("type", "text");

        Map<String, Object> multiFields = new HashMap<>();
        multiFields.put("keyword", keywordType);
        textType.put("fields", multiFields);

        Map<String, Object> subObject = new HashMap<>();
        subObject.put("properties", Map.of("baz", keywordType));
        subObject.put("type", "keyword");

        Map<String, Object> properties = new HashMap<>();
        properties.put("foo", textType);
        properties.put("bar", subObject);

        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", properties);

        Set<String> fields = new HashSet<>();
        Set<String> subFields = new HashSet<>();
        collectFieldsAndSubFields(mapping, fields, subFields);
        assertEquals(Set.of("foo", "bar", "bar.baz"), fields);
        assertEquals(Set.of("foo.keyword"), subFields);
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
}
