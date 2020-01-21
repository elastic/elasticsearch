/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        MappingVisitor.visitMapping(mapping,
                m -> {
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
}
