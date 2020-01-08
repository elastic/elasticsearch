/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.constantkeyword;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

public class ConstantKeywordUsageTransportActionTests extends ESTestCase {

    public void testCountTopLevelFields() {
        Map<String, Object> mapping = new HashMap<>();
        assertEquals(0, ConstantKeywordUsageTransportAction.countConstantKeywordFields(mapping));

        Map<String, Object> properties = new HashMap<>();
        mapping.put("properties", properties);

        Map<String, Object> keywordField = new HashMap<>();
        keywordField.put("type", "keyword");
        properties.put("foo", keywordField);
        assertEquals(0, ConstantKeywordUsageTransportAction.countConstantKeywordFields(mapping));

        Map<String, Object> constantKeywordField = new HashMap<>();
        constantKeywordField.put("type", "constant_keyword");
        properties.put("bar", constantKeywordField);
        assertEquals(1, ConstantKeywordUsageTransportAction.countConstantKeywordFields(mapping));

        properties.put("baz", constantKeywordField);
        assertEquals(2, ConstantKeywordUsageTransportAction.countConstantKeywordFields(mapping));
    }

    public void testCountInnerFields() {
        Map<String, Object> constantKeywordField = new HashMap<>();
        constantKeywordField.put("type", "constant_keyword");
        
        Map<String, Object> properties = new HashMap<>();
        properties.put("foo", constantKeywordField);

        Map<String, Object> objectMapping = new HashMap<>();
        objectMapping.put("properties", properties);

        Map<String, Object> mapping = new HashMap<>();
        assertEquals(0, ConstantKeywordUsageTransportAction.countConstantKeywordFields(mapping));

        properties = new HashMap<>();
        properties.put("obj", objectMapping);
        mapping.put("properties", properties);
        assertEquals(1, ConstantKeywordUsageTransportAction.countConstantKeywordFields(mapping));

        properties.put("bar", constantKeywordField);
        assertEquals(2, ConstantKeywordUsageTransportAction.countConstantKeywordFields(mapping));
    }

}
