/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.join.mapper;

import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class JoinFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        MappedFieldType fieldType = new ParentJoinFieldMapper.Builder("field").build(MapperBuilderContext.root(false, false)).fieldType();

        Map<String, String> parentValue = Map.of("relation", "parent");
        assertEquals(List.of(parentValue), fetchSourceValue(fieldType, parentValue));

        Map<String, String> childValue = Map.of("relation", "child", "parent", "1");
        assertEquals(List.of(childValue), fetchSourceValue(fieldType, childValue));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> fetchSourceValue(fieldType, parentValue, "format"));
        assertEquals("Field [field] of type [join] doesn't support formats.", e.getMessage());
    }
}
