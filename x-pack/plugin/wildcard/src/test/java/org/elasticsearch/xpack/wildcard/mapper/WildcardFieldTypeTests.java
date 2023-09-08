/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;

import java.io.IOException;
import java.util.List;

public class WildcardFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        MappedFieldType mapper = new WildcardFieldMapper.Builder("field", IndexVersion.current()).build(MapperBuilderContext.root(false))
            .fieldType();
        assertEquals(List.of("value"), fetchSourceValue(mapper, "value"));
        assertEquals(List.of("42"), fetchSourceValue(mapper, 42L));
        assertEquals(List.of("true"), fetchSourceValue(mapper, true));

        MappedFieldType ignoreAboveMapper = new WildcardFieldMapper.Builder("field", IndexVersion.current()).ignoreAbove(4)
            .build(MapperBuilderContext.root(false))
            .fieldType();
        assertEquals(List.of(), fetchSourceValue(ignoreAboveMapper, "value"));
        assertEquals(List.of("42"), fetchSourceValue(ignoreAboveMapper, 42L));
        assertEquals(List.of("true"), fetchSourceValue(ignoreAboveMapper, true));

        MappedFieldType nullValueMapper = new WildcardFieldMapper.Builder("field", IndexVersion.current()).nullValue("NULL")
            .build(MapperBuilderContext.root(false))
            .fieldType();
        assertEquals(List.of("NULL"), fetchSourceValue(nullValueMapper, null));
    }
}
