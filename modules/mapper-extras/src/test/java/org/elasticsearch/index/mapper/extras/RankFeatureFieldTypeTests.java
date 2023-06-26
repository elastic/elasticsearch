/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class RankFeatureFieldTypeTests extends FieldTypeTestCase {

    public void testIsNotAggregatable() {
        MappedFieldType fieldType = new RankFeatureFieldMapper.RankFeatureFieldType("field", Collections.emptyMap(), true, null);
        assertFalse(fieldType.isAggregatable());
    }

    public void testFetchSourceValue() throws IOException {
        MappedFieldType mapper = new RankFeatureFieldMapper.Builder("field").nullValue(2.0f)
            .build(MapperBuilderContext.root(false))
            .fieldType();

        assertEquals(List.of(3.14f), fetchSourceValue(mapper, 3.14));
        assertEquals(List.of(42.9f), fetchSourceValue(mapper, "42.9"));
        assertEquals(List.of(2.0f), fetchSourceValue(mapper, null));
    }
}
