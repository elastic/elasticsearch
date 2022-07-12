/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.icu;

import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MapperBuilderContext;

import java.io.IOException;
import java.util.List;

public class ICUCollationKeywordFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {

        ICUCollationKeywordFieldMapper mapper = new ICUCollationKeywordFieldMapper.Builder("field").build(MapperBuilderContext.ROOT);
        assertEquals(List.of("42"), fetchSourceValue(mapper.field(), 42L));
        assertEquals(List.of("true"), fetchSourceValue(mapper.field(), true));

        ICUCollationKeywordFieldMapper ignoreAboveMapper = new ICUCollationKeywordFieldMapper.Builder("field").ignoreAbove(4)
            .build(MapperBuilderContext.ROOT);
        assertEquals(List.of(), fetchSourceValue(ignoreAboveMapper.field(), "value"));
        assertEquals(List.of("42"), fetchSourceValue(ignoreAboveMapper.field(), 42L));
        assertEquals(List.of("true"), fetchSourceValue(ignoreAboveMapper.field(), true));

        ICUCollationKeywordFieldMapper nullValueMapper = new ICUCollationKeywordFieldMapper.Builder("field").nullValue("NULL")
            .build(MapperBuilderContext.ROOT);
        assertEquals(List.of("NULL"), fetchSourceValue(nullValueMapper.field(), null));
    }
}
