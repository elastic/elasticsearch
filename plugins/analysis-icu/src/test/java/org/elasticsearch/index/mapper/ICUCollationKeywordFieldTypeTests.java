/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import java.io.IOException;
import java.util.List;

public class ICUCollationKeywordFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {

        ICUCollationKeywordFieldMapper mapper = new ICUCollationKeywordFieldMapper.Builder("field").build(new ContentPath());
        assertEquals(List.of("42"), fetchSourceValue(mapper.fieldType(), 42L));
        assertEquals(List.of("true"), fetchSourceValue(mapper.fieldType(), true));

        ICUCollationKeywordFieldMapper ignoreAboveMapper = new ICUCollationKeywordFieldMapper.Builder("field")
            .ignoreAbove(4)
            .build(new ContentPath());
        assertEquals(List.of(), fetchSourceValue(ignoreAboveMapper.fieldType(), "value"));
        assertEquals(List.of("42"), fetchSourceValue(ignoreAboveMapper.fieldType(), 42L));
        assertEquals(List.of("true"), fetchSourceValue(ignoreAboveMapper.fieldType(), true));

        ICUCollationKeywordFieldMapper nullValueMapper = new ICUCollationKeywordFieldMapper.Builder("field")
            .nullValue("NULL")
            .build(new ContentPath());
        assertEquals(List.of("NULL"), fetchSourceValue(nullValueMapper.fieldType(), null));
    }
}
