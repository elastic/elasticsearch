/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.constantkeyword.mapper;

import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.Collections;
import java.util.List;

public class ConstantKeywordFieldTypeTests extends FieldTypeTestCase {

    public void testFetchValue() throws Exception {
        MappedFieldType fieldType = new ConstantKeywordFieldMapper.ConstantKeywordFieldType("field", null);
        ValueFetcher fetcher = fieldType.valueFetcher(null, null, null);

        SourceLookup missingValueLookup = new SourceLookup();
        SourceLookup nullValueLookup = new SourceLookup();
        nullValueLookup.setSource(Collections.singletonMap("field", null));

        assertTrue(fetcher.fetchValues(missingValueLookup).isEmpty());
        assertTrue(fetcher.fetchValues(nullValueLookup).isEmpty());

        MappedFieldType valued = new ConstantKeywordFieldMapper.ConstantKeywordFieldType("field", null);
        fetcher = valued.valueFetcher(null, null, null);

        assertEquals(List.of("foo"), fetcher.fetchValues(missingValueLookup));
        assertEquals(List.of("foo"), fetcher.fetchValues(nullValueLookup));
    }
}
