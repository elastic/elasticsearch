/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.index.query.QueryShardException;

import java.io.IOException;
import java.util.Arrays;

public class DocCountFieldTypeTests extends FieldTypeTestCase {

    public void testTermQuery() {
        MappedFieldType ft = new DocCountFieldMapper.DocCountFieldType();
        QueryShardException e = expectThrows(QueryShardException.class, () -> ft.termQuery(10L, randomMockContext()));
        assertEquals("Field [_doc_count] of type [_doc_count] is not searchable", e.getMessage());
    }

    public void testRangeQuery() {
        MappedFieldType ft = new DocCountFieldMapper.DocCountFieldType();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ft.rangeQuery(null, null, randomBoolean(), randomBoolean(), null, null, null, null)
        );
        assertEquals("Field [_doc_count] of type [_doc_count] does not support range queries", e.getMessage());
    }

    public void testExistsQuery() {
        MappedFieldType ft = new DocCountFieldMapper.DocCountFieldType();
        QueryShardException e = expectThrows(QueryShardException.class, () -> ft.existsQuery(randomMockContext()));
        assertEquals("Field [_doc_count] of type [_doc_count] does not support exists queries", e.getMessage());
    }

    public void testFetchSourceValue() throws IOException {
        MappedFieldType fieldType = new DocCountFieldMapper.DocCountFieldType();
        assertEquals(Arrays.asList(14), fetchSourceValue(fieldType, 14));
        assertEquals(Arrays.asList(14), fetchSourceValue(fieldType, "14"));
        assertEquals(Arrays.asList(1), fetchSourceValue(fieldType, ""));
        assertEquals(Arrays.asList(1), fetchSourceValue(fieldType, null));
    }
}
