/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.search.Query;

import static org.hamcrest.Matchers.containsString;

public class SeqNoFieldTypeTests extends FieldTypeTestCase {

    private static Query seqNoRangeQuery(MappedFieldType ft, long lower, long upper) {
        return ft.rangeQuery(lower, upper, true, true, null, null, null, MOCK_CONTEXT);
    }

    public void testRangeQueryWithPoints() {
        MappedFieldType ft = SeqNoFieldMapper.WITH_POINT.fieldType();
        assertEquals(LongPoint.newRangeQuery("_seq_no", 0, 10), seqNoRangeQuery(ft, 0, 10));
    }

    public void testRangeQueryWithDocValuesOnly() {
        MappedFieldType ft = SeqNoFieldMapper.NO_POINT.fieldType();
        assertEquals(NumericDocValuesField.newSlowRangeQuery("_seq_no", 0, 10), seqNoRangeQuery(ft, 0, 10));
    }

    public void testRangeQueryDisabled() {
        MappedFieldType ft = SeqNoFieldMapper.UNSEARCHABLE.fieldType();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> seqNoRangeQuery(ft, 0, 10));
        assertThat(e.getMessage(), containsString("_seq_no cannot be queried when [index.disable_sequence_numbers] is [true]"));
    }

    public void testTermQueryDisabled() {
        MappedFieldType ft = SeqNoFieldMapper.UNSEARCHABLE.fieldType();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ft.termQuery(1, MOCK_CONTEXT));
        assertThat(e.getMessage(), containsString("_seq_no cannot be queried when [index.disable_sequence_numbers] is [true]"));
    }
}
