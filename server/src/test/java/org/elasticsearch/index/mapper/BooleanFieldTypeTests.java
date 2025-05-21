/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class BooleanFieldTypeTests extends FieldTypeTestCase {

    public void testValueFormat() {
        MappedFieldType ft = new BooleanFieldMapper.BooleanFieldType("field");
        assertEquals(false, ft.docValueFormat(null, null).format(0));
        assertEquals(true, ft.docValueFormat(null, null).format(1));
    }

    public void testValueForSearch() {
        MappedFieldType ft = new BooleanFieldMapper.BooleanFieldType("field");
        assertEquals(true, ft.valueForDisplay("T"));
        assertEquals(false, ft.valueForDisplay("F"));
        expectThrows(IllegalArgumentException.class, () -> ft.valueForDisplay(0));
        expectThrows(IllegalArgumentException.class, () -> ft.valueForDisplay("true"));
        expectThrows(IllegalArgumentException.class, () -> ft.valueForDisplay("G"));
    }

    public void testTermQuery() {
        MappedFieldType ft = new BooleanFieldMapper.BooleanFieldType("field");
        assertEquals(new TermQuery(new Term("field", "T")), ft.termQuery("true", MOCK_CONTEXT));
        assertEquals(new TermQuery(new Term("field", "F")), ft.termQuery("false", MOCK_CONTEXT));

        MappedFieldType ft2 = new BooleanFieldMapper.BooleanFieldType("field", false);
        assertEquals(SortedNumericDocValuesField.newSlowExactQuery("field", 1), ft2.termQuery("true", MOCK_CONTEXT));
        assertEquals(SortedNumericDocValuesField.newSlowExactQuery("field", 0), ft2.termQuery("false", MOCK_CONTEXT));

        MappedFieldType unsearchable = new BooleanFieldMapper.BooleanFieldType("field", false, false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("true", MOCK_CONTEXT));
        assertEquals("Cannot search on field [field] since it is not indexed nor has doc values.", e.getMessage());
    }

    public void testRangeQuery() {
        MappedFieldType ft = new BooleanFieldMapper.BooleanFieldType("field");
        Query expected = new TermRangeQuery("field", BooleanFieldMapper.Values.FALSE, BooleanFieldMapper.Values.TRUE, true, true);
        assertEquals(expected, ft.rangeQuery("false", "true", true, true, null, null, null, MOCK_CONTEXT));

        ft = new BooleanFieldMapper.BooleanFieldType("field", false);
        expected = SortedNumericDocValuesField.newSlowRangeQuery("field", 0, 1);
        assertEquals(expected, ft.rangeQuery("false", "true", true, true, null, null, null, MOCK_CONTEXT));

        MappedFieldType unsearchable = new BooleanFieldMapper.BooleanFieldType("field", false, false);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.rangeQuery("false", "true", true, true, null, null, null, MOCK_CONTEXT)
        );
        assertEquals("Cannot search on field [field] since it is not indexed nor has doc values.", e.getMessage());
    }

    public void testFetchSourceValue() throws IOException {

        MappedFieldType fieldType = new BooleanFieldMapper.BooleanFieldType("field");
        assertEquals(List.of(true), fetchSourceValue(fieldType, true));
        assertEquals(List.of(false), fetchSourceValue(fieldType, "false"));
        assertEquals(List.of(false), fetchSourceValue(fieldType, ""));

        MappedFieldType nullFieldType = new BooleanFieldMapper.BooleanFieldType(
            "field",
            true,
            false,
            true,
            true,
            null,
            Collections.emptyMap(),
            false,
            false
        );
        assertEquals(List.of(true), fetchSourceValue(nullFieldType, null));
    }
}
