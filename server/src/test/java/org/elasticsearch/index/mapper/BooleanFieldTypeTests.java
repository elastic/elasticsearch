/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;

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
        assertEquals(new TermQuery(new Term("field", "T")), ft.termQuery("true", null));
        assertEquals(new TermQuery(new Term("field", "F")), ft.termQuery("false", null));

        MappedFieldType unsearchable = new BooleanFieldMapper.BooleanFieldType("field", false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> unsearchable.termQuery("true", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testFetchSourceValue() throws IOException {

        MappedFieldType fieldType = new BooleanFieldMapper.BooleanFieldType("field");
        assertEquals(List.of(true), fetchSourceValue(fieldType, true));
        assertEquals(List.of(false), fetchSourceValue(fieldType, "false"));
        assertEquals(List.of(false), fetchSourceValue(fieldType, ""));

        MappedFieldType nullFieldType = new BooleanFieldMapper.BooleanFieldType(
            "field", true, false, true, true, null, Collections.emptyMap()
        );
        assertEquals(List.of(true), fetchSourceValue(nullFieldType, null));
    }
}
