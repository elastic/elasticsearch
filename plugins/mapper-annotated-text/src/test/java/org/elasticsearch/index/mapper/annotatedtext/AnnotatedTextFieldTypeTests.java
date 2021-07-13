/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.annotatedtext;

import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class AnnotatedTextFieldTypeTests extends FieldTypeTestCase {

    public void testIntervals() throws IOException {
        MappedFieldType ft = new AnnotatedTextFieldMapper.AnnotatedTextFieldType("field", Collections.emptyMap());
        IntervalsSource source = ft.termIntervals(new BytesRef("donald"), null);
        assertEquals(Intervals.term("donald"), source);
    }

    public void testFetchSourceValue() throws IOException {
        MappedFieldType fieldType = new AnnotatedTextFieldMapper.Builder("field", createDefaultIndexAnalyzers())
            .build(new ContentPath())
            .fieldType();

        assertEquals(List.of("value"), fetchSourceValue(fieldType, "value"));
        assertEquals(List.of("42"), fetchSourceValue(fieldType, 42L));
        assertEquals(List.of("true"), fetchSourceValue(fieldType, true));
    }
}
