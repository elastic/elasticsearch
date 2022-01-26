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
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;

import java.io.IOException;
import java.util.Collections;

public class AnnotatedTextFieldTypeTests extends FieldTypeTestCase {

    public void testIntervals() throws IOException {
        MappedFieldType ft = new AnnotatedTextFieldMapper.AnnotatedTextFieldType("field", Collections.emptyMap());
        IntervalsSource source = ft.termIntervals(new BytesRef("donald"), null);
        assertEquals(Intervals.term("donald"), source);
    }

    public void testFetchSourceValue() throws IOException {
        MappedFieldType fieldType = new AnnotatedTextFieldMapper.Builder("field", createDefaultIndexAnalyzers()).build(
            MapperBuilderContext.ROOT
        ).fieldType();

        assertEquals(Collections.singletonList("value"), fetchSourceValue(fieldType, "value"));
        assertEquals(Collections.singletonList("42"), fetchSourceValue(fieldType, 42L));
        assertEquals(Collections.singletonList("true"), fetchSourceValue(fieldType, true));
    }
}
