/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.expression;

import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.script.FieldScript;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExpressionFieldScriptTests extends ESTestCase {
    private ExpressionScriptEngine service;
    private SearchLookup lookup;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        NumberFieldMapper.NumberFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.DOUBLE);

        SortedNumericDoubleValues doubleValues = mock(SortedNumericDoubleValues.class);
        when(doubleValues.advanceExact(anyInt())).thenReturn(true);
        when(doubleValues.nextValue()).thenReturn(2.718);

        LeafNumericFieldData atomicFieldData = mock(LeafNumericFieldData.class);
        when(atomicFieldData.getDoubleValues()).thenReturn(doubleValues);

        IndexNumericFieldData fieldData = mock(IndexNumericFieldData.class);
        when(fieldData.getFieldName()).thenReturn("field");
        when(fieldData.load(any())).thenReturn(atomicFieldData);

        service = new ExpressionScriptEngine();
        lookup = new SearchLookup(
            field -> field.equals("field") ? fieldType : null,
            (ignored, _lookup, fdt) -> fieldData,
            new SourceLookup.ReaderSourceProvider()
        );
    }

    private FieldScript.LeafFactory compile(String expression) {
        FieldScript.Factory factory = service.compile(null, expression, FieldScript.CONTEXT, Collections.emptyMap());
        return factory.newFactory(Collections.emptyMap(), lookup);
    }

    public void testCompileError() {
        ScriptException e = expectThrows(ScriptException.class, () -> { compile("doc['field'].value * *@#)(@$*@#$ + 4"); });
        assertTrue(e.getCause() instanceof ParseException);
    }

    public void testLinkError() {
        ScriptException e = expectThrows(ScriptException.class, () -> { compile("doc['nonexistent'].value * 5"); });
        assertTrue(e.getCause() instanceof ParseException);
    }

    public void testFieldAccess() throws IOException {
        FieldScript script = compile("doc['field'].value").newInstance(null);
        script.setDocument(1);

        Object result = script.execute();
        assertThat(result, equalTo(2.718));
    }
}
