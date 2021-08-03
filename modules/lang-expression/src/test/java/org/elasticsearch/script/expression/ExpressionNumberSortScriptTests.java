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
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.script.DocValuesReader;
import org.elasticsearch.script.NumberSortScript;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExpressionNumberSortScriptTests extends ESTestCase {
    private ExpressionScriptEngine service;
    private SearchLookup lookup;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        NumberFieldType fieldType = new NumberFieldType("field", NumberType.DOUBLE);

        SortedNumericDoubleValues doubleValues = mock(SortedNumericDoubleValues.class);
        when(doubleValues.advanceExact(anyInt())).thenReturn(true);
        when(doubleValues.nextValue()).thenReturn(2.718);

        LeafNumericFieldData atomicFieldData = mock(LeafNumericFieldData.class);
        when(atomicFieldData.getDoubleValues()).thenReturn(doubleValues);

        IndexNumericFieldData fieldData = mock(IndexNumericFieldData.class);
        when(fieldData.getFieldName()).thenReturn("field");
        when(fieldData.load(anyObject())).thenReturn(atomicFieldData);

        service = new ExpressionScriptEngine();
        lookup = new SearchLookup(field -> field.equals("field") ? fieldType : null, (ignored, lookup) -> fieldData);
    }

    private NumberSortScript.LeafFactory compile(String expression) {
        NumberSortScript.Factory factory =
            service.compile(null, expression, NumberSortScript.CONTEXT, Collections.emptyMap());
        return factory.newFactory(Collections.emptyMap(), lookup);
    }

    public void testCompileError() {
        ScriptException e = expectThrows(ScriptException.class, () -> {
            compile("doc['field'].value * *@#)(@$*@#$ + 4");
        });
        assertTrue(e.getCause() instanceof ParseException);
    }

    public void testLinkError() {
        ScriptException e = expectThrows(ScriptException.class, () -> {
            compile("doc['nonexistent'].value * 5");
        });
        assertTrue(e.getCause() instanceof ParseException);
    }

    public void testFieldAccess() throws IOException {
        NumberSortScript script = compile("doc['field'].value").newInstance(mock(DocValuesReader.class));
        script.setDocument(1);

        double result = script.execute();
        assertEquals(2.718, result, 0.0);
    }
}
