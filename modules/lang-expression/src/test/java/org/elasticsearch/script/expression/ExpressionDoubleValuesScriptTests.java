/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.expression;

import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.DoubleValuesScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;
import java.util.Map;

/**
 * Tests {@link ExpressionDoubleValuesScript} through the {@link ScriptService}
 */
public class ExpressionDoubleValuesScriptTests extends ESTestCase {
    private ExpressionScriptEngine engine;
    private ScriptService scriptService;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        engine = new ExpressionScriptEngine();
        scriptService = new ScriptService(Settings.EMPTY, Map.of("expression", engine), ScriptModule.CORE_CONTEXTS, () -> 1L);
    }

    @SuppressWarnings("unchecked")
    private DoubleValuesScript compile(String expression) {
        var script = new Script(ScriptType.INLINE, "expression", expression, Collections.emptyMap());
        return scriptService.compile(script, DoubleValuesScript.CONTEXT).newInstance();
    }

    public void testCompileError() {
        ScriptException e = expectThrows(ScriptException.class, () -> compile("10 * log(10)"));
        assertTrue(e.getCause() instanceof ParseException);
        assertEquals("Invalid expression '10 * log(10)': Unrecognized function call (log).", e.getCause().getMessage());
    }

    public void testEvaluate() {
        var expression = compile("10 * log10(10)");
        assertEquals("10 * log10(10)", expression.sourceText());
        assertEquals(10.0, expression.evaluate(new DoubleValues[0]), 0.00001);
        assertEquals(10.0, expression.execute(), 0.00001);

        expression = compile("20 * log10(a)");
        assertEquals("20 * log10(a)", expression.sourceText());
        assertEquals(20.0, expression.evaluate(new DoubleValues[] { DoubleValues.withDefault(DoubleValues.EMPTY, 10.0) }), 0.00001);
    }

    public void testDoubleValuesSource() throws IOException {
        SimpleBindings bindings = new SimpleBindings();
        bindings.add("popularity", DoubleValuesSource.constant(5));

        var expression = compile("10 * log10(popularity)");
        var doubleValues = expression.getDoubleValuesSource((name) -> bindings.getDoubleValuesSource(name));
        assertEquals("expr(10 * log10(popularity))", doubleValues.toString());
        var values = doubleValues.getValues(null, null);
        assertTrue(values.advanceExact(0));
        assertEquals(6, (int) values.doubleValue());

        var sortField = expression.getSortField((name) -> bindings.getDoubleValuesSource(name), false);
        assertEquals("expr(10 * log10(popularity))", sortField.getField());
        assertEquals(SortField.Type.CUSTOM, sortField.getType());
        assertFalse(sortField.getReverse());

        var rescorer = expression.getRescorer((name) -> bindings.getDoubleValuesSource(name));
        assertNotNull(rescorer);
    }

}
