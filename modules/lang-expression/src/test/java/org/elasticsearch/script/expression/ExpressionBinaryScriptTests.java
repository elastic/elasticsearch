/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.expression;

import org.apache.lucene.expressions.Expression;
import org.apache.lucene.search.DoubleValues;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.BinaryScript;
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

public class ExpressionBinaryScriptTests extends ESTestCase {
    private ExpressionScriptEngine engine;
    private ScriptService scriptService;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        engine = new ExpressionScriptEngine();
        scriptService = new ScriptService(Settings.EMPTY, Map.of("expression", engine), ScriptModule.CORE_CONTEXTS, () -> 1L);
    }

    @SuppressWarnings("unchecked")
    private BinaryScript<Expression> compile(String expression) throws IOException {
        var script = new Script(ScriptType.INLINE, "expression", expression, Collections.emptyMap());
        return scriptService.compile(script, BinaryScript.CONTEXT).newFactory().newInstance();
    }

    public void testCompileError() {
        ScriptException e = expectThrows(ScriptException.class, () -> compile("10 * log(10)"));
        assertTrue(e.getCause() instanceof ParseException);
        assertEquals("Invalid expression '10 * log(10)': Unrecognized function call (log).", e.getCause().getMessage());
    }

    public void testCompileToExpression() throws IOException {
        var expression = compile("10 * log10(10)").execute();
        assertEquals("10 * log10(10)", expression.sourceText);
        assertEquals(10.0, expression.evaluate(new DoubleValues[0]), 0.00001);

        expression = compile("20 * log10(a)").execute();
        assertEquals("20 * log10(a)", expression.sourceText);
        assertEquals(20.0, expression.evaluate(new DoubleValues[] { DoubleValues.withDefault(DoubleValues.EMPTY, 10.0) }), 0.00001);
    }

}
