/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.elasticsearch.script.ScriptContext;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class DoubleScriptFieldScriptTests extends ScriptFieldScriptTestCase<DoubleScriptFieldScript.Factory> {
    public static final DoubleScriptFieldScript.Factory DUMMY = (params, lookup) -> ctx -> new DoubleScriptFieldScript(
        params,
        lookup,
        ctx
    ) {
        @Override
        public double[] execute() {
            return new double[] { 1.0 };
        }
    };

    @Override
    protected ScriptContext<DoubleScriptFieldScript.Factory> context() {
        return DoubleScriptFieldScript.CONTEXT;
    }

    @Override
    protected DoubleScriptFieldScript.Factory dummyScript() {
        return DUMMY;
    }

    public void testConvertDouble() {
        double v = randomDouble();
        assertThat(DoubleScriptFieldScript.convertFromDouble(v), equalTo(new double[] { v }));
        assertThat(DoubleScriptFieldScript.convertFromDef(v), equalTo(new double[] { v }));
    }

    public void testConvertFromCollection() {
        double d = randomDouble();
        long l = randomLong();
        int i = randomInt();
        assertThat(DoubleScriptFieldScript.convertFromCollection(List.of(d, l, i)), equalTo(new double[] { d, l, i }));
        assertThat(DoubleScriptFieldScript.convertFromDef(List.of(d, l, i)), equalTo(new double[] { d, l, i }));
    }

    public void testConvertDoubleArrayFromDef() {
        double[] a = new double[] { 1, 2, 3 };
        assertThat(DoubleScriptFieldScript.convertFromDef(a), equalTo(a));
    }

    public void testConvertNumberFromDef() {
        for (Number n : new Number[] { randomByte(), randomShort(), randomInt(), randomLong(), randomFloat() }) {
            assertThat(DoubleScriptFieldScript.convertFromDef(n), equalTo(new double[] { n.doubleValue() }));
        }
    }
}
