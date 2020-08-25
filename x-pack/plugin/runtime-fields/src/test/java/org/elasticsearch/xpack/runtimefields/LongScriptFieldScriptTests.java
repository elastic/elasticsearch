/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.elasticsearch.script.ScriptContext;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class LongScriptFieldScriptTests extends ScriptFieldScriptTestCase<LongScriptFieldScript.Factory> {
    public static final LongScriptFieldScript.Factory DUMMY = (params, lookup) -> ctx -> new LongScriptFieldScript(params, lookup, ctx) {
        @Override
        public long[] execute() {
            return new long[] { 1 };
        }
    };

    @Override
    protected ScriptContext<LongScriptFieldScript.Factory> context() {
        return LongScriptFieldScript.CONTEXT;
    }

    @Override
    protected LongScriptFieldScript.Factory dummyScript() {
        return DUMMY;
    }

    public void testConvertFromLong() {
        long l = randomLong();
        assertThat(LongScriptFieldScript.convertFromLong(l), equalTo(new long[] { l }));
        assertThat(LongScriptFieldScript.convertFromDef(l), equalTo(new long[] { l }));
    }

    public void testConvertFromCollection() {
        long l = randomLong();
        int i = randomInt();
        short s = randomShort();
        byte b = randomByte();
        List<?> collection = List.of(l, i, s, b);
        long[] result = new long[] { l, i, s, b };
        assertThat(LongScriptFieldScript.convertFromCollection(collection), equalTo(result));
        assertThat(LongScriptFieldScript.convertFromDef(collection), equalTo(result));
    }

    public void testConvertFromCollectionBad() {
        Exception e = expectThrows(ClassCastException.class, () -> LongScriptFieldScript.convertFromCollection(List.of(1.0)));
        assertThat(e.getMessage(), equalTo(badCollectionConversionMessage("java.lang.Double")));
        e = expectThrows(ClassCastException.class, () -> LongScriptFieldScript.convertFromDef(List.of(1.0)));
        assertThat(e.getMessage(), equalTo(badCollectionConversionMessage("java.lang.Double")));
        e = expectThrows(ClassCastException.class, () -> LongScriptFieldScript.convertFromCollection(List.of(1.0F)));
        assertThat(e.getMessage(), equalTo(badCollectionConversionMessage("java.lang.Float")));
        e = expectThrows(ClassCastException.class, () -> LongScriptFieldScript.convertFromDef(List.of(1.0F)));
        assertThat(e.getMessage(), equalTo(badCollectionConversionMessage("java.lang.Float")));
        e = expectThrows(ClassCastException.class, () -> LongScriptFieldScript.convertFromCollection(List.of("1.0")));
        assertThat(e.getMessage(), equalTo(badCollectionConversionMessage("java.lang.String")));
        e = expectThrows(ClassCastException.class, () -> LongScriptFieldScript.convertFromDef(List.of("1.0")));
        assertThat(e.getMessage(), equalTo(badCollectionConversionMessage("java.lang.String")));
    }

    private String badCollectionConversionMessage(String cls) {
        return "Exception casting collection member [1.0]: Can't cast [" + cls + "] to long, int, short, or byte";
    }

    public void testConvertFromLongArrayFromDef() {
        long[] a = new long[] { 1, 2, 3 };
        assertThat(LongScriptFieldScript.convertFromDef(a), equalTo(a));
    }

    public void testConvertFromNumberInDef() {
        for (Number n : new Number[] { randomByte(), randomShort(), randomInt() }) {
            assertThat(LongScriptFieldScript.convertFromDef(n), equalTo(new long[] { n.longValue() }));
        }
    }

    public void testConvertFromDefBad() {
        Exception e = expectThrows(ClassCastException.class, () -> LongScriptFieldScript.convertFromDef(1.0));
        assertThat(e.getMessage(), equalTo(badDefConversionMessage("java.lang.Double")));
        e = expectThrows(ClassCastException.class, () -> LongScriptFieldScript.convertFromDef(1.0F));
        assertThat(e.getMessage(), equalTo(badDefConversionMessage("java.lang.Float")));
        e = expectThrows(ClassCastException.class, () -> LongScriptFieldScript.convertFromDef("1.0"));
        assertThat(e.getMessage(), equalTo(badDefConversionMessage("java.lang.String")));
    }

    private String badDefConversionMessage(String cls) {
        return "Can't cast [" + cls + "] to long, long[], int, short, byte, or a collection";
    }
}
