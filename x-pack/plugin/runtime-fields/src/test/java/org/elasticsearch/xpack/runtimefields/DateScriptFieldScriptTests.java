/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.elasticsearch.script.ScriptContext;

import java.time.Instant;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class DateScriptFieldScriptTests extends ScriptFieldScriptTestCase<DateScriptFieldScript.Factory> {
    public static final DateScriptFieldScript.Factory DUMMY = (params, lookup, formatter) -> ctx -> new DateScriptFieldScript(
        params,
        lookup,
        formatter,
        ctx
    ) {
        @Override
        public long[] execute() {
            return new long[] { 1595431354874L };
        }
    };

    @Override
    protected ScriptContext<DateScriptFieldScript.Factory> context() {
        return DateScriptFieldScript.CONTEXT;
    }

    @Override
    protected DateScriptFieldScript.Factory dummyScript() {
        return DUMMY;
    }

    public void testConvertFromLong() {
        long l = randomLong();
        assertThat(DateScriptFieldScript.convertFromLong(l), equalTo(new long[] { l }));
        assertThat(DateScriptFieldScript.convertFromDef(l), equalTo(new long[] { l }));
    }

    public void testConvertFromTemporalAccessor() {
        Instant instant = randomInstant();
        assertThat(DateScriptFieldScript.convertFromTemporalAccessor(instant), equalTo(new long[] { instant.toEpochMilli() }));
        assertThat(DateScriptFieldScript.convertFromDef(instant), equalTo(new long[] { instant.toEpochMilli() }));
    }

    public void testConvertFromTemporalAccessorArray() {
        Instant[] instants = new Instant[] { randomInstant(), randomInstant() };
        long[] expected = new long[] { instants[0].toEpochMilli(), instants[1].toEpochMilli() };
        assertThat(DateScriptFieldScript.convertFromTemporalAccessorArray(instants), equalTo(expected));
        assertThat(DateScriptFieldScript.convertFromDef(instants), equalTo(expected));
    }

    private Instant randomInstant() {
        return Instant.ofEpochMilli(randomLongBetween(0, 1595431354874L));
    }

    public void testConvertFromCollection() {
        long l = randomLong();
        int i = randomInt();
        short s = randomShort();
        byte b = randomByte();
        Instant instant = randomInstant();
        List<?> collection = List.of(l, i, s, b, instant);
        long[] result = new long[] { l, i, s, b, instant.toEpochMilli() };
        assertThat(DateScriptFieldScript.convertFromCollection(collection), equalTo(result));
        assertThat(DateScriptFieldScript.convertFromDef(collection), equalTo(result));
    }

    public void testConvertFromCollectionBad() {
        Exception e = expectThrows(ClassCastException.class, () -> DateScriptFieldScript.convertFromCollection(List.of(1.0)));
        assertThat(e.getMessage(), equalTo(badCollectionConversionMessage("java.lang.Double")));
        e = expectThrows(ClassCastException.class, () -> DateScriptFieldScript.convertFromDef(List.of(1.0)));
        assertThat(e.getMessage(), equalTo(badCollectionConversionMessage("java.lang.Double")));
        e = expectThrows(ClassCastException.class, () -> DateScriptFieldScript.convertFromCollection(List.of(1.0F)));
        assertThat(e.getMessage(), equalTo(badCollectionConversionMessage("java.lang.Float")));
        e = expectThrows(ClassCastException.class, () -> DateScriptFieldScript.convertFromDef(List.of(1.0F)));
        assertThat(e.getMessage(), equalTo(badCollectionConversionMessage("java.lang.Float")));
        e = expectThrows(ClassCastException.class, () -> DateScriptFieldScript.convertFromCollection(List.of("1.0")));
        assertThat(e.getMessage(), equalTo(badCollectionConversionMessage("java.lang.String")));
        e = expectThrows(ClassCastException.class, () -> DateScriptFieldScript.convertFromDef(List.of("1.0")));
        assertThat(e.getMessage(), equalTo(badCollectionConversionMessage("java.lang.String")));
    }

    private String badCollectionConversionMessage(String cls) {
        return "Exception casting collection member [1.0]: Can't cast [" + cls + "] to long, TemporalAccessor, int, short, or byte";
    }

    public void testConvertFromLongArrayFromDef() {
        long[] a = new long[] { 1, 2, 3 };
        assertThat(DateScriptFieldScript.convertFromDef(a), equalTo(a));
    }

    public void testConvertFromNumberInDef() {
        for (Number n : new Number[] { randomByte(), randomShort(), randomInt() }) {
            assertThat(DateScriptFieldScript.convertFromDef(n), equalTo(new long[] { n.longValue() }));
        }
    }

    public void testConvertFromDefBad() {
        Exception e = expectThrows(ClassCastException.class, () -> DateScriptFieldScript.convertFromDef(1.0));
        assertThat(e.getMessage(), equalTo(badDefConversionMessage("java.lang.Double")));
        e = expectThrows(ClassCastException.class, () -> DateScriptFieldScript.convertFromDef(1.0F));
        assertThat(e.getMessage(), equalTo(badDefConversionMessage("java.lang.Float")));
        e = expectThrows(ClassCastException.class, () -> DateScriptFieldScript.convertFromDef("1.0"));
        assertThat(e.getMessage(), equalTo(badDefConversionMessage("java.lang.String")));
    }

    private String badDefConversionMessage(String cls) {
        return "Can't cast [" + cls + "] to long, long[], TemporalAccessor, TemporalAccessor[], int, short, byte, or a collection";
    }
}
