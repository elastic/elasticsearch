/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.startsWith;

public class DefEncodingTests extends ESTestCase {
    public void testShortEncoding() {
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> Def.Encoding.parse(""));
        assertThat(expected.getMessage(), hasToString(startsWith("Encoding too short. Minimum 6")));

        expected = expectThrows(IllegalArgumentException.class, () -> Def.Encoding.parse("S"));
        assertThat(expected.getMessage(), hasToString(startsWith("Encoding too short. Minimum 6")));

        expected = expectThrows(IllegalArgumentException.class, () -> Def.Encoding.parse("St"));
        assertThat(expected.getMessage(), hasToString(startsWith("Encoding too short. Minimum 6")));

        expected = expectThrows(IllegalArgumentException.class, () -> Def.Encoding.parse("St.,"));
        assertThat(expected.getMessage(), hasToString(startsWith("Encoding too short. Minimum 6")));

        expected = expectThrows(IllegalArgumentException.class, () -> Def.Encoding.parse("St.a,"));
        assertThat(expected.getMessage(), hasToString(startsWith("Encoding too short. Minimum 6")));
    }

    public void testStaticEncoding() {
        assertTrue(Def.Encoding.isStatic("St.a,0"));
        assertFalse(Def.Encoding.isStatic("Df.a,0"));
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> Def.Encoding.isStatic("qt.a,0"));
        assertThat(expected.getMessage(), hasToString(startsWith("Invalid static specifier at position 0, expected 'S' or 'D', not [q]")));
    }

    public void testNeedsInstanceEncoding() {
        assertTrue(Def.Encoding.needsInstance("St.a,0"));
        assertFalse(Def.Encoding.needsInstance("Df.a,0"));
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
            () -> Def.Encoding.needsInstance("Dq.a,0"));
        assertThat(expected.getMessage(),
            hasToString(startsWith("Invalid needsInstance specifier at position 1, expected 't' or 'f', not [q]")));
    }

    public void testSymbolEncoding() {
        assertEquals("", Def.Encoding.symbol("St.a,0"));
        assertEquals("this", Def.Encoding.symbol("Stthis.a,0"));
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
            () -> Def.Encoding.symbol("Dfa,0"));
        assertThat(expected.getMessage(),
            hasToString(startsWith("Invalid symbol, could not find '.' at expected position after index 1, instead found index [-1]")));
        expected = expectThrows(IllegalArgumentException.class, () -> Def.Encoding.symbol("D.a,0"));
        assertThat(expected.getMessage(),
            hasToString(startsWith("Invalid symbol, could not find '.' at expected position after index 1, instead found index [1]")));
    }

    public void testMethodName() {
        assertEquals("a", Def.Encoding.methodName("St.a,0"));
        assertEquals("abc", Def.Encoding.methodName("Stthis.abc,0"));
        assertEquals("toString", Def.Encoding.methodName("Dfjava.lang.String.toString,5"));
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
            () -> Def.Encoding.methodName("Dfjava,lang.String.toString,5"));
        assertThat(expected.getMessage(),
            startsWith("Invalid symbol, could not find ',' at expected position after '.' at [18], instead found index [6]"));
    }

    public void testNumCaptures() {
        assertEquals(0, Def.Encoding.numCaptures("St.a,0"));
        assertEquals(1, Def.Encoding.numCaptures("Dfjava.util.Comparator.thenComparing,1"));
        assertEquals(12345, Def.Encoding.numCaptures("Stthis.abc,12345"));
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
            () -> Def.Encoding.numCaptures("Dfjava.util.Comparator.thenComparing,"));
        assertThat(expected.getMessage(),
            startsWith("Invalid symbol, could not find ',' at expected position, instead found index [36], encoding:"));

        expected = expectThrows(IllegalArgumentException.class,
            () -> Def.Encoding.numCaptures("Dfjava.util.Comparator.thenComparing,-1"));
        assertThat(expected.getMessage(),
            startsWith("numCaptures must be non-negative, not [-1]"));

        NumberFormatException number = expectThrows(NumberFormatException.class,
            () -> Def.Encoding.numCaptures("Dfjava.util.Comparator.thenComparing,a1"));
        assertThat(number.getMessage(), startsWith("For input string: \"a1\""));
    }

    public void testParse() {
        assertEquals(new Def.Encoding(true, false, "java.util.Comparator", "thenComparing", 1),
            Def.Encoding.parse("Sfjava.util.Comparator.thenComparing,1"));

        assertEquals(new Def.Encoding(false, false, "ft0", "augmentInjectMultiTimesX", 1),
            Def.Encoding.parse("Dfft0.augmentInjectMultiTimesX,1"));

        assertEquals(new Def.Encoding(false, false, "x", "concat", 1),
            Def.Encoding.parse("Dfx.concat,1"));

        assertEquals(new Def.Encoding(true, false, "java.lang.StringBuilder", "setLength", 1),
            Def.Encoding.parse("Sfjava.lang.StringBuilder.setLength,1"));

        assertEquals(new Def.Encoding(true, false, "org.elasticsearch.painless.FeatureTestObject", "overloadedStatic", 0),
            Def.Encoding.parse("Sforg.elasticsearch.painless.FeatureTestObject.overloadedStatic,0"));

        assertEquals(new Def.Encoding(true, false, "this", "lambda$synthetic$0", 1),
            Def.Encoding.parse("Sfthis.lambda$synthetic$0,1"));

        assertEquals(new Def.Encoding(true, true, "this", "lambda$synthetic$0", 2),
            Def.Encoding.parse("Stthis.lambda$synthetic$0,2"));

        assertEquals(new Def.Encoding(true, true, "this", "mycompare", 0),
            Def.Encoding.parse("Stthis.mycompare,0"));
    }

    public void testValidate() {
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
            () -> new Def.Encoding(false, false, "this", "myMethod", 0));

        assertThat(expected.getMessage(),
            startsWith("Def.Encoding must be static if symbol is 'this', encoding [Dfthis.myMethod,0]"));

        expected = expectThrows(IllegalArgumentException.class,
            () -> new Def.Encoding(true, true, "org.elasticsearch.painless.FeatureTestObject", "overloadedStatic", 0));

        assertThat(expected.getMessage(),
            startsWith("Def.Encoding symbol must be 'this', not [org.elasticsearch.painless.FeatureTestObject] if needsInstance"));

        expected = expectThrows(IllegalArgumentException.class,
            () -> new Def.Encoding(false, false, "x", "", 1));

        assertThat(expected.getMessage(),
            startsWith("methodName must be non-empty, encoding [Dfx.,1]"));
    }
}
