/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.startsWith;

public class DefEncodingTests extends ESTestCase {

    public void testParse() {
        assertEquals(
            new Def.Encoding(true, false, "java.util.Comparator", "thenComparing", 1),
            new Def.Encoding("Sfjava.util.Comparator.thenComparing,1")
        );

        assertEquals(
            new Def.Encoding(false, false, "ft0", "augmentInjectMultiTimesX", 1),
            new Def.Encoding("Dfft0.augmentInjectMultiTimesX,1")
        );

        assertEquals(new Def.Encoding(false, false, "x", "concat", 1), new Def.Encoding("Dfx.concat,1"));

        assertEquals(
            new Def.Encoding(true, false, "java.lang.StringBuilder", "setLength", 1),
            new Def.Encoding("Sfjava.lang.StringBuilder.setLength,1")
        );

        assertEquals(
            new Def.Encoding(true, false, "org.elasticsearch.painless.FeatureTestObject", "overloadedStatic", 0),
            new Def.Encoding("Sforg.elasticsearch.painless.FeatureTestObject.overloadedStatic,0")
        );

        assertEquals(new Def.Encoding(true, false, "this", "lambda$synthetic$0", 1), new Def.Encoding("Sfthis.lambda$synthetic$0,1"));

        assertEquals(new Def.Encoding(true, true, "this", "lambda$synthetic$0", 2), new Def.Encoding("Stthis.lambda$synthetic$0,2"));

        assertEquals(new Def.Encoding(true, true, "this", "mycompare", 0), new Def.Encoding("Stthis.mycompare,0"));
    }

    public void testValidate() {
        IllegalArgumentException expected = expectThrows(
            IllegalArgumentException.class,
            () -> new Def.Encoding(false, false, "this", "myMethod", 0)
        );

        assertThat(expected.getMessage(), startsWith("Def.Encoding must be static if symbol is 'this', encoding [Dfthis.myMethod,0]"));

        expected = expectThrows(
            IllegalArgumentException.class,
            () -> new Def.Encoding(true, true, "org.elasticsearch.painless.FeatureTestObject", "overloadedStatic", 0)
        );

        assertThat(
            expected.getMessage(),
            startsWith("Def.Encoding symbol must be 'this', not [org.elasticsearch.painless.FeatureTestObject] if needsInstance")
        );

        expected = expectThrows(IllegalArgumentException.class, () -> new Def.Encoding(false, false, "x", "", 1));

        assertThat(expected.getMessage(), startsWith("methodName must be non-empty, encoding [Dfx.,1]"));
    }
}
