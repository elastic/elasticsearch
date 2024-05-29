/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

/** Tests method overloading */
public class OverloadTests extends ScriptTestCase {

    public void testMethod() {
        // assertEquals(2, exec("return 'abc123abc'.indexOf('c');"));
        // assertEquals(8, exec("return 'abc123abc'.indexOf('c', 3);"));
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("return 'abc123abc'.indexOf('c', 3, 'bogus');");
        });
        assertTrue(expected.getMessage().contains("[java.lang.String, indexOf/3]"));
    }

    public void testMethodDynamic() {
        assertEquals(2, exec("def x = 'abc123abc'; return x.indexOf('c');"));
        assertEquals(8, exec("def x = 'abc123abc'; return x.indexOf('c', 3);"));
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("def x = 'abc123abc'; return x.indexOf('c', 3, 'bogus');");
        });
        assertTrue(expected.getMessage().contains("dynamic method [java.lang.String, indexOf/3] not found"));
    }

    public void testConstructor() {
        assertEquals(
            true,
            exec(
                "org.elasticsearch.painless.FeatureTestObject f = new org.elasticsearch.painless.FeatureTestObject();"
                    + "return f.x == 0 && f.y == 0;"
            )
        );
        assertEquals(
            true,
            exec(
                "org.elasticsearch.painless.FeatureTestObject f = new org.elasticsearch.painless.FeatureTestObject(1, 2);"
                    + "return f.x == 1 && f.y == 2;"
            )
        );
    }

    public void testStatic() {
        assertEquals(true, exec("return org.elasticsearch.painless.FeatureTestObject.overloadedStatic();"));
        assertEquals(false, exec("return org.elasticsearch.painless.FeatureTestObject.overloadedStatic(false);"));
    }
}
