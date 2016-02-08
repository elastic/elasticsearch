/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless;

import java.util.Arrays;
import java.util.Collections;

public class WhenThingsGoWrongTests extends ScriptTestCase {
    public void testNullPointer() {
        try {
            exec("int x = (int) ((Map) input).get(\"missing\"); return x;");
            fail("should have hit npe");
        } catch (NullPointerException expected) {}
    }

    public void testInvalidShift() {
        try {
            exec("float x = 15F; x <<= 2; return x;");
            fail("should have hit cce");
        } catch (ClassCastException expected) {}

        try {
            exec("double x = 15F; x <<= 2; return x;");
            fail("should have hit cce");
        } catch (ClassCastException expected) {}
    }

    public void testBogusParameter() {
        try {
            exec("return 5;", null, Collections.singletonMap("bogusParameterKey", "bogusParameterValue"));
            fail("should have hit IAE");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("Unrecognized compile-time parameter"));
        }
    }

    public void testInfiniteLoops() {
        try {
            exec("boolean x = true; while (x) {}");
            fail("should have hit PainlessError");
        } catch (PainlessError expected) {
            assertTrue(expected.getMessage().contains(
                "The maximum number of statements that can be executed in a loop has been reached."));
        }

        try {
            exec("while (true) {int y = 5}");
            fail("should have hit PainlessError");
        } catch (PainlessError expected) {
            assertTrue(expected.getMessage().contains(
                "The maximum number of statements that can be executed in a loop has been reached."));
        }

        try {
            exec("while (true) { boolean x = true; while (x) {} }");
            fail("should have hit PainlessError");
        } catch (PainlessError expected) {
            assertTrue(expected.getMessage().contains(
                "The maximum number of statements that can be executed in a loop has been reached."));
        }

        try {
            exec("while (true) { boolean x = false; while (x) {} }");
            fail("should have hit PainlessError");
        } catch (PainlessError expected) {
            assertTrue(expected.getMessage().contains(
                "The maximum number of statements that can be executed in a loop has been reached."));
        }

        try {
            exec("boolean x = true; for (;x;) {}");
            fail("should have hit PainlessError");
        } catch (PainlessError expected) {
            assertTrue(expected.getMessage().contains(
                "The maximum number of statements that can be executed in a loop has been reached."));
        }

        try {
            exec("for (;;) {int x = 5}");
            fail("should have hit PainlessError");
        } catch (PainlessError expected) {
            assertTrue(expected.getMessage().contains(
                "The maximum number of statements that can be executed in a loop has been reached."));
        }

        try {
            exec("def x = true; do {int y = 5;} while (x)");
            fail("should have hit PainlessError");
        } catch (PainlessError expected) {
            assertTrue(expected.getMessage().contains(
                "The maximum number of statements that can be executed in a loop has been reached."));
        }

        try {
            exec("try { int x } catch (PainlessError error) {}");
            fail("should have hit ParseException");
        } catch (RuntimeException expected) {
            assertTrue(expected.getMessage().contains(
                "unexpected token ['PainlessError'] was expecting one of [TYPE]."));
        }

    }

    public void testLoopLimits() {
        exec("for (int x = 0; x < 9999; ++x) {}");

        try {
            exec("for (int x = 0; x < 10000; ++x) {}");
            fail("should have hit PainlessError");
        } catch (PainlessError expected) {
            assertTrue(expected.getMessage().contains(
                "The maximum number of statements that can be executed in a loop has been reached."));
        }
    }

    public void testSourceLimits() {
        char[] chars = new char[Compiler.MAXIMUM_SOURCE_LENGTH + 1];
        Arrays.fill(chars, '0');

        try {
            exec(new String(chars));
            fail("should have hit IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("Scripts may be no longer than"));
        }

        chars = new char[Compiler.MAXIMUM_SOURCE_LENGTH];
        Arrays.fill(chars, '0');

        assertEquals(0, exec(new String(chars)));
    }
}
