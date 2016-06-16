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

import java.lang.invoke.WrongMethodTypeException;
import java.util.Arrays;
import java.util.Collections;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;

public class WhenThingsGoWrongTests extends ScriptTestCase {
    public void testNullPointer() {
        expectScriptThrows(NullPointerException.class, () -> {
            exec("int x = params['missing']; return x;");
        });
    }

    /** test "line numbers" in the bytecode, which are really 1-based offsets */
    public void testLineNumbers() {
        // trigger NPE at line 1 of the script
        NullPointerException exception = expectScriptThrows(NullPointerException.class, () -> {
            exec("String x = null; boolean y = x.isEmpty();\n" +
                 "return y;");
        });
        // null deref at x.isEmpty(), the '.' is offset 30 (+1)
        assertEquals(30 + 1, exception.getStackTrace()[0].getLineNumber());

        // trigger NPE at line 2 of the script
        exception = expectScriptThrows(NullPointerException.class, () -> {
            exec("String x = null;\n" +
                 "return x.isEmpty();");
        });
        // null deref at x.isEmpty(), the '.' is offset 25 (+1)
        assertEquals(25 + 1, exception.getStackTrace()[0].getLineNumber());

        // trigger NPE at line 3 of the script
        exception = expectScriptThrows(NullPointerException.class, () -> {
            exec("String x = null;\n" +
                 "String y = x;\n" +
                 "return y.isEmpty();");
        });
        // null deref at y.isEmpty(), the '.' is offset 39 (+1)
        assertEquals(39 + 1, exception.getStackTrace()[0].getLineNumber());

        // trigger NPE at line 4 in script (inside conditional)
        exception = expectScriptThrows(NullPointerException.class, () -> {
            exec("String x = null;\n" +
                 "boolean y = false;\n" +
                 "if (!y) {\n" +
                 "  y = x.isEmpty();\n" +
                 "}\n" +
                 "return y;");
        });
        // null deref at x.isEmpty(), the '.' is offset 53 (+1)
        assertEquals(53 + 1, exception.getStackTrace()[0].getLineNumber());
    }

    public void testInvalidShift() {
        expectScriptThrows(ClassCastException.class, () -> {
            exec("float x = 15F; x <<= 2; return x;");
        });

        expectScriptThrows(ClassCastException.class, () -> {
            exec("double x = 15F; x <<= 2; return x;");
        });
    }

    public void testBogusParameter() {
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
            exec("return 5;", null, Collections.singletonMap("bogusParameterKey", "bogusParameterValue"), null);
        });
        assertTrue(expected.getMessage().contains("Unrecognized compile-time parameter"));
    }

    public void testInfiniteLoops() {
        PainlessError expected = expectScriptThrows(PainlessError.class, () -> {
            exec("boolean x = true; while (x) {}");
        });
        assertTrue(expected.getMessage().contains(
                   "The maximum number of statements that can be executed in a loop has been reached."));

        expected = expectScriptThrows(PainlessError.class, () -> {
            exec("while (true) {int y = 5;}");
        });
        assertTrue(expected.getMessage().contains(
                   "The maximum number of statements that can be executed in a loop has been reached."));

        expected = expectScriptThrows(PainlessError.class, () -> {
            exec("while (true) { boolean x = true; while (x) {} }");
        });
        assertTrue(expected.getMessage().contains(
                   "The maximum number of statements that can be executed in a loop has been reached."));

        expected = expectScriptThrows(PainlessError.class, () -> {
            exec("while (true) { boolean x = false; while (x) {} }");
            fail("should have hit PainlessError");
        });
        assertTrue(expected.getMessage().contains(
                   "The maximum number of statements that can be executed in a loop has been reached."));

        expected = expectScriptThrows(PainlessError.class, () -> {
            exec("boolean x = true; for (;x;) {}");
            fail("should have hit PainlessError");
        });
        assertTrue(expected.getMessage().contains(
                   "The maximum number of statements that can be executed in a loop has been reached."));

        expected = expectScriptThrows(PainlessError.class, () -> {
            exec("for (;;) {int x = 5;}");
            fail("should have hit PainlessError");
        });
        assertTrue(expected.getMessage().contains(
                   "The maximum number of statements that can be executed in a loop has been reached."));

        expected = expectScriptThrows(PainlessError.class, () -> {
            exec("def x = true; do {int y = 5;} while (x)");
            fail("should have hit PainlessError");
        });
        assertTrue(expected.getMessage().contains(
                   "The maximum number of statements that can be executed in a loop has been reached."));

        RuntimeException parseException = expectScriptThrows(RuntimeException.class, () -> {
            exec("try { int x; } catch (PainlessError error) {}");
            fail("should have hit ParseException");
        });
        assertTrue(parseException.getMessage().contains("unexpected token ['PainlessError']"));
    }

    public void testLoopLimits() {
        // right below limit: ok
        exec("for (int x = 0; x < 9999; ++x) {}");

        PainlessError expected = expectScriptThrows(PainlessError.class, () -> {
            exec("for (int x = 0; x < 10000; ++x) {}");
        });
        assertTrue(expected.getMessage().contains(
                   "The maximum number of statements that can be executed in a loop has been reached."));
    }

    public void testSourceLimits() {
        final char[] tooManyChars = new char[Compiler.MAXIMUM_SOURCE_LENGTH + 1];
        Arrays.fill(tooManyChars, '0');

        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec(new String(tooManyChars));
        });
        assertTrue(expected.getMessage().contains("Scripts may be no longer than"));

        final char[] exactlyAtLimit = new char[Compiler.MAXIMUM_SOURCE_LENGTH];
        Arrays.fill(exactlyAtLimit, '0');
        // ok
        assertEquals(0, exec(new String(exactlyAtLimit)));
    }

    public void testIllegalDynamicMethod() {
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("def x = 'test'; return x.getClass().toString()");
        });
        assertTrue(expected.getMessage().contains("Unable to find dynamic method"));
    }

    public void testDynamicNPE() {
        expectScriptThrows(NullPointerException.class, () -> {
            exec("def x = null; return x.toString()");
        });
    }

    public void testDynamicWrongArgs() {
        expectScriptThrows(WrongMethodTypeException.class, () -> {
            exec("def x = new ArrayList(); return x.get('bogus');");
        });
    }

    public void testDynamicArrayWrongIndex() {
        expectScriptThrows(WrongMethodTypeException.class, () -> {
            exec("def x = new long[1]; x[0]=1; return x['bogus'];");
        });
    }

    public void testDynamicListWrongIndex() {
        expectScriptThrows(WrongMethodTypeException.class, () -> {
            exec("def x = new ArrayList(); x.add('foo'); return x['bogus'];");
        });
    }

    /**
     * Makes sure that we present a useful error message with a misplaced right-curly. This is important because we do some funky things in
     * the parser with right-curly brackets to allow statements to be delimited by them at the end of blocks.
     */
    public void testRCurlyNotDelim() {
        IllegalArgumentException e = expectScriptThrows(IllegalArgumentException.class, () -> {
            // We don't want PICKY here so we get the normal error message
            exec("def i = 1} return 1", emptyMap(), emptyMap(), null);
        });
        assertEquals("invalid sequence of tokens near ['}'].", e.getMessage());
    }

    public void testBadBoxingCast() {
        expectScriptThrows(ClassCastException.class, () -> {
            exec("BitSet bs = new BitSet(); bs.and(2);");
        });
    }
}
