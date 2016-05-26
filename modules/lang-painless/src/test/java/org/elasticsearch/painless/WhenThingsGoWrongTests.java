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

public class WhenThingsGoWrongTests extends ScriptTestCase {
    public void testNullPointer() {
        expectThrows(NullPointerException.class, () -> {
            exec("int x = params['missing']; return x;");
        });
    }

    public void testLineNumbers() {
        // trigger NPE at line 1 of the script
        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            exec("String x = null; boolean y = x.isEmpty();\n" +
                 "return y;");
        });
        assertEquals(1, exception.getStackTrace()[0].getLineNumber());

        // trigger NPE at line 2 of the script
        exception = expectThrows(NullPointerException.class, () -> {
            exec("String x = null;\n" +
                 "return x.isEmpty();");
        });
        assertEquals(2, exception.getStackTrace()[0].getLineNumber());

        // trigger NPE at line 3 of the script
        exception = expectThrows(NullPointerException.class, () -> {
            exec("String x = null;\n" +
                 "String y = x;\n" +
                 "return y.isEmpty();");
        });
        assertEquals(3, exception.getStackTrace()[0].getLineNumber());

        // trigger NPE at line 4 in script (inside conditional)
        exception = expectThrows(NullPointerException.class, () -> {
            exec("String x = null;\n" +
                 "boolean y = false;\n" +
                 "if (!y) {\n" +
                 "  y = x.isEmpty();\n" +
                 "}\n" +
                 "return y;");
        });
        assertEquals(4, exception.getStackTrace()[0].getLineNumber());
    }

    public void testInvalidShift() {
        expectThrows(ClassCastException.class, () -> {
            exec("float x = 15F; x <<= 2; return x;");
        });

        expectThrows(ClassCastException.class, () -> {
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
        PainlessError expected = expectThrows(PainlessError.class, () -> {
            exec("boolean x = true; while (x) {}");
        });
        assertTrue(expected.getMessage().contains(
                   "The maximum number of statements that can be executed in a loop has been reached."));

        expected = expectThrows(PainlessError.class, () -> {
            exec("while (true) {int y = 5;}");
        });
        assertTrue(expected.getMessage().contains(
                   "The maximum number of statements that can be executed in a loop has been reached."));

        expected = expectThrows(PainlessError.class, () -> {
            exec("while (true) { boolean x = true; while (x) {} }");
        });
        assertTrue(expected.getMessage().contains(
                   "The maximum number of statements that can be executed in a loop has been reached."));

        expected = expectThrows(PainlessError.class, () -> {
            exec("while (true) { boolean x = false; while (x) {} }");
            fail("should have hit PainlessError");
        });
        assertTrue(expected.getMessage().contains(
                   "The maximum number of statements that can be executed in a loop has been reached."));

        expected = expectThrows(PainlessError.class, () -> {
            exec("boolean x = true; for (;x;) {}");
            fail("should have hit PainlessError");
        });
        assertTrue(expected.getMessage().contains(
                   "The maximum number of statements that can be executed in a loop has been reached."));

        expected = expectThrows(PainlessError.class, () -> {
            exec("for (;;) {int x = 5;}");
            fail("should have hit PainlessError");
        });
        assertTrue(expected.getMessage().contains(
                   "The maximum number of statements that can be executed in a loop has been reached."));

        expected = expectThrows(PainlessError.class, () -> {
            exec("def x = true; do {int y = 5;} while (x)");
            fail("should have hit PainlessError");
        });
        assertTrue(expected.getMessage().contains(
                   "The maximum number of statements that can be executed in a loop has been reached."));

        RuntimeException parseException = expectThrows(RuntimeException.class, () -> {
            exec("try { int x; } catch (PainlessError error) {}");
            fail("should have hit ParseException");
        });
        assertTrue(parseException.getMessage().contains("unexpected token ['PainlessError']"));
    }

    public void testLoopLimits() {
        // right below limit: ok
        exec("for (int x = 0; x < 9999; ++x) {}");

        PainlessError expected = expectThrows(PainlessError.class, () -> {
            exec("for (int x = 0; x < 10000; ++x) {}");
        });
        assertTrue(expected.getMessage().contains(
                   "The maximum number of statements that can be executed in a loop has been reached."));
    }

    public void testSourceLimits() {
        final char[] tooManyChars = new char[Compiler.MAXIMUM_SOURCE_LENGTH + 1];
        Arrays.fill(tooManyChars, '0');

        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
            exec(new String(tooManyChars));
        });
        assertTrue(expected.getMessage().contains("Scripts may be no longer than"));

        final char[] exactlyAtLimit = new char[Compiler.MAXIMUM_SOURCE_LENGTH];
        Arrays.fill(exactlyAtLimit, '0');
        // ok
        assertEquals(0, exec(new String(exactlyAtLimit)));
    }

    public void testIllegalDynamicMethod() {
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
            exec("def x = 'test'; return x.getClass().toString()");
        });
        assertTrue(expected.getMessage().contains("Unable to find dynamic method"));
    }

    public void testDynamicNPE() {
        expectThrows(NullPointerException.class, () -> {
            exec("def x = null; return x.toString()");
        });
    }

    public void testDynamicWrongArgs() {
        expectThrows(WrongMethodTypeException.class, () -> {
            exec("def x = new ArrayList(); return x.get('bogus');");
        });
    }

    public void testDynamicArrayWrongIndex() {
        expectThrows(WrongMethodTypeException.class, () -> {
            exec("def x = new long[1]; x[0]=1; return x['bogus'];");
        });
    }

    public void testDynamicListWrongIndex() {
        expectThrows(WrongMethodTypeException.class, () -> {
            exec("def x = new ArrayList(); x.add('foo'); return x['bogus'];");
        });
    }
}
