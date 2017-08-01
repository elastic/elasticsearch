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

import junit.framework.AssertionFailedError;

import org.apache.lucene.util.Constants;
import org.elasticsearch.script.ScriptException;

import java.lang.invoke.WrongMethodTypeException;
import java.util.Arrays;
import java.util.Collections;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.instanceOf;

public class WhenThingsGoWrongTests extends ScriptTestCase {
    public void testNullPointer() {
        expectScriptThrows(NullPointerException.class, () -> {
            exec("int x = params['missing']; return x;");
        });
        expectScriptThrows(NullPointerException.class, () -> {
            exec("Double.parseDouble(params['missing'])");
        });
    }

    /**
     * Test that the scriptStack looks good. By implication this tests that we build proper "line numbers" in stack trace. These line
     * numbers are really 1 based character numbers.
     */
    public void testScriptStack() {
        for (String type : new String[] {"String", "def   "}) {
            // trigger NPE at line 1 of the script
            ScriptException exception = expectThrows(ScriptException.class, () -> {
                exec(type + " x = null; boolean y = x.isEmpty();\n" +
                     "return y;");
            });
            // null deref at x.isEmpty(), the '.' is offset 30
            assertScriptElementColumn(30, exception);
            assertScriptStack(exception,
                    "y = x.isEmpty();\n",
                    "     ^---- HERE");
            assertThat(exception.getCause(), instanceOf(NullPointerException.class));

            // trigger NPE at line 2 of the script
            exception = expectThrows(ScriptException.class, () -> {
                exec(type + " x = null;\n" +
                     "return x.isEmpty();");
            });
            // null deref at x.isEmpty(), the '.' is offset 25
            assertScriptElementColumn(25, exception);
            assertScriptStack(exception,
                    "return x.isEmpty();",
                    "        ^---- HERE");
            assertThat(exception.getCause(), instanceOf(NullPointerException.class));

            // trigger NPE at line 3 of the script
            exception = expectThrows(ScriptException.class, () -> {
                exec(type + " x = null;\n" +
                     type + " y = x;\n" +
                     "return y.isEmpty();");
            });
            // null deref at y.isEmpty(), the '.' is offset 39
            assertScriptElementColumn(39, exception);
            assertScriptStack(exception,
                    "return y.isEmpty();",
                    "        ^---- HERE");
            assertThat(exception.getCause(), instanceOf(NullPointerException.class));

            // trigger NPE at line 4 in script (inside conditional)
            exception = expectThrows(ScriptException.class, () -> {
                exec(type + " x = null;\n" +
                     "boolean y = false;\n" +
                     "if (!y) {\n" +
                     "  y = x.isEmpty();\n" +
                     "}\n" +
                     "return y;");
            });
            // null deref at x.isEmpty(), the '.' is offset 53
            assertScriptElementColumn(53, exception);
            assertScriptStack(exception,
                    "y = x.isEmpty();\n}\n",
                    "     ^---- HERE");
            assertThat(exception.getCause(), instanceOf(NullPointerException.class));
        }
    }

    private void assertScriptElementColumn(int expectedColumn, ScriptException exception) {
        StackTraceElement[] stackTrace = exception.getCause().getStackTrace();
        for (int i = 0; i < stackTrace.length; i++) {
            if (WriterConstants.CLASS_NAME.equals(stackTrace[i].getClassName())) {
                if (expectedColumn + 1 != stackTrace[i].getLineNumber()) {
                    AssertionFailedError assertion = new AssertionFailedError("Expected column to be [" + expectedColumn + "] but was ["
                            + stackTrace[i].getLineNumber() + "]");
                    assertion.initCause(exception);
                    throw assertion;
                }
                return;
            }
        }
        fail("didn't find script stack element");
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
            exec("return 5;", null, Collections.singletonMap("bogusParameterKey", "bogusParameterValue"), null, true);
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
            exec("try { int x; } catch (PainlessError error) {}", false);
            fail("should have hit ParseException");
        });
        assertTrue(parseException.getMessage().contains("unexpected token ['PainlessError']"));
    }

    public void testLoopLimits() {
        // right below limit: ok
        exec("for (int x = 0; x < 999999; ++x) {}");

        PainlessError expected = expectScriptThrows(PainlessError.class, () -> {
            exec("for (int x = 0; x < 1000000; ++x) {}");
        });
        assertTrue(expected.getMessage().contains(
                   "The maximum number of statements that can be executed in a loop has been reached."));
    }

    public void testSourceLimits() {
        final char[] tooManyChars = new char[Compiler.MAXIMUM_SOURCE_LENGTH + 1];
        Arrays.fill(tooManyChars, '0');

        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, false, () -> {
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
            exec("def i = 1} return 1", emptyMap(), emptyMap(), null, false);
        });
        assertEquals("unexpected token ['}'] was expecting one of [<EOF>].", e.getMessage());
    }

    public void testBadBoxingCast() {
        expectScriptThrows(ClassCastException.class, () -> {
            exec("BitSet bs = new BitSet(); bs.and(2);");
        });
    }

    public void testOutOfMemoryError() {
        assumeTrue("test only happens to work for sure on oracle jre", Constants.JAVA_VENDOR.startsWith("Oracle"));
        expectScriptThrows(OutOfMemoryError.class, () -> {
            exec("int[] x = new int[Integer.MAX_VALUE - 1];");
        });
    }

    public void testStackOverflowError() {
        expectScriptThrows(StackOverflowError.class, () -> {
            exec("void recurse(int x, int y) {recurse(x, y)} recurse(1, 2);");
        });
    }

    public void testRegexDisabledByDefault() {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> exec("return 'foo' ==~ /foo/"));
        assertEquals("Regexes are disabled. Set [script.painless.regex.enabled] to [true] in elasticsearch.yaml to allow them. "
                + "Be careful though, regexes break out of Painless's protection against deep recursion and long loops.", e.getMessage());
    }

    public void testCanNotOverrideRegexEnabled() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> exec("", null, singletonMap(CompilerSettings.REGEX_ENABLED.getKey(), "true"), null, false));
        assertEquals("[painless.regex.enabled] can only be set on node startup.", e.getMessage());
    }

    public void testInvalidIntConstantSuggestsLong() {
        IllegalArgumentException e = expectScriptThrows(IllegalArgumentException.class, () -> exec("return 864000000000"));
        assertEquals("Invalid int constant [864000000000]. If you want a long constant then change it to [864000000000L].", e.getMessage());
        assertEquals(864000000000L, exec("return 864000000000L"));
        e = expectScriptThrows(IllegalArgumentException.class, () -> exec("return -864000000000"));
        assertEquals("Invalid int constant [-864000000000]. If you want a long constant then change it to [-864000000000L].",
                e.getMessage());
        assertEquals(-864000000000L, exec("return -864000000000L"));

        // If it isn't a valid long we don't give any suggestions
        e = expectScriptThrows(IllegalArgumentException.class, () -> exec("return 92233720368547758070"));
        assertEquals("Invalid int constant [92233720368547758070].", e.getMessage());
        e = expectScriptThrows(IllegalArgumentException.class, () -> exec("return -92233720368547758070"));
        assertEquals("Invalid int constant [-92233720368547758070].", e.getMessage());
    }

    public void testQuestionSpaceDotIsNotNullSafeDereference() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, () -> exec("return params.a? .b", false));
        assertEquals("invalid sequence of tokens near ['.'].", e.getMessage());
    }

    public void testBadStringEscape() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, () -> exec("'\\a'", false));
        assertEquals("unexpected character ['\\a]. The only valid escape sequences in strings starting with ['] are [\\\\] and [\\'].",
                e.getMessage());
        e = expectScriptThrows(IllegalArgumentException.class, () -> exec("\"\\a\"", false));
        assertEquals("unexpected character [\"\\a]. The only valid escape sequences in strings starting with [\"] are [\\\\] and [\\\"].",
                e.getMessage());
    }

    public void testRegularUnexpectedCharacter() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, () -> exec("'", false));
        assertEquals("unexpected character ['].", e.getMessage());
        e = expectScriptThrows(IllegalArgumentException.class, () -> exec("'cat", false));
        assertEquals("unexpected character ['cat].", e.getMessage());
    }
}
