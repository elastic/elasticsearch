/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import junit.framework.AssertionFailedError;
import org.apache.lucene.util.Constants;
import org.elasticsearch.script.ScriptException;

import java.lang.invoke.WrongMethodTypeException;
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

    public void testDefNullPointer() {
        NullPointerException npe = expectScriptThrows(NullPointerException.class, () -> {
            exec("def x = null; x.intValue(); return null;");
        });
        assertEquals(npe.getMessage(), "cannot access method/field [intValue] from a null def reference");
        npe = expectScriptThrows(NullPointerException.class, () -> {
            exec("def x = [1, null]; for (y in x) y.intValue(); return null;");
        });
        assertEquals(npe.getMessage(), "cannot access method/field [intValue] from a null def reference");
        npe = expectScriptThrows(NullPointerException.class, () -> {
            exec("def x = [1, 2L, 3.0, 'test', (byte)1, (short)1, (char)1, null]; for (y in x) y.toString(); return null;");
        });
        assertEquals(npe.getMessage(), "cannot access method/field [toString] from a null def reference");
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
            exec("return 5;", null, Collections.singletonMap("bogusParameterKey", "bogusParameterValue"), true);
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
        assertTrue(parseException.getMessage().contains("cannot resolve type [PainlessError]"));
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

    public void testIllegalDynamicMethod() {
        IllegalArgumentException expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("def x = 'test'; return x.getClass().toString()");
        });
        assertTrue(expected.getMessage().contains("dynamic method [java.lang.String, getClass/0] not found"));
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
            exec("def i = 1} return 1", emptyMap(), emptyMap(), false);
        });
        assertEquals("unexpected token ['}'] was expecting one of [{<EOF>, ';'}].", e.getMessage());
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

    public void testCanNotOverrideRegexEnabled() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> exec("", null, singletonMap(CompilerSettings.REGEX_ENABLED.getKey(), "true"), false));
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

    public void testNotAStatement() {
        IllegalArgumentException iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("1 * 1; return null;"));
        assertEquals(iae.getMessage(), "not a statement: result not used from multiplication operation [*]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("boolean x = false; x && true; return null;"));
        assertEquals(iae.getMessage(), "not a statement: result not used from boolean and operation [&&]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("false; return null;"));
        assertEquals(iae.getMessage(), "not a statement: boolean constant [false] not used");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("boolean x = false; x == true; return null;"));
        assertEquals(iae.getMessage(), "not a statement: result not used from equals operation [==]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("boolean x = false; x ? 1 : 2; return null;"));
        assertEquals(iae.getMessage(), "not a statement: result not used from conditional operation [?:]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("1.1; return null;"));
        assertEquals(iae.getMessage(), "not a statement: decimal constant [1.1] not used");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("List x = null; x ?: [2]; return null;"));
        assertEquals(iae.getMessage(), "not a statement: result not used from elvis operation [?:]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("List x = null; (ArrayList)x; return null;"));
        assertEquals(iae.getMessage(), "not a statement: result not used from explicit cast with target type [ArrayList]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("List x = null; x instanceof List; return null;"));
        assertEquals(iae.getMessage(), "not a statement: result not used from instanceof with target type [List]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("[]; return null;"));
        assertEquals(iae.getMessage(), "not a statement: result not used from list initializer");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("[:]; return null;"));
        assertEquals(iae.getMessage(), "not a statement: result not used from map initializer");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("new int[] {1, 2}; return null;"));
        assertEquals(iae.getMessage(), "not a statement: result not used from new array");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("null; return null;"));
        assertEquals(iae.getMessage(), "not a statement: null constant not used");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("1; return null;"));
        assertEquals(iae.getMessage(), "not a statement: numeric constant [1] not used");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("/a/; return null;"));
        assertEquals(iae.getMessage(), "not a statement: regex constant [a] with flags [] not used");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("'1'; return null;"));
        assertEquals(iae.getMessage(), "not a statement: string constant [1] not used");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("+1; return null;"));
        assertEquals(iae.getMessage(), "not a statement: result not used from addition operation [+]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("boolean x; x; return null;"));
        assertEquals(iae.getMessage(), "not a statement: variable [x] not used");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("int[] x = new int[] {1}; x[0]; return null;"));
        assertEquals(iae.getMessage(), "not a statement: result of brace operator not used");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("Integer.MAX_VALUE; return null;"));
        assertEquals(iae.getMessage(), "not a statement: result of dot operator [.] not used");
    }

    public void testInvalidAssignment() {
        IllegalArgumentException iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("1 * 1 = 2; return null;"));
        assertEquals(iae.getMessage(), "invalid assignment: cannot assign a value to multiplication operation [*]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("boolean x = true; x && false = 2; return null;"));
        assertEquals(iae.getMessage(), "invalid assignment: cannot assign a value to boolean and operation [&&]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("false = 2; return null;"));
        assertEquals(iae.getMessage(), "invalid assignment: cannot assign a value to boolean constant [false]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("x() = 1; return null;"));
        assertEquals(iae.getMessage(), "invalid assignment: cannot assign a value to function call [x/0]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("1 == 1 = 2; return null;"));
        assertEquals(iae.getMessage(), "invalid assignment: cannot assign a value to equals operation [==]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("boolean x = true; (x ? 1 : 1) = 2; return null;"));
        assertEquals(iae.getMessage(), "invalid assignment: cannot assign a value to conditional operation [?:]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("1.1 = 1; return null;"));
        assertEquals(iae.getMessage(), "invalid assignment: cannot assign a value to decimal constant [1.1]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("List x = []; (x ?: []) = 2; return null;"));
        assertEquals(iae.getMessage(), "invalid assignment: cannot assign a value to elvis operation [?:]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("List x = []; x instanceof List = 5; return null;"));
        assertEquals(iae.getMessage(), "invalid assignment: cannot assign a value to instanceof with target type [List]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("[] = 5; return null;"));
        assertEquals(iae.getMessage(), "invalid assignment: cannot assign a value to list initializer");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("[:] = 5; return null;"));
        assertEquals(iae.getMessage(), "invalid assignment: cannot assign a value to map initializer");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("new int[] {} = 5; return null;"));
        assertEquals(iae.getMessage(), "invalid assignment: cannot assign a value to new array");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("new ArrayList() = 1; return null;"));
        assertEquals(iae.getMessage(), "invalid assignment cannot assign a value to new object with constructor [ArrayList/0]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("null = 1; return null;"));
        assertEquals(iae.getMessage(), "invalid assignment: cannot assign a value to null constant");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("1 = 1; return null;"));
        assertEquals(iae.getMessage(), "invalid assignment: cannot assign a value to numeric constant [1]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("/a/ = 1; return null;"));
        assertEquals(iae.getMessage(), "invalid assignment: cannot assign a value to regex constant [a] with flags []");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("'1' = 1; return null;"));
        assertEquals(iae.getMessage(), "invalid assignment: cannot assign a value to string constant [1]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("+1 = 2; return null;"));
        assertEquals(iae.getMessage(), "invalid assignment: cannot assign a value to addition operation [+]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("Double.x() = 1;"));
        assertEquals(iae.getMessage(), "invalid assignment: cannot assign a value to method call [x/0]");
    }

    public void testCannotResolveSymbol() {
        // assignment
        IllegalArgumentException iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("test0 = 1"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("int x; x = test0"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("test0 += 1"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // binary
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("test0 + 1"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("1 + test0"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // boolean comparison
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("test0 || true"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("true || test0"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // brace access
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("test0[0]"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("int[] x = new int[1]; x[test0]"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("def x = new int[1]; x[test0]"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("Map x = new HashMap(); x[test0]"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("List x = new ArrayList(); x[test0]"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // method call
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("List x = new ArrayList(); x.add(test0)"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("def x = new ArrayList(); x.add(test0)"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // function call
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("staticAddIntsTest(test0, 1)"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // numeric comparison
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("test0 > true"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("true > test0"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // conditional
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("test0 ? 2 : 1"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("boolean x = true; x ? test0 : 1"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("boolean x = true; x ? 2 : test0"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // dot access
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("test0[0]"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("int[] x = new int[1]; x[test0]"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // elvis
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("test0 ?: []"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("boolean x = true; x ?: test0"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // explicit cast
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("(int)test0"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // instanceof
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("test0 instanceof List"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // list initialization
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("[test0]"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // map initialization
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("[test0 : 1]"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("[1 : test0]"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // new array
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("new int[test0]"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // new object
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("new ArrayList(test0)"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // unary
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("!test0"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("-test0"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // declaration
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("int x = test0;"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // declaration
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("do {int x = 1;} while (test0);"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // foreach
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("for (def x : test0) {int x = 1;}"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // expression as statement
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("test0"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // for
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("for (int x = test0;;) {int x = 1;}"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("for (;test0;) {int x = 1;}"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("for (;;++test0) {int x = 1;}"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // if
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("if (test0) {int x = 1;}"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // if/else
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("if (test0) {int x = 1;} else {int x = 2;}"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // return
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("return test0;"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // throw
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("throw test0;"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");

        // while
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("while (test0) {int x = 1;}"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [test0]");
    }

    public void testPartialType() {
        int dots = randomIntBetween(1, 5);
        StringBuilder builder = new StringBuilder("test0");
        for (int dot = 0; dot < dots; ++dot) {
            builder.append(".test");
            builder.append(dot + 1);
        }
        String symbol = builder.toString();

        // assignment
        IllegalArgumentException iae = expectScriptThrows(IllegalArgumentException.class, () -> exec(symbol + " = 1"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("int x; x = " + symbol));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec(symbol + " += 1"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // binary
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec(symbol + " + 1"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("1 + " + symbol));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // boolean comparison
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec(symbol + " || true"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("true || " + symbol));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // brace access
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec(symbol + "[0]"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("int[] x = new int[1]; x[" + symbol + "]"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("def x = new int[1]; x[" + symbol + "]"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("Map x = new HashMap(); x[" + symbol + "]"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("List x = new ArrayList(); x[" + symbol + "]"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // method call
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("List x = new ArrayList(); x.add(" + symbol + ")"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("def x = new ArrayList(); x.add(" + symbol + ")"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // function call
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("staticAddIntsTest(" + symbol + ", 1)"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // numeric comparison
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec(symbol + " > true"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("true > " + symbol));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // conditional
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec(symbol + " ? 2 : 1"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("boolean x = true; x ? " + symbol + " : 1"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("boolean x = true; x ? 2 : " + symbol));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // dot access
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec(symbol + "[0]"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("int[] x = new int[1]; x[" + symbol + "]"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // elvis
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec(symbol + " ?: []"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("boolean x = true; x ?: " + symbol));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // explicit cast
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("(int)" + symbol));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // instanceof
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec(symbol + " instanceof List"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // list initialization
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("[" + symbol + "]"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // map initialization
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("[" + symbol + " : 1]"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("[1 : " + symbol + "]"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // new array
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("new int[" + symbol + "]"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // new object
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("new ArrayList(" + symbol + ")"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // unary
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("!" + symbol));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("-" + symbol));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // declaration
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("int x = " + symbol + ";"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // declaration
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("do {int x = 1;} while (" + symbol + ");"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // foreach
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("for (def x : " + symbol + ") {int x = 1;}"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // expression as statement
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec(symbol));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // for
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("for (int x = " + symbol + ";;) {int x = 1;}"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("for (;" + symbol + ";) {int x = 1;}"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("for (;;++" + symbol + ") {int x = 1;}"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // if
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("if (" + symbol + ") {int x = 1;}"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // if/else
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("if (" + symbol + ") {int x = 1;} else {int x = 2;}"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // return
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("return " + symbol + ";"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // throw
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("throw " + symbol + ";"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");

        // while
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("while (" + symbol + ") {int x = 1;}"));
        assertEquals(iae.getMessage(), "cannot resolve symbol [" + symbol + "]");
    }

    public void testInvalidFullyQualifiedStaticReferenceType() {
        // assignment
        IllegalArgumentException iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("java.util.List = 1"));
        assertEquals(iae.getMessage(), "invalid assignment: cannot write a value to a static type [java.util.List]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("java.util.List x; x = java.util.List"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("java.util.List += 1"));
        assertEquals(iae.getMessage(), "invalid assignment: cannot write a value to a static type [java.util.List]");

        // binary
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("java.util.List + 1"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("1 + java.util.List"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // boolean comparison
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("java.util.List || true"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("true || java.util.List"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // brace access
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("java.util.List[0]"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");
        iae = expectScriptThrows(IllegalArgumentException.class, () ->
                exec("java.util.List[] x = new java.util.List[1]; x[java.util.List]"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("def x = new java.util.List[1]; x[java.util.List]"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("Map x = new HashMap(); x[java.util.List]"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");
        iae = expectScriptThrows(IllegalArgumentException.class, () ->
                exec("java.util.List x = new java.util.ArrayList(); x[java.util.List]"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // method call
        iae = expectScriptThrows(IllegalArgumentException.class, () ->
                exec("java.util.List x = new java.util.ArrayList(); x.add(java.util.List)"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("def x = new java.util.ArrayList(); x.add(java.util.List)"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // function call
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("staticAddIntsTest(java.util.List, 1)"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // numeric comparison
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("java.util.List > true"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("true > java.util.List"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // conditional
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("java.util.List ? 2 : 1"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("boolean x = true; x ? java.util.List : 1"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("boolean x = true; x ? 2 : java.util.List"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // dot access
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("java.util.List[0]"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");
        iae = expectScriptThrows(IllegalArgumentException.class, () ->
                exec("java.util.List[] x = new java.util.List[1]; x[java.util.List]"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // elvis
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("java.util.List ?: []"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("boolean x = true; x ?: java.util.List"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // explicit cast
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("(java.util.List)java.util.List"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // instanceof
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("java.util.List instanceof java.util.List"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // list initialization
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("[java.util.List]"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // map initialization
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("[java.util.List : 1]"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("[1 : java.util.List]"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // new array
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("new java.util.List[java.util.List]"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // new object
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("new java.util.ArrayList(java.util.List)"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // unary
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("!java.util.List"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("-java.util.List"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // declaration
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("java.util.List x = java.util.List;"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // declaration
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("do {java.util.List x = [];} while (java.util.List);"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // foreach
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("for (def x : java.util.List) {java.util.List x = 1;}"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // expression as statement
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("java.util.List"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // for
        iae = expectScriptThrows(IllegalArgumentException.class, () ->
                exec("for (java.util.List x = java.util.List;;) {java.util.List x = 1;}"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("for (;java.util.List;) {java.util.List x = 1;}"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("for (;;++java.util.List) {java.util.List x = 1;}"));
        assertEquals(iae.getMessage(), "invalid assignment: cannot write a value to a static type [java.util.List]");

        // if
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("if (java.util.List) {java.util.List x = 1;}"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // if/else
        iae = expectScriptThrows(IllegalArgumentException.class, () ->
                exec("if (java.util.List) {java.util.List x = 1;} else {java.util.List x = 2;}"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // return
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("return java.util.List;"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // throw
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("throw java.util.List;"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");

        // while
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("while (java.util.List) {java.util.List x = 1;}"));
        assertEquals(iae.getMessage(), "value required: instead found unexpected type [java.util.List]");
    }

    public void testInvalidNullSafeBehavior() {
        expectScriptThrows(ClassCastException.class, () ->
                exec("def test = ['hostname': 'somehostname']; test?.hostname && params.host.hostname != ''"));
        expectScriptThrows(NullPointerException.class, () -> exec("params?.host?.hostname && params.host?.hostname != ''"));
    }

    public void testInstanceMethodNotFound() {
        IllegalArgumentException iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("doesNotExist()"));
        assertEquals(iae.getMessage(), "Unknown call [doesNotExist] with [0] arguments.");
        iae = expectScriptThrows(IllegalArgumentException.class, () -> exec("doesNotExist(1, 'string', false)"));
        assertEquals(iae.getMessage(), "Unknown call [doesNotExist] with [3] arguments.");
    }
}
