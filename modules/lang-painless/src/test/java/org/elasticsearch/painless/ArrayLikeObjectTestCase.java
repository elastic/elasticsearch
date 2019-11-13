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

import org.elasticsearch.common.Nullable;
import org.hamcrest.Matcher;

import static java.util.Collections.singletonMap;

/**
 * Superclass for testing array-like objects (arrays and lists).
 */
public abstract class ArrayLikeObjectTestCase extends ScriptTestCase {
    /**
     * Build the string for declaring the variable holding the array-like-object to test. So {@code int[]} for arrays and {@code List} for
     * lists.
     */
    protected abstract String declType(String valueType);
    /**
     * Build the string for calling the constructor for the array-like-object to test. So {@code new int[5]} for arrays and
     * {@code [0, 0, 0, 0, 0]} or {@code [null, null, null, null, null]} for lists.
     */
    protected abstract String valueCtorCall(String valueType, int size);
    /**
     * Matcher for the message of the out of bounds exceptions thrown for too negative or too positive offsets.
     */
    protected abstract Matcher<String> outOfBoundsExceptionMessageMatcher(int index, int size);

    private void arrayLoadStoreTestCase(boolean declareAsDef, String valueType, Object val, @Nullable Number valPlusOne) {
        String declType = declareAsDef ? "def" : declType(valueType);
        String valueCtorCall = valueCtorCall(valueType, 5);
        String decl = declType + " x = " + valueCtorCall;
        assertEquals(5, exec(decl + "; return x.length", true));
        assertEquals(val, exec(decl + "; x[ 0] = params.val; return x[ 0];", singletonMap("val", val), true));
        assertEquals(val, exec(decl + "; x[ 0] = params.val; return x[-5];", singletonMap("val", val), true));
        assertEquals(val, exec(decl + "; x[-5] = params.val; return x[-5];", singletonMap("val", val), true));

        expectOutOfBounds( 6, decl + "; return x[ 6]", val);
        expectOutOfBounds(-1, decl + "; return x[-6]", val);
        expectOutOfBounds( 6, decl + "; x[ 6] = params.val; return 0", val);
        expectOutOfBounds(-1, decl + "; x[-6] = params.val; return 0", val);

        if (valPlusOne != null) {
            assertEquals(val,        exec(decl + "; x[0] = params.val; x[ 0] = x[ 0]++; return x[0];", singletonMap("val", val), true));
            assertEquals(val,        exec(decl + "; x[0] = params.val; x[ 0] = x[-5]++; return x[0];", singletonMap("val", val), true));
            assertEquals(valPlusOne, exec(decl + "; x[0] = params.val; x[ 0] = ++x[ 0]; return x[0];", singletonMap("val", val), true));
            assertEquals(valPlusOne, exec(decl + "; x[0] = params.val; x[ 0] = ++x[-5]; return x[0];", singletonMap("val", val), true));
            assertEquals(valPlusOne, exec(decl + "; x[0] = params.val; x[ 0]++        ; return x[0];", singletonMap("val", val), true));
            assertEquals(valPlusOne, exec(decl + "; x[0] = params.val; x[-5]++        ; return x[0];", singletonMap("val", val), true));
            assertEquals(valPlusOne, exec(decl + "; x[0] = params.val; x[ 0] += 1     ; return x[0];", singletonMap("val", val), true));
            assertEquals(valPlusOne, exec(decl + "; x[0] = params.val; x[-5] += 1     ; return x[0];", singletonMap("val", val), true));

            expectOutOfBounds( 6, decl + "; return x[ 6]++", val);
            expectOutOfBounds(-1, decl + "; return x[-6]++", val);
            expectOutOfBounds( 6, decl + "; return ++x[ 6]", val);
            expectOutOfBounds(-1, decl + "; return ++x[-6]", val);
            expectOutOfBounds( 6, decl + "; x[ 6] += 1; return 0", val);
            expectOutOfBounds(-1, decl + "; x[-6] += 1; return 0", val);
        }
    }

    private void expectOutOfBounds(int index, String script, Object val) {
        IndexOutOfBoundsException e = expectScriptThrows(IndexOutOfBoundsException.class, () ->
            exec(script, singletonMap("val", val), true));
        try {
            /* If this fails you *might* be missing -XX:-OmitStackTraceInFastThrow in the test jvm
             * In Eclipse you can add this by default by going to Preference->Java->Installed JREs,
             * clicking on the default JRE, clicking edit, and adding the flag to the
             * "Default VM Arguments".
             */
            assertThat(e.getMessage(), outOfBoundsExceptionMessageMatcher(index, 5));
        } catch (AssertionError ae) {
            // Mark the exception we are testing as suppressed so we get its stack trace.
            ae.addSuppressed(e);
            throw ae;
        }
    }

    public void testInts() {         arrayLoadStoreTestCase(false, "int",    5,         6); }
    public void testIntsInDef() {    arrayLoadStoreTestCase(true,  "int",    5,         6); }
    public void testLongs() {        arrayLoadStoreTestCase(false, "long",   5L,        6L); }
    public void testLongsInDef() {   arrayLoadStoreTestCase(true,  "long",   5L,        6L); }
    public void testShorts() {       arrayLoadStoreTestCase(false, "short",  (short) 5, (short) 6); }
    public void testShortsInDef() {  arrayLoadStoreTestCase(true,  "short",  (short) 5, (short) 6); }
    public void testBytes() {        arrayLoadStoreTestCase(false, "byte",   (byte) 5,  (byte) 6); }
    public void testBytesInDef() {   arrayLoadStoreTestCase(true,  "byte",   (byte) 5,  (byte) 6); }
    public void testFloats() {       arrayLoadStoreTestCase(false, "float",  5.0f,      6.0f); }
    public void testFloatsInDef() {  arrayLoadStoreTestCase(true,  "float",  5.0f,      6.0f); }
    public void testDoubles() {      arrayLoadStoreTestCase(false, "double", 5.0d,      6.0d); }
    public void testDoublesInDef() { arrayLoadStoreTestCase(true,  "double", 5.0d,      6.0d); }
    public void testStrings() {      arrayLoadStoreTestCase(false, "String", "cat",     null); }
    public void testStringsInDef() { arrayLoadStoreTestCase(true,  "String", "cat",     null); }
    public void testDef() {          arrayLoadStoreTestCase(true,  "def",    5,         null); }
}
