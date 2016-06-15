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

public class RegexTests extends ScriptTestCase {
    public void testPatternAfterReturn() {
        assertEquals(true, exec("return /foo/.matcher(\"foo\").matches()"));
        assertEquals(false, exec("return /foo/.matcher(\"bar\").matches()"));
    }

    public void testSlashesEscapePattern() {
        assertEquals(true, exec("return /\\/\\//.matcher(\"//\").matches()"));
    }

    public void testPatternAfterAssignment() {
        assertEquals(true, exec("def a = /foo/; return a.matcher(\"foo\").matches()"));
    }

    public void testPatternInIfStement() {
        assertEquals(true, exec("if (/foo/.matcher(\"foo\").matches()) { return true } else { return false }"));
    }

    public void testPatternAfterInfixBoolean() {
        assertEquals(true, exec("return false || /foo/.matcher(\"foo\").matches()"));
        assertEquals(true, exec("return true && /foo/.matcher(\"foo\").matches()"));
    }

    public void testPatternAfterUnaryNotBoolean() {
        assertEquals(false, exec("return !/foo/.matcher(\"foo\").matches()"));
        assertEquals(true, exec("return !/foo/.matcher(\"bar\").matches()"));
    }

    public void testInTernaryCondition()  {
        assertEquals(true, exec("return /foo/.matcher(\"foo\").matches() ? true : false"));
        assertEquals(1, exec("def i = 0; i += /foo/.matcher(\"foo\").matches() ? 1 : 1; return i"));
    }

    public void testInTernaryTrueArm()  {
        assertEquals(true, exec("def i = true; return i ? /foo/.matcher(\"foo\").matches() : false"));
    }

    public void testInTernaryFalseArm()  {
        assertEquals(true, exec("def i = false; return i ? false : /foo/.matcher(\"foo\").matches()"));
    }

    public void testRegexInFunction() {
        assertEquals(true, exec("boolean m(String s) {/foo/.matcher(s).matches()} m(\"foo\")"));
    }

    public void testReturnRegexFromFunction() {
        assertEquals(true, exec("Pattern m(boolean a) {a ? /foo/ : /bar/} m(true).matcher(\"foo\").matches()"));
    }

}
