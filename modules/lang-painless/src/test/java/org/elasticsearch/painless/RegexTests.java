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
import java.util.HashSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;

public class RegexTests extends ScriptTestCase {
    public void testPatternAfterReturn() {
        assertEquals(true, exec("return 'foo' ==~ /foo/"));
        assertEquals(false, exec("return 'bar' ==~ /foo/"));
    }

    public void testSlashesEscapePattern() {
        assertEquals(true, exec("return '//' ==~ /\\/\\//"));
    }

    public void testPatternAfterAssignment() {
        assertEquals(true, exec("def a = /foo/; return 'foo' ==~ a"));
    }

    public void testPatternInIfStement() {
        assertEquals(true, exec("if (/foo/.matcher('foo').matches()) { return true } else { return false }"));
        assertEquals(true, exec("if ('foo' ==~ /foo/) { return true } else { return false }"));
    }

    public void testPatternAfterInfixBoolean() {
        assertEquals(true, exec("return false || /foo/.matcher('foo').matches()"));
        assertEquals(true, exec("return true && /foo/.matcher('foo').matches()"));
        assertEquals(true, exec("return false || 'foo' ==~ /foo/"));
        assertEquals(true, exec("return true && 'foo' ==~ /foo/"));
    }

    public void testPatternAfterUnaryNotBoolean() {
        assertEquals(false, exec("return !/foo/.matcher('foo').matches()"));
        assertEquals(true, exec("return !/foo/.matcher('bar').matches()"));
    }

    public void testInTernaryCondition()  {
        assertEquals(true, exec("return /foo/.matcher('foo').matches() ? true : false"));
        assertEquals(1, exec("def i = 0; i += /foo/.matcher('foo').matches() ? 1 : 1; return i"));
        assertEquals(true, exec("return 'foo' ==~ /foo/ ? true : false"));
        assertEquals(1, exec("def i = 0; i += 'foo' ==~ /foo/ ? 1 : 1; return i"));
    }

    public void testInTernaryTrueArm()  {
        assertEquals(true, exec("def i = true; return i ? /foo/.matcher('foo').matches() : false"));
        assertEquals(true, exec("def i = true; return i ? 'foo' ==~ /foo/ : false"));
    }

    public void testInTernaryFalseArm()  {
        assertEquals(true, exec("def i = false; return i ? false : 'foo' ==~ /foo/"));
    }

    public void testRegexInFunction() {
        assertEquals(true, exec("boolean m(String s) {/foo/.matcher(s).matches()} m('foo')"));
        assertEquals(true, exec("boolean m(String s) {s ==~ /foo/} m('foo')"));
    }

    public void testReturnRegexFromFunction() {
        assertEquals(true, exec("Pattern m(boolean a) {a ? /foo/ : /bar/} m(true).matcher('foo').matches()"));
        assertEquals(true, exec("Pattern m(boolean a) {a ? /foo/ : /bar/} 'foo' ==~ m(true)"));
        assertEquals(false, exec("Pattern m(boolean a) {a ? /foo/ : /bar/} m(false).matcher('foo').matches()"));
        assertEquals(false, exec("Pattern m(boolean a) {a ? /foo/ : /bar/} 'foo' ==~ m(false)"));
    }

    public void testCallMatcherDirectly() {
        assertEquals(true, exec("return /foo/.matcher('foo').matches()"));
        assertEquals(false, exec("return /foo/.matcher('bar').matches()"));
    }

    public void testFindInIf() {
        assertEquals(true, exec("if ('fooasdfbasdf' =~ /foo/) {return true} else {return false}"));
        assertEquals(true, exec("if ('1fooasdfbasdf' =~ /foo/) {return true} else {return false}"));
        assertEquals(false, exec("if ('1f11ooasdfbasdf' =~ /foo/) {return true} else {return false}"));
    }

    public void testFindCastToBoolean() {
        assertEquals(true, exec("return (boolean)('fooasdfbasdf' =~ /foo/)"));
        assertEquals(true, exec("return (boolean)('111fooasdfbasdf' =~ /foo/)"));
        assertEquals(false, exec("return (boolean)('fo11oasdfbasdf' =~ /foo/)"));
    }

    public void testFindOrStringConcat() {
        assertEquals(true, exec("return 'f' + 'o' + 'o' =~ /foo/"));
    }

    public void testFindOfDef() {
        assertEquals(true, exec("def s = 'foo'; return s =~ /foo/"));
    }

    public void testFindOnInput() {
        assertEquals(true, exec("return params.s =~ /foo/", singletonMap("s", "fooasdfdf")));
        assertEquals(false, exec("return params.s =~ /foo/", singletonMap("s", "11f2ooasdfdf")));
    }

    public void testGroup() {
        assertEquals("foo", exec("Matcher m = /foo/.matcher('foo'); m.find(); return m.group()"));
    }

    public void testNumberedGroup() {
        assertEquals("o", exec("Matcher m = /(f)(o)o/.matcher('foo'); m.find(); return m.group(2)"));
    }

    public void testNamedGroup() {
        assertEquals("o", exec("Matcher m = /(?<first>f)(?<second>o)o/.matcher('foo'); m.find(); return m.namedGroup('second')"));
    }

    // Make sure some methods on Pattern are whitelisted
    public void testSplit() {
        assertArrayEquals(new String[] {"cat", "dog"}, (String[]) exec("/,/.split('cat,dog')"));
    }

    public void testSplitAsStream() {
        assertEquals(new HashSet<>(Arrays.asList("cat", "dog")), exec("/,/.splitAsStream('cat,dog').collect(Collectors.toSet())"));
    }

    // Make sure the flags are set
    public void testMultilineFlag() {
        assertEquals(Pattern.MULTILINE, exec("/./m.flags()"));
    }

    public void testSinglelineFlag() {
        assertEquals(Pattern.DOTALL, exec("/./s.flags()"));
    }

    public void testInsensitiveFlag() {
        assertEquals(Pattern.CASE_INSENSITIVE, exec("/./i.flags()"));
    }

    public void testExtendedFlag() {
        assertEquals(Pattern.COMMENTS, exec("/./x.flags()"));
    }

    public void testUnicodeCaseFlag() {
        assertEquals(Pattern.UNICODE_CASE, exec("/./u.flags()"));
    }

    public void testUnicodeCharacterClassFlag() {
        assertEquals(Pattern.UNICODE_CASE | Pattern.UNICODE_CHARACTER_CLASS, exec("/./U.flags()"));
    }

    public void testLiteralFlag() {
        assertEquals(Pattern.LITERAL, exec("/./l.flags()"));
    }

    public void testCanonicalEquivalenceFlag() {
        assertEquals(Pattern.CANON_EQ, exec("/./c.flags()"));
    }

    public void testManyFlags() {
        assertEquals(Pattern.CANON_EQ | Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.COMMENTS, exec("/./ciux.flags()"));
    }

    public void testCantUsePatternCompile() {
        IllegalArgumentException e = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("Pattern.compile('aa')");
        });
        assertEquals("Unknown call [compile] with [1] arguments on type [Pattern].", e.getMessage());
    }

    public void testBadRegexPattern() {
        PatternSyntaxException e = expectScriptThrows(PatternSyntaxException.class, () -> {
            exec("/\\ujjjj/"); // Invalid unicode
        });
        assertThat(e.getMessage(), containsString("Illegal Unicode escape sequence near index 2"));
        assertThat(e.getMessage(), containsString("\\ujjjj"));
    }

    public void testRegexAgainstNumber() {
        ClassCastException e = expectScriptThrows(ClassCastException.class, () -> {
            exec("12 ==~ /cat/");
        });
        assertEquals("Cannot cast from [int] to [String].", e.getMessage());
    }

    public void testBogusRegexFlag() {
        IllegalArgumentException e = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("/asdf/b", emptyMap(), emptyMap(), null); // Not picky so we get a non-assertion error
        });
        assertEquals("unexpected token ['b'] was expecting one of [{<EOF>, ';'}].", e.getMessage());
    }
}
