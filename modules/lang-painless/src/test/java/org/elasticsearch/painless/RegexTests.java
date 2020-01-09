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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptException;

import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.regex.Pattern;

import static java.util.Collections.singletonMap;

public class RegexTests extends ScriptTestCase {
    @Override
    protected Settings scriptEngineSettings() {
        // Enable regexes just for this test. They are disabled by default.
        return Settings.builder()
                .put(CompilerSettings.REGEX_ENABLED.getKey(), true)
                .build();
    }

    public void testPatternAfterReturn() {
        assertEquals(true, exec("return 'foo' ==~ /foo/"));
        assertEquals(false, exec("return 'bar' ==~ /foo/"));
    }

    public void testBackslashEscapesForwardSlash() {
        assertEquals(true, exec("'//' ==~ /\\/\\//"));
    }

    public void testBackslashEscapeBackslash() {
        // Both of these are single backslashes but java escaping + Painless escaping....
        assertEquals(true, exec("'\\\\' ==~ /\\\\/"));
    }

    public void testRegexIsNonGreedy() {
        assertEquals(true, exec("def s = /\\\\/.split('.\\\\.'); return s[1] ==~ /\\./"));
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
        assertEquals(true, exec("return params.s =~ /foo/", singletonMap("s", "fooasdfdf"), true));
        assertEquals(false, exec("return params.s =~ /foo/", singletonMap("s", "11f2ooasdfdf"), true));
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
        assertEquals(new HashSet<String>(Arrays.asList("cat", "dog")), exec("/,/.splitAsStream('cat,dog').collect(Collectors.toSet())"));
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

    public void testReplaceAllMatchesString() {
        assertEquals("thE qUIck brOwn fOx", exec("'the quick brown fox'.replaceAll(/[aeiou]/, m -> m.group().toUpperCase(Locale.ROOT))"));
    }

    public void testReplaceAllMatchesCharSequence() {
        CharSequence charSequence = CharBuffer.wrap("the quick brown fox");
        assertEquals("thE qUIck brOwn fOx",
                exec("params.a.replaceAll(/[aeiou]/, m -> m.group().toUpperCase(Locale.ROOT))", singletonMap("a", charSequence), true));
    }

    public void testReplaceAllNoMatchString() {
        assertEquals("i am cat", exec("'i am cat'.replaceAll(/dolphin/, m -> m.group().toUpperCase(Locale.ROOT))"));
    }

    public void testReplaceAllNoMatchCharSequence() {
        CharSequence charSequence = CharBuffer.wrap("i am cat");
        assertEquals("i am cat",
                exec("params.a.replaceAll(/dolphin/, m -> m.group().toUpperCase(Locale.ROOT))", singletonMap("a", charSequence), true));
    }

    public void testReplaceAllQuoteReplacement() {
        assertEquals("th/E q/U/Ick br/Own f/Ox",
                exec("'the quick brown fox'.replaceAll(/[aeiou]/, m -> '/' + m.group().toUpperCase(Locale.ROOT))"));
        assertEquals("th$E q$U$Ick br$Own f$Ox",
                exec("'the quick brown fox'.replaceAll(/[aeiou]/, m -> '$' + m.group().toUpperCase(Locale.ROOT))"));
    }

    public void testReplaceFirstMatchesString() {
        assertEquals("thE quick brown fox",
                exec("'the quick brown fox'.replaceFirst(/[aeiou]/, m -> m.group().toUpperCase(Locale.ROOT))"));
    }

    public void testReplaceFirstMatchesCharSequence() {
        CharSequence charSequence = CharBuffer.wrap("the quick brown fox");
        assertEquals("thE quick brown fox",
                exec("params.a.replaceFirst(/[aeiou]/, m -> m.group().toUpperCase(Locale.ROOT))", singletonMap("a", charSequence), true));
    }

    public void testReplaceFirstNoMatchString() {
        assertEquals("i am cat", exec("'i am cat'.replaceFirst(/dolphin/, m -> m.group().toUpperCase(Locale.ROOT))"));
    }

    public void testReplaceFirstNoMatchCharSequence() {
        CharSequence charSequence = CharBuffer.wrap("i am cat");
        assertEquals("i am cat",
                exec("params.a.replaceFirst(/dolphin/, m -> m.group().toUpperCase(Locale.ROOT))", singletonMap("a", charSequence), true));
    }

    public void testReplaceFirstQuoteReplacement() {
        assertEquals("th/E quick brown fox",
                exec("'the quick brown fox'.replaceFirst(/[aeiou]/, m -> '/' + m.group().toUpperCase(Locale.ROOT))"));
        assertEquals("th$E quick brown fox",
                exec("'the quick brown fox'.replaceFirst(/[aeiou]/, m -> '$' + m.group().toUpperCase(Locale.ROOT))"));
    }

    public void testCantUsePatternCompile() {
        IllegalArgumentException e = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("Pattern.compile('aa')");
        });
        assertTrue(e.getMessage().contains("[java.util.regex.Pattern, compile/1]"));
    }

    public void testBadRegexPattern() {
        ScriptException e = expectThrows(ScriptException.class, () -> {
            exec("/\\ujjjj/"); // Invalid unicode
        });
        assertEquals("Error compiling regex: Illegal Unicode escape sequence", e.getCause().getMessage());

        // And make sure the location of the error points to the offset inside the pattern
        assertScriptStack(e,
                "/\\ujjjj/",
                "   ^---- HERE");
    }

    public void testRegexAgainstNumber() {
        ClassCastException e = expectScriptThrows(ClassCastException.class, () -> {
            exec("12 ==~ /cat/");
        });
        assertEquals("Cannot cast from [int] to [java.lang.String].", e.getMessage());
    }

    public void testBogusRegexFlag() {
        IllegalArgumentException e = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("/asdf/b", false); // Not picky so we get a non-assertion error
        });
        assertEquals("unexpected token ['b'] was expecting one of [{<EOF>, ';'}].", e.getMessage());
    }
}
