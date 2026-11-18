/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2;

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * What {@link Re2RegExp} matches and rejects. The expectations are those of Go's {@code regexp}, parsed and anchored
 * as Prometheus does for a label matcher.
 */
public class Re2RegExpTests extends ESTestCase {

    /** RE2 has none of Lucene's optional operators, nor its quoted strings. */
    public void testLuceneOperatorsAreLiterals() {
        assertMatches("a&b", "a&b");
        assertDoesNotMatch("a&b", "ab");
        assertMatches("#", "#");
        assertDoesNotMatch("#", "");
        assertMatches("@", "@");
        assertDoesNotMatch("@", "a");
        assertMatches("~a", "~a");
        assertDoesNotMatch("~a", "b");
        assertMatches("a<1-3>", "a<1-3>");
        assertDoesNotMatch("a<1-3>", "a2");
        assertMatches("\"q\"", "\"q\"");
        assertDoesNotMatch("\"q\"", "q");
        assertMatches("[#-@]+", "#1@");
        assertDoesNotMatch("[#-@]+", "a");
    }

    public void testGroups() {
        assertMatches("(?:ab)+", "abab");
        assertDoesNotMatch("(?:ab)+", "aba");
        assertMatches("(?P<name>a)b", "ab");
        assertDoesNotMatch("(?P<name>a)b", "a");
        assertMatches("(?<name>a|b)c", "bc");
        assertDoesNotMatch("(?<name>a|b)c", "abc");
        assertMatches("a(|b)", "a", "ab");
        assertDoesNotMatch("a(|b)", "b");
        assertMatches("|a", "", "a");
        assertMatches("()", "");
        assertDoesNotMatch("()", "a");
    }

    /** A flag holds to the end of the group it is set in, alternatives included. */
    public void testFlags() {
        assertMatches("(?i)abc", "ABC", "aBc");
        assertDoesNotMatch("(?i)abc", "abd");
        assertMatches("a(?i)b", "aB");
        assertDoesNotMatch("a(?i)b", "AB");
        assertMatches("(?i:a)b", "Ab");
        assertDoesNotMatch("(?i:a)b", "AB");
        assertMatches("(a(?i)b)c", "aBc");
        assertDoesNotMatch("(a(?i)b)c", "aBC");
        assertMatches("(?i)a(?-i)b", "Ab");
        assertDoesNotMatch("(?i)a(?-i)b", "AB");
        assertMatches("a(?i)b|c", "C", "aB");
        assertMatches(".", "\n", "a");
        assertMatches("(?-s).", "a");
        assertDoesNotMatch("(?-s).", "\n");
        assertMatches("(?U)a+", "aaa");
        assertDoesNotMatch("(?U)a+", "");
    }

    /** Case folding takes in a code point's whole orbit, and folds a class before negating it. */
    public void testCaseFolding() {
        assertMatches("(?i)k", "K", "k", "K");
        assertMatches("(?i)\\x{3c3}", "Σ", "ς");
        assertMatches("(?i)[a-c]+", "AbC");
        assertDoesNotMatch("(?i)[a-c]+", "d");
        assertMatches("(?i)[^k]", "a");
        assertDoesNotMatch("(?i)[^k]", "K");
        assertMatches("(?i)\\W", "-");
        assertDoesNotMatch("(?i)\\W", "K", "ſ");
        // the Turkish dotted and dotless i fold to nothing else in RE2
        assertDoesNotMatch("(?i)i", "İ", "ı");
    }

    public void testIgnoreCase() {
        CharacterRunAutomaton run = run(new Re2RegExp("ab(?-i)c", true));
        assertTrue(run.run("ABc"));
        assertFalse(run.run("ABC"));
    }

    public void testEscapes() {
        assertMatches("\\n\\t", "\n\t");
        assertDoesNotMatch("\\n\\t", "nt");
        assertMatches("\\x41\\x{263a}", "A☺");
        assertDoesNotMatch("\\x41\\x{263a}", "x41");
        assertMatches("\\101\\0", "A\u0000");
        assertDoesNotMatch("\\101\\0", "101");
        assertMatches("\\12", "\n");
        assertDoesNotMatch("\\12", "12");
        assertMatches("\\Q.*|\\E+", ".*|", ".*||");
        assertDoesNotMatch("\\Q.*|\\E+", "a");
        assertMatches("\\Qab", "ab");
        assertDoesNotMatch("\\Qab", "a");
        assertMatches("\\.\\-\\_\\ ", ".-_ ");
        assertDoesNotMatch("\\.\\-\\_\\ ", "a-_ ");
    }

    public void testClasses() {
        assertMatches("\\d+\\D", "12a");
        assertDoesNotMatch("\\d+\\D", "123");
        assertMatches("\\s\\S", " a");
        assertDoesNotMatch("\\s\\S", "\u000Ba");
        assertMatches("\\w+", "a_1");
        assertDoesNotMatch("\\w+", "a-1", "α");
        assertMatches("[[:alpha:]]+[[:^digit:]]", "ab-");
        assertDoesNotMatch("[[:alpha:]]+[[:^digit:]]", "ab1");
        assertMatches("[[:space:]]", "\u000B");
        assertDoesNotMatch("[[:space:]]", "a");
        assertMatches("[^a]", "\n");
        assertDoesNotMatch("[^a]", "a");
        assertMatches("[]a]", "]", "a");
        assertDoesNotMatch("[]a]", "b");
        assertMatches("[^]a]", "b");
        assertDoesNotMatch("[^]a]", "]");
        assertMatches("[a-]", "-");
        assertDoesNotMatch("[a-]", "b");
        assertMatches("[\\d-z]", "5", "-", "z");
        assertDoesNotMatch("[\\d-z]", "a");
    }

    public void testUnicodeClasses() {
        assertMatches("\\pL+", "aα中");
        assertDoesNotMatch("\\pL+", "1");
        assertMatches("\\p{Greek}+", "αβ");
        assertDoesNotMatch("\\p{Greek}+", "a");
        assertMatches("\\p{greek}", "α");
        assertMatches("\\PL", "1");
        assertDoesNotMatch("\\PL", "a");
        assertMatches("\\p{^Lu}", "a");
        assertDoesNotMatch("\\p{^Lu}", "A");
        assertMatches("\\p{Letter}", "a");
        assertDoesNotMatch("\\p{Letter}", "1");
        assertMatches("\\p{Any}", "\n");
        assertDoesNotMatch("\\p{Any}", "");
        assertMatches("[\\p{Greek}\\d]", "α", "1");
        assertDoesNotMatch("[\\p{Greek}\\d]", "a");
    }

    /** Prometheus matches the whole value, so {@code ^} and {@code $} hold only at its ends unless {@code (?m)}. */
    public void testAssertions() {
        assertMatches("^abc$", "abc");
        assertDoesNotMatch("a^b", "ab", "a^b");
        assertDoesNotMatch("a$b", "ab", "a$b");
        assertMatches("\\Aab\\z", "ab");
        assertMatches("(?m)a$\\n^b", "a\nb");
        assertDoesNotMatch("(?m)a$\\n^b", "ab");
        assertDoesNotMatch("a$\\n^b", "a\nb");
        assertMatches("foo\\b.*", "foo bar", "foo");
        assertDoesNotMatch("foo\\b.*", "foobar");
        assertDoesNotMatch("a\\bb", "ab");
        assertMatches("a\\Bb", "ab");
        assertDoesNotMatch("\\b", "");
        assertMatches("\\B", "");
        assertMatches(".*\\bx\\b.*", "a x b", "x");
        assertDoesNotMatch(".*\\bx\\b.*", "axb");
    }

    /** A '{' that starts no repeat count is a literal. */
    public void testRepeats() {
        assertMatches("a{2,3}", "aa", "aaa");
        assertDoesNotMatch("a{2,3}", "a", "aaaa");
        assertMatches("a{2,}", "aaaaa");
        assertDoesNotMatch("a{2,}", "a");
        assertMatches("a{2}", "aa");
        assertDoesNotMatch("a{2}", "aaa");
        assertMatches("a{,3}", "a{,3}");
        assertDoesNotMatch("a{,3}", "aaa");
        assertMatches("a{01}", "a{01}");
        assertDoesNotMatch("a{01}", "a");
        assertMatches("a{", "a{");
        assertMatches("a{1,2", "a{1,2");
        assertMatches("{", "{");
        assertMatches("}", "}");
        assertMatches("x*?y+?z??", "yz", "xxy");
        assertDoesNotMatch("x*?y+?z??", "z");
        assertMatches("x{0}y", "y");
        assertDoesNotMatch("x{0}y", "xy");
        assertMatches("^*a", "a");
        // a flag group between them doesn't make the repetition nested
        assertMatches("a(?i)*", "aaa");
        assertDoesNotMatch("a(?i)*", "A");
        assertMatches("a{1000}", "a".repeat(1000));
    }

    public void testInvalid() {
        assertInvalid("(", "error parsing regexp: missing closing ): `(`");
        assertInvalid("a)", "error parsing regexp: unexpected ): `a)`");
        assertInvalid("[a", "error parsing regexp: missing closing ]: `[a`");
        assertInvalid("[]", "error parsing regexp: missing closing ]: `[]`");
        assertInvalid("\\", "error parsing regexp: trailing backslash at end of expression: ``");
        assertInvalid("a**", "error parsing regexp: invalid nested repetition operator: `**`");
        assertInvalid("a*??", "error parsing regexp: invalid nested repetition operator: `*??`");
        assertInvalid("x{2}{3}", "error parsing regexp: invalid nested repetition operator: `{2}{3}`");
        assertInvalid("*a", "error parsing regexp: missing argument to repetition operator: `*`");
        assertInvalid("a|*", "error parsing regexp: missing argument to repetition operator: `*`");
        assertInvalid("(*)", "error parsing regexp: missing argument to repetition operator: `*`");
        assertInvalid("(?i)*", "error parsing regexp: missing argument to repetition operator: `*`");
        assertInvalid("{2}", "error parsing regexp: missing argument to repetition operator: `{2}`");
        assertInvalid("a{1001}", "error parsing regexp: invalid repeat count: `{1001}`");
        assertInvalid("a{2,1}", "error parsing regexp: invalid repeat count: `{2,1}`");
        assertInvalid("(a{500}){3}", "error parsing regexp: invalid repeat count: `{3}`");
        assertInvalid("\\8", "error parsing regexp: invalid escape sequence: `\\8`");
        assertInvalid("\\1", "error parsing regexp: invalid escape sequence: `\\1`");
        assertInvalid("\\x{110000}", "error parsing regexp: invalid escape sequence: `\\x{110000`");
        assertInvalid("\\xg", "error parsing regexp: invalid escape sequence: `\\xg`");
        assertInvalid("\\é", "error parsing regexp: invalid escape sequence: `\\é`");
        assertInvalid("\\C", "error parsing regexp: invalid escape sequence: `\\C`");
        assertInvalid("\\Z", "error parsing regexp: invalid escape sequence: `\\Z`");
        assertInvalid("\\E", "error parsing regexp: invalid escape sequence: `\\E`");
        assertInvalid("[z-a]", "error parsing regexp: invalid character class range: `z-a`");
        assertInvalid("[\\b]", "error parsing regexp: invalid escape sequence: `\\b`");
        assertInvalid("[[:foo:]]", "error parsing regexp: invalid character class range: `[:foo:]`");
        assertInvalid("\\pX", "error parsing regexp: invalid character class range: `\\pX`");
        assertInvalid("\\p{Grek}", "error parsing regexp: invalid character class range: `\\p{Grek}`");
        assertInvalid("\\p{IsGreek}", "error parsing regexp: invalid character class range: `\\p{IsGreek}`");
        assertInvalid("\\p{", "error parsing regexp: invalid character class range: `\\p{`");
        assertInvalid("(?=a)", "error parsing regexp: invalid or unsupported Perl syntax: `(?=`");
        assertInvalid("(?#c)", "error parsing regexp: invalid or unsupported Perl syntax: `(?#`");
        assertInvalid("(?P=a)", "error parsing regexp: invalid or unsupported Perl syntax: `(?P`");
        assertInvalid("(?i-)", "error parsing regexp: invalid or unsupported Perl syntax: `(?i-)`");
        assertInvalid("(?--i)", "error parsing regexp: invalid or unsupported Perl syntax: `(?--`");
        assertInvalid("(?x)a", "error parsing regexp: invalid or unsupported Perl syntax: `(?x`");
        assertInvalid("(?<>a)", "error parsing regexp: invalid named capture: `(?<>`");
        assertInvalid("(?P<a-b>c)", "error parsing regexp: invalid named capture: `(?P<a-b>`");
    }

    /**
     * RE2/J, which this parser is forked from and ES|QL uses for {@code label_replace}, agrees on what random
     * expressions match and reject. The expressions avoid where the fork follows Go instead: a repeated literal
     * <code>&#123;</code>, a capture group name used twice, {@code (?i)} on a negated Unicode class, and alternatives
     * that start with the same literal in different cases.
     */
    public void testAgreesWithRe2j() {
        List<String> values = values();
        for (int i = 0; i < 200; i++) {
            StringBuilder regexBuilder = new StringBuilder();
            for (int tokens = between(1, 7); tokens > 0; tokens--) {
                regexBuilder.append(randomFrom(TOKENS));
            }
            String regex = regexBuilder.toString();
            if (regex.contains("|") && regex.contains("(?i")) {
                continue;
            }
            Pattern re2j;
            try {
                re2j = Pattern.compile(regex, Pattern.DOTALL);
            } catch (PatternSyntaxException e) {
                expectThrows(IllegalArgumentException.class, "[" + regex + "] should be invalid", () -> new Re2RegExp(regex));
                continue;
            }
            CharacterRunAutomaton run = run(new Re2RegExp(regex));
            for (String value : values) {
                assertThat("[" + regex + "] on [" + value + "]", run.run(value), equalTo(re2j.matches(value)));
            }
        }
    }

    private static final List<String> TOKENS = List.of(
        "a",
        "b",
        "A",
        "k",
        "\u212a",
        "_",
        " ",
        "-",
        "1",
        "#",
        "&",
        "~",
        "@",
        "<",
        ">",
        "\"",
        "\u017f",
        "\u03b1",
        ".",
        "*",
        "+",
        "?",
        "|",
        "(",
        ")",
        "[",
        "]",
        "^",
        "$",
        "}",
        ":",
        ",",
        "2",
        "0",
        "(?i)",
        "(?-i)",
        "(?s)",
        "(?-s)",
        "(?m)",
        "(?-m)",
        "(?:",
        "(?i:",
        "(?U)",
        "(?is)",
        "(?i-s:",
        "\\b",
        "\\B",
        "\\A",
        "\\z",
        "\\d",
        "\\D",
        "\\w",
        "\\W",
        "\\s",
        "\\S",
        "\\pL",
        "\\PL",
        "\\p{Greek}",
        "\\p{Lu}",
        "\\pN",
        "[[:alpha:]]",
        "[[:^alpha:]]",
        "[[:word:]]",
        "[^a]",
        "[a-c]",
        "[^\\n]",
        "[ak]",
        "[^k]",
        "[\\w-]",
        "[]a]",
        "[a-]",
        "[-a]",
        "{2}",
        "{1,2}",
        "{0,}",
        "{0}",
        "\\x41",
        "\\x{3b1}",
        "\\n",
        "\\Q",
        "\\E",
        "\\.",
        "\\-",
        "\\#",
        "\\101",
        "\\0",
        "\\\\"
    );

    private static final int[] ALPHABET = { 'a', 'b', 'A', 'k', 0x212A, '_', ' ', '\n', '-', '1', '#', 0x3B1 };

    /** Every string of up to two code points of {@link #ALPHABET}, and some longer ones. */
    private static List<String> values() {
        List<String> values = new ArrayList<>();
        values.add("");
        for (int first : ALPHABET) {
            values.add(Character.toString(first));
            for (int second : ALPHABET) {
                values.add(Character.toString(first) + Character.toString(second));
            }
        }
        for (int i = 0; i < 20; i++) {
            StringBuilder value = new StringBuilder();
            for (int length = between(3, 6); length > 0; length--) {
                value.appendCodePoint(ALPHABET[between(0, ALPHABET.length - 1)]);
            }
            values.add(value.toString());
        }
        return values;
    }

    private static void assertMatches(String regex, String... values) {
        CharacterRunAutomaton run = run(new Re2RegExp(regex));
        for (String value : values) {
            assertTrue("[" + regex + "] should match [" + value + "]", run.run(value));
        }
    }

    private static void assertDoesNotMatch(String regex, String... values) {
        CharacterRunAutomaton run = run(new Re2RegExp(regex));
        for (String value : values) {
            assertFalse("[" + regex + "] should not match [" + value + "]", run.run(value));
        }
    }

    private static void assertInvalid(String regex, String message) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new Re2RegExp(regex));
        assertThat(regex, e.getMessage(), equalTo(message));
    }

    private static CharacterRunAutomaton run(Re2RegExp regExp) {
        return new CharacterRunAutomaton(Operations.determinize(regExp.toAutomaton(), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT));
    }
}
