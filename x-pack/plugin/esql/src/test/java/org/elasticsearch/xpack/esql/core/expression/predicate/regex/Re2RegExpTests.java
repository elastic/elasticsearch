/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import org.apache.lucene.tests.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.TrackingCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2.Pattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2.RE2;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

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
        CharacterRunAutomaton run = run(new Re2RegExp("ab(?-i)c", FLAGS | RE2.FOLD_CASE));
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
     * The automaton built from a random expression and the matcher {@code label_replace} runs on the same parse agree
     * on what it matches: the strings the automaton accepts and random strings over the expression's alphabet.
     */
    public void testAgreesWithMatcher() {
        assertAgreesWithMatcher(randomRegex(2));
    }

    /**
     * An alternation of literals, the shape of most label matchers, is built without parsing it; it matches what the
     * matcher does, anchors and groups around it included.
     */
    public void testLiteralsAgreeWithMatcher() {
        StringBuilder regex = new StringBuilder(randomFrom("", "^"));
        String group = randomFrom("", "(", "(?:");
        regex.append(group);
        for (int alternatives = between(1, 5); alternatives > 0; alternatives--) {
            for (int length = between(0, 4); length > 0; length--) {
                regex.appendCodePoint(randomFrom(ALPHABET));
            }
            if (alternatives > 1) {
                regex.append('|');
            }
        }
        regex.append(group.isEmpty() ? "" : ")").append(randomFrom("", "$"));
        assertAgreesWithMatcher(regex.toString());
    }

    public void testLiterals() {
        assertMatches("", "");
        assertDoesNotMatch("", "a");
        assertMatches("^$", "");
        assertMatches("^(?:)$", "");
        assertMatches("a|", "a", "");
        assertMatches("|", "");
        assertMatches("^(?:foo|bar)$", "foo", "bar");
        assertDoesNotMatch("^(?:foo|bar)$", "^foo", "foo|bar", "(foo)");
        assertMatches("(a)", "a");
        assertMatches("b|a|b", "a", "b");
        assertMatches("\u212a|\u03b1", "\u212a", "\u03b1");
        assertDoesNotMatch("k", "K", "\u212a");
    }

    private static void assertAgreesWithMatcher(String regex) {
        Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
        Automaton automaton;
        try {
            automaton = Operations.determinize(new Re2RegExp(regex, FLAGS).toAutomaton(), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        } catch (TooComplexToDeterminizeException e) {
            // an expression this complex is rejected rather than matched, as Lucene's regexp is
            assumeNoException("[" + regex + "] is too complex to determinize", e);
            return;
        }
        CharacterRunAutomaton run = new CharacterRunAutomaton(automaton);
        if (Operations.isEmpty(automaton) == false) {
            AutomatonTestUtil.RandomAcceptedStrings accepted = new AutomatonTestUtil.RandomAcceptedStrings(automaton);
            for (int i = 0; i < 20; i++) {
                int[] codePoints = accepted.getRandomAcceptedString(random());
                String value = new String(codePoints, 0, codePoints.length);
                assertTrue("[" + regex + "] should match [" + value + "]", pattern.matches(value));
            }
        }
        for (int i = 0; i < 100; i++) {
            StringBuilder value = new StringBuilder();
            for (int length = between(0, 6); length > 0; length--) {
                value.appendCodePoint(randomFrom(ALPHABET));
            }
            assertThat("[" + regex + "] on [" + value + "]", run.run(value.toString()), equalTo(pattern.matches(value.toString())));
        }
    }

    /** The parser flags Prometheus parses a label matcher with. */
    private static final int FLAGS = RE2.PERL | RE2.DOT_NL;

    private static final List<Integer> ALPHABET = List.of(
        (int) 'a',
        (int) 'b',
        (int) 'A',
        (int) 'k',
        0x212A,
        0x17F,
        (int) '_',
        (int) ' ',
        (int) '\n',
        (int) '-',
        (int) '1',
        (int) '#',
        0x3B1,
        0x391
    );

    private static final List<String> CLASSES = List.of(
        ".",
        "[a-c]",
        "[^k]",
        "[ak]",
        "[\\w-]",
        "[^\\n]",
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
        "\\p{^Ll}",
        "[[:alpha:]]",
        "[[:^alpha:]]",
        "\\x41",
        "\\x{3b1}"
    );

    private static final List<String> ASSERTIONS = List.of("^", "$", "\\A", "\\z", "\\b", "\\B");

    private static final List<String> FLAG_GROUPS = List.of("(?i:", "(?s:", "(?m:", "(?U:", "(?-i:", "(?im:", "(?i-s:");

    private static final List<String> QUANTIFIERS = List.of("*", "+", "?", "*?", "+?", "??", "{2}", "{0,2}", "{1,}", "{0}", "{1,2}?");

    /** A random valid expression over {@link #ALPHABET}, nested at most {@code depth} groups deep. */
    private static String randomRegex(int depth) {
        StringBuilder regex = new StringBuilder();
        for (int branches = between(1, 3); branches > 0; branches--) {
            for (int terms = between(0, 3); terms > 0; terms--) {
                regex.append(randomTerm(depth));
            }
            if (branches > 1) {
                regex.append('|');
            }
        }
        return regex.toString();
    }

    private static String randomTerm(int depth) {
        String atom = switch (between(0, depth > 0 ? 4 : 2)) {
            case 0 -> Pattern.quote(Character.toString(randomFrom(ALPHABET)));
            case 1 -> randomFrom(CLASSES);
            case 2 -> randomFrom(ASSERTIONS);
            case 3 -> randomFrom("(", "(?:", "(?P<n>") + randomRegex(depth - 1) + ")";
            case 4 -> randomFrom(FLAG_GROUPS) + randomRegex(depth - 1) + ")";
            default -> throw new AssertionError("unexpected term");
        };
        return randomBoolean() ? atom : atom + randomFrom(QUANTIFIERS);
    }

    private static void assertMatches(String regex, String... values) {
        CharacterRunAutomaton run = run(new Re2RegExp(regex, FLAGS));
        for (String value : values) {
            assertTrue("[" + regex + "] should match [" + value + "]", run.run(value));
        }
    }

    /**
     * A program that fits the breaker but whose transitions don't: 100 copies of a class of 4,000 ranges is 100
     * instructions and 400,000 transitions. The builder charges the transitions as it adds them, stops when the breaker
     * trips, and leaves nothing reserved.
     */
    public void testTransitionsAreChargedToTheBreaker() {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(ByteSizeValue.ofMb(1).getBytes());
        Re2RegExp regExp = new Re2RegExp(wideClass(4000) + "{100}", FLAGS);
        expectThrows(CircuitBreakingException.class, () -> regExp.toAutomaton(breaker, "test"));
        assertThat("past the program's reservation", breaker.peak(), greaterThan(100 * Re2RegExp.BYTES_PER_INSTRUCTION));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** A program too large for the breaker is refused before it is simplified and compiled. */
    public void testProgramIsReservedBeforeItIsCompiled() {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(ByteSizeValue.ofMb(1).getBytes());
        // 100 repeats of 1,000 copies: 100,000 instructions
        Re2RegExp regExp = new Re2RegExp("\\pL{1000}".repeat(100), FLAGS);
        expectThrows(CircuitBreakingException.class, () -> regExp.toAutomaton(breaker, "test"));
        assertThat(breaker.peak(), equalTo(0L));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * Simplifying collapses a plus of a plus, so the reservation counts it once: 50 nested pluses in a repeat of 1,000
     * would be 51,000 instructions counted each, past the breaker, and are 2,000.
     */
    public void testNestedIdenticalRepeatsAreCountedOnce() {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(ByteSizeValue.ofMb(1).getBytes());
        String regex = "(?:" + "(?:".repeat(50) + "a+" + ")+".repeat(50) + "){1000}";
        Re2RegExp regExp = new Re2RegExp(regex, FLAGS);
        assertThat(regExp.toAutomaton(breaker, "test").getNumStates(), greaterThan(0));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** Without a breaker, the size of the build is still bounded. */
    public void testBuildIsBoundedWithoutBreaker() {
        // 1,000 copies of a class of 4,000 ranges, each a single rune, take 4,000,000 transitions, past the bound
        Re2RegExp regExp = new Re2RegExp(wideClass(4000) + "{1000}", FLAGS);
        expectThrows(TooComplexToDeterminizeException.class, regExp::toAutomaton);
    }

    /** A class of {@code ranges} single runes, every other one from U+1000, so that none merges with the next. */
    private static String wideClass(int ranges) {
        StringBuilder regex = new StringBuilder("[");
        for (int i = 0; i < ranges; i++) {
            regex.append("\\x{").append(Integer.toHexString(0x1000 + 2 * i)).append('}');
        }
        return regex.append(']').toString();
    }

    private static void assertDoesNotMatch(String regex, String... values) {
        CharacterRunAutomaton run = run(new Re2RegExp(regex, FLAGS));
        for (String value : values) {
            assertFalse("[" + regex + "] should not match [" + value + "]", run.run(value));
        }
    }

    private static void assertInvalid(String regex, String message) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new Re2RegExp(regex, FLAGS));
        assertThat(regex, e.getMessage(), equalTo(message));
    }

    private static CharacterRunAutomaton run(Re2RegExp regExp) {
        return new CharacterRunAutomaton(Operations.determinize(regExp.toAutomaton(), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT));
    }
}
