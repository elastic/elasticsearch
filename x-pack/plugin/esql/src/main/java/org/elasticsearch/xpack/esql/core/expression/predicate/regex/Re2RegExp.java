/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.RegExp;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntPredicate;

import static java.util.Map.entry;

/**
 * A regular expression in RE2 syntax, compiled to a Lucene {@link Automaton} that accepts the strings it matches whole.
 * <p>
 * RE2 is the syntax of Go's {@code regexp} package, and so of PromQL label matchers. Lucene's {@link RegExp} has a
 * grammar of its own: {@code " # & < > @ ~} are operators there and literals in RE2, and RE2's groups, flags, escapes,
 * classes and assertions don't parse there. This parses the expression the way Go's {@code regexp/syntax} does with
 * the {@code Perl} and {@code DotNL} flags Prometheus parses label matchers with, rejecting what it rejects, and builds
 * the automaton directly. Unicode classes and case folding come from the JDK's Unicode tables, which may be a newer
 * Unicode version than Go's.
 */
public final class Re2RegExp {

    // parse flags, as in Go's regexp/syntax; (?U) only changes which match is preferred, so it has none
    private static final int FOLD_CASE = 1;   // (?i)
    private static final int ONE_LINE = 2;    // ^ and $ match at the ends of the text only, cleared by (?m)
    private static final int DOT_NL = 4;      // . matches \n, (?s)

    private static final int MAX_REPEAT = 1000;
    private static final int MAX_NESTING = 1000;

    private static final int[] ANY = { 0, Character.MAX_CODE_POINT };
    private static final int[] ANY_BUT_NEWLINE = { 0, '\n' - 1, '\n' + 1, Character.MAX_CODE_POINT };

    private final Node root;

    public Re2RegExp(String pattern) {
        this(pattern, false);
    }

    /**
     * @param ignoreCase whether to match as if the expression started with {@code (?i)}
     * @throws IllegalArgumentException if the expression isn't valid RE2
     */
    public Re2RegExp(String pattern, boolean ignoreCase) {
        this.root = new Parser(pattern, ONE_LINE | DOT_NL | (ignoreCase ? FOLD_CASE : 0)).parse();
    }

    /**
     * The automaton accepting the strings the expression matches whole; it is not determinized.
     */
    public Automaton toAutomaton() {
        return new Compiler().compile(root);
    }

    /** The parsed expression; capture groups are dropped since only which strings match matters here. */
    private sealed interface Node {}

    /** Any one code point in {@code ranges}: sorted, disjoint, non-adjacent {@code [lo, hi]} pairs. */
    private record CharClass(int[] ranges) implements Node {}

    private record Concat(List<Node> nodes) implements Node {}

    private record Alternate(List<Node> nodes) implements Node {}

    /** {@code max} is {@code -1} when unbounded. */
    private record Repeat(Node node, int min, int max) implements Node {}

    private record Assertion(Assert kind) implements Node {}

    /**
     * The zero-width assertions, each a condition on the code points either side of a position. Prometheus anchors
     * the whole expression, so {@code ^} and {@code $} hold only at the ends of the value unless {@code (?m)}.
     */
    private enum Assert {
        BEGIN_TEXT,
        END_TEXT,
        BEGIN_LINE,
        END_LINE,
        WORD_BOUNDARY,
        NO_WORD_BOUNDARY
    }

    // Go's error codes, which its messages and so Prometheus' report
    private static final String INVALID_CHAR_RANGE = "invalid character class range";
    private static final String INVALID_ESCAPE = "invalid escape sequence";
    private static final String INVALID_NAMED_CAPTURE = "invalid named capture";
    private static final String INVALID_PERL_OP = "invalid or unsupported Perl syntax";
    private static final String INVALID_REPEAT_OP = "invalid nested repetition operator";
    private static final String INVALID_REPEAT_SIZE = "invalid repeat count";
    private static final String MISSING_BRACKET = "missing closing ]";
    private static final String MISSING_PAREN = "missing closing )";
    private static final String MISSING_REPEAT_ARGUMENT = "missing argument to repetition operator";
    private static final String TRAILING_BACKSLASH = "trailing backslash at end of expression";
    private static final String UNEXPECTED_PAREN = "unexpected )";
    private static final String NESTING_DEPTH = "expression nests too deeply";

    private static IllegalArgumentException error(String code, String expression) {
        return new IllegalArgumentException("error parsing regexp: " + code + ": `" + expression + "`");
    }

    /**
     * A recursive descent port of Go's {@code regexp/syntax} parser: the same syntax, the same scoping of flags and
     * the same errors.
     */
    private static final class Parser {
        private final String pattern;
        private int pos;
        private int flags;
        private int depth;

        Parser(String pattern, int flags) {
            this.pattern = pattern;
            this.flags = flags;
        }

        Node parse() {
            Node node = parseAlternate();
            if (more()) {
                // an alternation stops early only at a ')' that closes no group
                throw error(UNEXPECTED_PAREN, pattern);
            }
            return node;
        }

        private boolean more() {
            return pos < pattern.length();
        }

        private boolean at(char c) {
            return pos < pattern.length() && pattern.charAt(pos) == c;
        }

        private Node parseAlternate() {
            List<Node> branches = new ArrayList<>();
            branches.add(parseConcat());
            while (at('|')) {
                pos++;
                branches.add(parseConcat());
            }
            return branches.size() == 1 ? branches.getFirst() : new Alternate(branches);
        }

        private Node parseConcat() {
            List<Node> nodes = new ArrayList<>();
            // where the repetition operator just parsed starts, as RE2 rejects one repeating another
            int lastRepeat = -1;
            while (more() && at('|') == false && at(')') == false) {
                int start = pos;
                int repeat = -1;
                switch (pattern.charAt(pos)) {
                    case '(' -> {
                        Node group = parseGroup();
                        if (group != null) {
                            nodes.add(group);
                        }
                    }
                    case '[' -> nodes.add(parseClass());
                    case '.' -> {
                        pos++;
                        nodes.add(new CharClass((flags & DOT_NL) != 0 ? ANY : ANY_BUT_NEWLINE));
                    }
                    case '^' -> {
                        pos++;
                        nodes.add(new Assertion((flags & ONE_LINE) != 0 ? Assert.BEGIN_TEXT : Assert.BEGIN_LINE));
                    }
                    case '$' -> {
                        pos++;
                        nodes.add(new Assertion((flags & ONE_LINE) != 0 ? Assert.END_TEXT : Assert.END_LINE));
                    }
                    case '*' -> {
                        pos++;
                        repeat(nodes, start, lastRepeat, 0, -1);
                        repeat = start;
                    }
                    case '+' -> {
                        pos++;
                        repeat(nodes, start, lastRepeat, 1, -1);
                        repeat = start;
                    }
                    case '?' -> {
                        pos++;
                        repeat(nodes, start, lastRepeat, 0, 1);
                        repeat = start;
                    }
                    case '{' -> {
                        int[] minMax = parseRepeatCount();
                        if (minMax == null) {
                            // a '{' that starts no repeat count is a literal
                            pos++;
                            nodes.add(literal('{'));
                        } else {
                            int min = minMax[0];
                            int max = minMax[1];
                            if (min < 0 || min > MAX_REPEAT || max > MAX_REPEAT || (max >= 0 && min > max)) {
                                throw error(INVALID_REPEAT_SIZE, pattern.substring(start, pos));
                            }
                            repeat(nodes, start, lastRepeat, min, max);
                            repeat = start;
                        }
                    }
                    case '\\' -> parseBackslash(nodes);
                    default -> {
                        int c = pattern.codePointAt(pos);
                        pos += Character.charCount(c);
                        nodes.add(literal(c));
                    }
                }
                lastRepeat = repeat;
            }
            return nodes.size() == 1 ? nodes.getFirst() : new Concat(nodes);
        }

        /** Applies the repetition operator just parsed, from {@code start}, to the last node. */
        private void repeat(List<Node> nodes, int start, int lastRepeat, int min, int max) {
            if (at('?')) {
                // non-greedy: it prefers other matches, but matches the same strings
                pos++;
            }
            if (lastRepeat >= 0) {
                throw error(INVALID_REPEAT_OP, pattern.substring(lastRepeat, pos));
            }
            if (nodes.isEmpty()) {
                throw error(MISSING_REPEAT_ARGUMENT, pattern.substring(start, pos));
            }
            Repeat repeat = new Repeat(nodes.removeLast(), min, max);
            if ((min >= 2 || max >= 2) && repeatIsValid(repeat, MAX_REPEAT) == false) {
                throw error(INVALID_REPEAT_SIZE, pattern.substring(start, pos));
            }
            nodes.add(repeat);
        }

        /** Whether nested counted repeats stay within {@code n} copies of their innermost node, as RE2 requires. */
        private static boolean repeatIsValid(Node node, int n) {
            return switch (node) {
                case Repeat repeat -> {
                    int m = repeat.max() < 0 ? repeat.min() : repeat.max();
                    if (repeat.max() == 0) {
                        yield true;
                    }
                    if (m > n) {
                        yield false;
                    }
                    yield repeatIsValid(repeat.node(), m > 0 ? n / m : n);
                }
                case Concat concat -> concat.nodes().stream().allMatch(sub -> repeatIsValid(sub, n));
                case Alternate alternate -> alternate.nodes().stream().allMatch(sub -> repeatIsValid(sub, n));
                case CharClass c -> true;
                case Assertion a -> true;
            };
        }

        /**
         * Parses {@code {n}}, {@code {n,}} or {@code {n,m}} at {@code pos} into {@code [n, m]}, with {@code -1} for no
         * upper bound and a negative {@code n} for a number too large. Null, leaving {@code pos}, if there is none.
         */
        private int[] parseRepeatCount() {
            int i = pos + 1;
            int min = parseInt(i);
            if (min == Integer.MIN_VALUE) {
                return null;
            }
            i = intEnd;
            int max = min;
            if (i < pattern.length() && pattern.charAt(i) == ',') {
                i++;
                if (i < pattern.length() && pattern.charAt(i) == '}') {
                    max = -1;
                } else {
                    max = parseInt(i);
                    if (max == Integer.MIN_VALUE) {
                        return null;
                    }
                    i = intEnd;
                    if (max < 0) {
                        min = -1;
                    }
                }
            }
            if (i >= pattern.length() || pattern.charAt(i) != '}') {
                return null;
            }
            pos = i + 1;
            return new int[] { min, max };
        }

        private int intEnd;

        /**
         * Parses the decimal number at {@code i}, setting {@code intEnd} past it. {@link Integer#MIN_VALUE} if there is
         * none, as for a leading zero, and {@code -1} if it is too large.
         */
        private int parseInt(int i) {
            int start = i;
            while (i < pattern.length() && pattern.charAt(i) >= '0' && pattern.charAt(i) <= '9') {
                i++;
            }
            if (i == start || (i - start > 1 && pattern.charAt(start) == '0')) {
                return Integer.MIN_VALUE;
            }
            intEnd = i;
            int n = 0;
            for (int d = start; d < i; d++) {
                if (n >= 100_000_000) {
                    return -1;
                }
                n = n * 10 + pattern.charAt(d) - '0';
            }
            return n;
        }

        /**
         * Parses the group at {@code pos}, or a {@code (?flags)} that sets flags for the rest of the enclosing group,
         * for which it returns null.
         */
        private Node parseGroup() {
            int start = pos;
            if (pattern.startsWith("(?", pos) == false) {
                pos++;
                return parseGroupBody(flags);
            }
            int remaining = pattern.length() - pos;
            if ((remaining > 4 && pattern.charAt(pos + 2) == 'P' && pattern.charAt(pos + 3) == '<')
                || (remaining > 3 && pattern.charAt(pos + 2) == '<')) {
                // (?P<name>re) or (?<name>re)
                int begin = pos + (pattern.charAt(pos + 2) == '<' ? 3 : 4);
                int end = pattern.indexOf('>', pos);
                if (end < 0) {
                    throw error(INVALID_NAMED_CAPTURE, pattern.substring(pos));
                }
                if (isValidCaptureName(pattern.substring(begin, end)) == false) {
                    throw error(INVALID_NAMED_CAPTURE, pattern.substring(pos, end + 1));
                }
                pos = end + 1;
                return parseGroupBody(flags);
            }
            // (?flags) or (?flags:re), where flags is like i, -s or im-s
            int groupFlags = flags;
            boolean negated = false;
            boolean sawFlag = false;
            int i = pos + 2;
            while (i < pattern.length()) {
                int c = pattern.codePointAt(i);
                i += Character.charCount(c);
                switch (c) {
                    case 'i' -> {
                        groupFlags = negated ? groupFlags & ~FOLD_CASE : groupFlags | FOLD_CASE;
                        sawFlag = true;
                    }
                    case 'm' -> {
                        groupFlags = negated ? groupFlags | ONE_LINE : groupFlags & ~ONE_LINE;
                        sawFlag = true;
                    }
                    case 's' -> {
                        groupFlags = negated ? groupFlags & ~DOT_NL : groupFlags | DOT_NL;
                        sawFlag = true;
                    }
                    case 'U' -> sawFlag = true;
                    case '-' -> {
                        if (negated) {
                            throw error(INVALID_PERL_OP, pattern.substring(start, i));
                        }
                        negated = true;
                        sawFlag = false;
                    }
                    case ':', ')' -> {
                        if (negated && sawFlag == false) {
                            throw error(INVALID_PERL_OP, pattern.substring(start, i));
                        }
                        pos = i;
                        if (c == ':') {
                            return parseGroupBody(groupFlags);
                        }
                        flags = groupFlags;
                        return null;
                    }
                    default -> throw error(INVALID_PERL_OP, pattern.substring(start, i));
                }
            }
            throw error(INVALID_PERL_OP, pattern.substring(start, i));
        }

        private static boolean isValidCaptureName(String name) {
            return name.isEmpty() == false && name.chars().allMatch(c -> c == '_' || isAlnum(c));
        }

        /** Parses a group's expression and its closing ')' with {@code groupFlags}, which end with the group. */
        private Node parseGroupBody(int groupFlags) {
            if (++depth > MAX_NESTING) {
                throw error(NESTING_DEPTH, pattern);
            }
            int outerFlags = flags;
            flags = groupFlags;
            Node node = parseAlternate();
            if (more() == false) {
                throw error(MISSING_PAREN, pattern);
            }
            pos++;
            flags = outerFlags;
            depth--;
            return node;
        }

        /** Parses what starts with a backslash at {@code pos} outside a character class. */
        private void parseBackslash(List<Node> nodes) {
            if (pos + 1 < pattern.length()) {
                switch (pattern.charAt(pos + 1)) {
                    case 'A' -> {
                        pos += 2;
                        nodes.add(new Assertion(Assert.BEGIN_TEXT));
                        return;
                    }
                    case 'z' -> {
                        pos += 2;
                        nodes.add(new Assertion(Assert.END_TEXT));
                        return;
                    }
                    case 'b' -> {
                        pos += 2;
                        nodes.add(new Assertion(Assert.WORD_BOUNDARY));
                        return;
                    }
                    case 'B' -> {
                        pos += 2;
                        nodes.add(new Assertion(Assert.NO_WORD_BOUNDARY));
                        return;
                    }
                    case 'C' -> throw error(INVALID_ESCAPE, "\\C");
                    case 'Q' -> {
                        // \Q...\E: literal text, up to the end of the expression without an \E
                        int end = pattern.indexOf("\\E", pos + 2);
                        String text = end < 0 ? pattern.substring(pos + 2) : pattern.substring(pos + 2, end);
                        pos = end < 0 ? pattern.length() : end + 2;
                        text.codePoints().forEach(c -> nodes.add(literal(c)));
                        return;
                    }
                    default -> {
                        // a class or an escaped code point, below
                    }
                }
            }
            int[] ranges = parseUnicodeClass();
            if (ranges == null) {
                ranges = parsePerlClass();
            }
            nodes.add(ranges != null ? new CharClass(ranges) : literal(parseEscape()));
        }

        /** Parses the escaped code point at {@code pos}, like {@code \.}, {@code \n}, {@code \x41} or {@code \101}. */
        private int parseEscape() {
            int start = pos;
            pos++;
            if (more() == false) {
                throw error(TRAILING_BACKSLASH, "");
            }
            int c = pattern.codePointAt(pos);
            pos += Character.charCount(c);
            if (c < 0x80 && isAlnum(c) == false) {
                // an escaped ASCII punctuation or space is itself
                return c;
            }
            switch (c) {
                case '0', '1', '2', '3', '4', '5', '6', '7' -> {
                    // octal; a single non-zero digit would be a backreference, which RE2 doesn't support
                    if (c == '0' || (more() && isOctal(pattern.charAt(pos)))) {
                        int value = c - '0';
                        for (int digits = 1; digits < 3 && more() && isOctal(pattern.charAt(pos)); digits++) {
                            value = value * 8 + pattern.charAt(pos++) - '0';
                        }
                        return value;
                    }
                }
                case 'x' -> {
                    int value = parseHex();
                    if (value >= 0) {
                        return value;
                    }
                }
                case 'a' -> {
                    return 0x07;
                }
                case 'f' -> {
                    return '\f';
                }
                case 'n' -> {
                    return '\n';
                }
                case 'r' -> {
                    return '\r';
                }
                case 't' -> {
                    return '\t';
                }
                case 'v' -> {
                    return 0x0B;
                }
                default -> {
                    // an escaped letter or digit that is no escape, or an escaped non-ASCII code point
                }
            }
            throw error(INVALID_ESCAPE, pattern.substring(start, pos));
        }

        /** Parses the {@code 41} or {@code {41}} after {@code \x}; {@code -1} if invalid. */
        private int parseHex() {
            if (more() == false) {
                return -1;
            }
            int c = pattern.codePointAt(pos);
            pos += Character.charCount(c);
            if (c == '{') {
                int value = 0;
                int digits = 0;
                while (true) {
                    if (more() == false) {
                        return -1;
                    }
                    c = pattern.codePointAt(pos);
                    pos += Character.charCount(c);
                    if (c == '}') {
                        return digits == 0 ? -1 : value;
                    }
                    int digit = Character.digit(c, 16);
                    if (c >= 0x80 || digit < 0) {
                        return -1;
                    }
                    value = value * 16 + digit;
                    if (value > Character.MAX_CODE_POINT) {
                        return -1;
                    }
                    digits++;
                }
            }
            int high = c < 0x80 ? Character.digit(c, 16) : -1;
            int low = -1;
            if (more()) {
                c = pattern.codePointAt(pos);
                pos += Character.charCount(c);
                low = c < 0x80 ? Character.digit(c, 16) : -1;
            }
            return high < 0 || low < 0 ? -1 : high * 16 + low;
        }

        /** Parses {@code \d \D \s \S \w \W} at {@code pos}; null if there is none. */
        private int[] parsePerlClass() {
            if (pos + 1 >= pattern.length() || pattern.charAt(pos) != '\\') {
                return null;
            }
            int[] ranges = switch (Character.toLowerCase(pattern.charAt(pos + 1))) {
                case 'd' -> DIGIT;
                case 's' -> PERL_SPACE;
                case 'w' -> WORD;
                default -> null;
            };
            if (ranges == null) {
                return null;
            }
            boolean negate = Character.isUpperCase(pattern.charAt(pos + 1));
            pos += 2;
            return group(ranges, negate);
        }

        /**
         * Parses {@code \pN}, {@code \p{Name}}, {@code \PN} or {@code \P{Name}} at {@code pos}, with {@code Name} a
         * Unicode general category, script or category alias; null if there is none.
         */
        private int[] parseUnicodeClass() {
            if (pos + 1 >= pattern.length() || pattern.charAt(pos) != '\\') {
                return null;
            }
            char p = pattern.charAt(pos + 1);
            if (p != 'p' && p != 'P') {
                return null;
            }
            boolean negate = p == 'P';
            int start = pos;
            int i = pos + 2;
            String sequence;
            String name;
            if (i < pattern.length() && pattern.charAt(i) == '{') {
                int end = pattern.indexOf('}', start);
                if (end < 0) {
                    throw error(INVALID_CHAR_RANGE, pattern.substring(start));
                }
                sequence = pattern.substring(start, end + 1);
                name = pattern.substring(i + 1, end);
                i = end + 1;
            } else {
                if (i < pattern.length()) {
                    i += Character.charCount(pattern.codePointAt(i));
                }
                sequence = pattern.substring(start, i);
                name = sequence.substring(2);
            }
            if (name.startsWith("^")) {
                negate = negate == false;
                name = name.substring(1);
            }
            UnicodeTable table = UnicodeTables.lookup(name);
            if (table == null) {
                throw error(INVALID_CHAR_RANGE, sequence);
            }
            pos = i;
            return group(table.ranges(), table.inverted() != negate);
        }

        /**
         * Parses the bracketed character class at {@code pos}, like {@code [a-z]}, {@code [^\d_]} or
         * {@code [[:alpha:]]}.
         */
        private Node parseClass() {
            int start = pos;
            pos++;
            boolean negate = false;
            if (at('^')) {
                negate = true;
                pos++;
            }
            List<int[]> parts = new ArrayList<>();
            // ']' right after the '[' or '[^' is a literal
            boolean first = true;
            while (first || at(']') == false) {
                first = false;
                if (pos + 2 < pattern.length() && pattern.startsWith("[:", pos)) {
                    int[] named = parsePosixClass();
                    if (named != null) {
                        parts.add(named);
                        continue;
                    }
                }
                int[] ranges = parseUnicodeClass();
                if (ranges == null) {
                    ranges = parsePerlClass();
                }
                if (ranges != null) {
                    parts.add(ranges);
                    continue;
                }
                int rangeStart = pos;
                int lo = parseClassChar(start);
                int hi = lo;
                // [a-] is 'a' or '-'
                if (pos + 1 < pattern.length() && pattern.charAt(pos) == '-' && pattern.charAt(pos + 1) != ']') {
                    pos++;
                    hi = parseClassChar(start);
                    if (hi < lo) {
                        throw error(INVALID_CHAR_RANGE, pattern.substring(rangeStart, pos));
                    }
                }
                int[] range = { lo, hi };
                parts.add((flags & FOLD_CASE) != 0 ? CaseFolding.fold(range) : range);
            }
            pos++;
            int[] ranges = union(parts);
            return new CharClass(negate ? negate(ranges) : ranges);
        }

        private int parseClassChar(int classStart) {
            if (more() == false) {
                throw error(MISSING_BRACKET, pattern.substring(classStart));
            }
            if (at('\\')) {
                return parseEscape();
            }
            int c = pattern.codePointAt(pos);
            pos += Character.charCount(c);
            return c;
        }

        /** Parses a POSIX class like {@code [:alpha:]} or {@code [:^space:]} at {@code pos}; null if there is none. */
        private int[] parsePosixClass() {
            int end = pattern.indexOf(":]", pos + 2);
            if (end < 0) {
                return null;
            }
            String name = pattern.substring(pos, end + 2);
            boolean negate = name.startsWith("[:^");
            int[] ranges = POSIX_CLASSES.get(negate ? "[:" + name.substring(3) : name);
            if (ranges == null) {
                throw error(INVALID_CHAR_RANGE, name);
            }
            pos = end + 2;
            return group(ranges, negate);
        }

        /** A named class, folded under {@code (?i)} before it is negated, as RE2 does. */
        private int[] group(int[] ranges, boolean negate) {
            int[] folded = (flags & FOLD_CASE) != 0 ? CaseFolding.fold(ranges) : ranges;
            return negate ? negate(folded) : folded;
        }

        private CharClass literal(int c) {
            int[] range = { c, c };
            return new CharClass((flags & FOLD_CASE) != 0 ? CaseFolding.fold(range) : range);
        }

        private static boolean isOctal(char c) {
            return c >= '0' && c <= '7';
        }

        private static boolean isAlnum(int c) {
            return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
        }
    }

    private static final int[] DIGIT = { '0', '9' };
    private static final int[] PERL_SPACE = { '\t', '\n', '\f', '\r', ' ', ' ' };
    private static final int[] WORD = { '0', '9', 'A', 'Z', '_', '_', 'a', 'z' };

    private static final Map<String, int[]> POSIX_CLASSES = Map.ofEntries(
        entry("[:alnum:]", new int[] { '0', '9', 'A', 'Z', 'a', 'z' }),
        entry("[:alpha:]", new int[] { 'A', 'Z', 'a', 'z' }),
        entry("[:ascii:]", new int[] { 0, 0x7F }),
        entry("[:blank:]", new int[] { '\t', '\t', ' ', ' ' }),
        entry("[:cntrl:]", new int[] { 0, 0x1F, 0x7F, 0x7F }),
        entry("[:digit:]", DIGIT),
        entry("[:graph:]", new int[] { '!', '~' }),
        entry("[:lower:]", new int[] { 'a', 'z' }),
        entry("[:print:]", new int[] { ' ', '~' }),
        entry("[:punct:]", new int[] { '!', '/', ':', '@', '[', '`', '{', '~' }),
        entry("[:space:]", new int[] { '\t', '\r', ' ', ' ' }),
        entry("[:upper:]", new int[] { 'A', 'Z' }),
        entry("[:word:]", WORD),
        entry("[:xdigit:]", new int[] { '0', '9', 'A', 'F', 'a', 'f' })
    );

    /** The union of {@code [lo, hi]} pair arrays, sorted and merged. */
    private static int[] union(List<int[]> parts) {
        List<int[]> ranges = new ArrayList<>();
        for (int[] part : parts) {
            for (int i = 0; i < part.length; i += 2) {
                ranges.add(new int[] { part[i], part[i + 1] });
            }
        }
        ranges.sort((a, b) -> Integer.compare(a[0], b[0]));
        int[] merged = new int[ranges.size() * 2];
        int n = 0;
        for (int[] range : ranges) {
            if (n > 0 && range[0] <= merged[n - 1] + 1) {
                merged[n - 1] = Math.max(merged[n - 1], range[1]);
            } else {
                merged[n++] = range[0];
                merged[n++] = range[1];
            }
        }
        return Arrays.copyOf(merged, n);
    }

    private static int[] negate(int[] ranges) {
        int[] negated = new int[ranges.length + 2];
        int n = 0;
        int next = 0;
        for (int i = 0; i < ranges.length; i += 2) {
            if (ranges[i] > next) {
                negated[n++] = next;
                negated[n++] = ranges[i] - 1;
            }
            next = ranges[i + 1] + 1;
        }
        if (next <= Character.MAX_CODE_POINT) {
            negated[n++] = next;
            negated[n++] = Character.MAX_CODE_POINT;
        }
        return Arrays.copyOf(negated, n);
    }

    /**
     * Unicode simple case folding as RE2 does it: {@code (?i)} matches a code point with every code point in its
     * orbit, the code points that fold to the same one, like {@code k}, {@code K} and the Kelvin sign U+212A.
     */
    private static final class CaseFolding {
        /** Every code point that has another case, sorted. */
        private static final int[] CODE_POINTS;
        /** {@code ORBITS[i]} is the orbit of {@code CODE_POINTS[i]}, itself included. */
        private static final int[][] ORBITS;

        static {
            Map<Integer, List<Integer>> orbits = new HashMap<>();
            for (int c = 0; c <= Character.MAX_CODE_POINT; c++) {
                orbits.computeIfAbsent(foldKey(c), k -> new ArrayList<>()).add(c);
            }
            Map<Integer, int[]> orbitOf = new HashMap<>();
            for (List<Integer> orbit : orbits.values()) {
                if (orbit.size() > 1) {
                    int[] members = orbit.stream().mapToInt(Integer::intValue).toArray();
                    for (int c : members) {
                        orbitOf.put(c, members);
                    }
                }
            }
            CODE_POINTS = orbitOf.keySet().stream().mapToInt(Integer::intValue).sorted().toArray();
            ORBITS = new int[CODE_POINTS.length][];
            for (int i = 0; i < CODE_POINTS.length; i++) {
                ORBITS[i] = orbitOf.get(CODE_POINTS[i]);
            }
        }

        /** Two code points are in one orbit if they fold to the same key. */
        private static int foldKey(int c) {
            // Go doesn't fold the Turkish dotted and dotless i, which the JDK maps to i and I
            if (c == 0x130 || c == 0x131) {
                return c;
            }
            return Character.toLowerCase(Character.toUpperCase(c));
        }

        /** {@code ranges} and every code point in the orbit of one of theirs. */
        static int[] fold(int[] ranges) {
            List<int[]> parts = new ArrayList<>();
            parts.add(ranges);
            for (int i = 0; i < ranges.length; i += 2) {
                int from = Arrays.binarySearch(CODE_POINTS, ranges[i]);
                for (int j = from >= 0 ? from : -from - 1; j < CODE_POINTS.length && CODE_POINTS[j] <= ranges[i + 1]; j++) {
                    for (int c : ORBITS[j]) {
                        parts.add(new int[] { c, c });
                    }
                }
            }
            return union(parts);
        }
    }

    /** A Unicode class's code points, to be negated if {@code inverted}. */
    private record UnicodeTable(int[] ranges, boolean inverted) {}

    /**
     * The names {@code \p} takes, resolved as Go 1.26 does: case-insensitively and ignoring spaces, underscores and
     * hyphens, against the general categories, their long aliases, and the scripts with a one-word name.
     */
    private static final class UnicodeTables {
        private static final Set<String> CATEGORIES = Set.of(
            "C",
            "Cc",
            "Cf",
            "Cn",
            "Co",
            "Cs",
            "L",
            "Ll",
            "Lm",
            "Lo",
            "Lt",
            "Lu",
            "M",
            "Mc",
            "Me",
            "Mn",
            "N",
            "Nd",
            "Nl",
            "No",
            "P",
            "Pc",
            "Pd",
            "Pe",
            "Pf",
            "Pi",
            "Po",
            "Ps",
            "S",
            "Sc",
            "Sk",
            "Sm",
            "So",
            "Z",
            "Zl",
            "Zp",
            "Zs"
        );

        private static final Map<String, String> CATEGORY_ALIASES = Map.ofEntries(
            entry("Casedletter", "LC"),
            entry("Closepunctuation", "Pe"),
            entry("Cntrl", "Cc"),
            entry("Combiningmark", "M"),
            entry("Connectorpunctuation", "Pc"),
            entry("Control", "Cc"),
            entry("Currencysymbol", "Sc"),
            entry("Dashpunctuation", "Pd"),
            entry("Decimalnumber", "Nd"),
            entry("Digit", "Nd"),
            entry("Enclosingmark", "Me"),
            entry("Finalpunctuation", "Pf"),
            entry("Format", "Cf"),
            entry("Initialpunctuation", "Pi"),
            entry("Letter", "L"),
            entry("Letternumber", "Nl"),
            entry("Lineseparator", "Zl"),
            entry("Lowercaseletter", "Ll"),
            entry("Mark", "M"),
            entry("Mathsymbol", "Sm"),
            entry("Modifierletter", "Lm"),
            entry("Modifiersymbol", "Sk"),
            entry("Nonspacingmark", "Mn"),
            entry("Number", "N"),
            entry("Openpunctuation", "Ps"),
            entry("Other", "C"),
            entry("Otherletter", "Lo"),
            entry("Othernumber", "No"),
            entry("Otherpunctuation", "Po"),
            entry("Othersymbol", "So"),
            entry("Paragraphseparator", "Zp"),
            entry("Privateuse", "Co"),
            entry("Punct", "P"),
            entry("Punctuation", "P"),
            entry("Separator", "Z"),
            entry("Spaceseparator", "Zs"),
            entry("Spacingmark", "Mc"),
            entry("Surrogate", "Cs"),
            entry("Symbol", "S"),
            entry("Titlecaseletter", "Lt"),
            entry("Unassigned", "Cn"),
            entry("Uppercaseletter", "Lu")
        );

        /** Go's scripts reachable through its name lookup, which drops the underscores of multi-word ones. */
        private static final Set<String> SCRIPTS = Set.of(
            "Adlam",
            "Ahom",
            "Arabic",
            "Armenian",
            "Avestan",
            "Balinese",
            "Bamum",
            "Batak",
            "Bengali",
            "Bhaiksuki",
            "Bopomofo",
            "Brahmi",
            "Braille",
            "Buginese",
            "Buhid",
            "Carian",
            "Chakma",
            "Cham",
            "Cherokee",
            "Chorasmian",
            "Common",
            "Coptic",
            "Cuneiform",
            "Cypriot",
            "Cyrillic",
            "Deseret",
            "Devanagari",
            "Dogra",
            "Duployan",
            "Elbasan",
            "Elymaic",
            "Ethiopic",
            "Georgian",
            "Glagolitic",
            "Gothic",
            "Grantha",
            "Greek",
            "Gujarati",
            "Gurmukhi",
            "Han",
            "Hangul",
            "Hanunoo",
            "Hatran",
            "Hebrew",
            "Hiragana",
            "Inherited",
            "Javanese",
            "Kaithi",
            "Kannada",
            "Katakana",
            "Kawi",
            "Kharoshthi",
            "Khmer",
            "Khojki",
            "Khudawadi",
            "Lao",
            "Latin",
            "Lepcha",
            "Limbu",
            "Lisu",
            "Lycian",
            "Lydian",
            "Mahajani",
            "Makasar",
            "Malayalam",
            "Mandaic",
            "Manichaean",
            "Marchen",
            "Medefaidrin",
            "Miao",
            "Modi",
            "Mongolian",
            "Mro",
            "Multani",
            "Myanmar",
            "Nabataean",
            "Nandinagari",
            "Newa",
            "Nko",
            "Nushu",
            "Ogham",
            "Oriya",
            "Osage",
            "Osmanya",
            "Palmyrene",
            "Phoenician",
            "Rejang",
            "Runic",
            "Samaritan",
            "Saurashtra",
            "Sharada",
            "Shavian",
            "Siddham",
            "Sinhala",
            "Sogdian",
            "Soyombo",
            "Sundanese",
            "Syriac",
            "Tagalog",
            "Tagbanwa",
            "Takri",
            "Tamil",
            "Tangsa",
            "Tangut",
            "Telugu",
            "Thaana",
            "Thai",
            "Tibetan",
            "Tifinagh",
            "Tirhuta",
            "Toto",
            "Ugaritic",
            "Vai",
            "Vithkuqi",
            "Wancho",
            "Yezidi",
            "Yi"
        );

        /** The two-letter general category of each {@link Character#getType} value. */
        private static final Map<Integer, String> TYPE_CATEGORIES = Map.ofEntries(
            entry((int) Character.CONTROL, "Cc"),
            entry((int) Character.FORMAT, "Cf"),
            entry((int) Character.UNASSIGNED, "Cn"),
            entry((int) Character.PRIVATE_USE, "Co"),
            entry((int) Character.SURROGATE, "Cs"),
            entry((int) Character.LOWERCASE_LETTER, "Ll"),
            entry((int) Character.MODIFIER_LETTER, "Lm"),
            entry((int) Character.OTHER_LETTER, "Lo"),
            entry((int) Character.TITLECASE_LETTER, "Lt"),
            entry((int) Character.UPPERCASE_LETTER, "Lu"),
            entry((int) Character.COMBINING_SPACING_MARK, "Mc"),
            entry((int) Character.ENCLOSING_MARK, "Me"),
            entry((int) Character.NON_SPACING_MARK, "Mn"),
            entry((int) Character.DECIMAL_DIGIT_NUMBER, "Nd"),
            entry((int) Character.LETTER_NUMBER, "Nl"),
            entry((int) Character.OTHER_NUMBER, "No"),
            entry((int) Character.CONNECTOR_PUNCTUATION, "Pc"),
            entry((int) Character.DASH_PUNCTUATION, "Pd"),
            entry((int) Character.END_PUNCTUATION, "Pe"),
            entry((int) Character.FINAL_QUOTE_PUNCTUATION, "Pf"),
            entry((int) Character.INITIAL_QUOTE_PUNCTUATION, "Pi"),
            entry((int) Character.OTHER_PUNCTUATION, "Po"),
            entry((int) Character.START_PUNCTUATION, "Ps"),
            entry((int) Character.CURRENCY_SYMBOL, "Sc"),
            entry((int) Character.MODIFIER_SYMBOL, "Sk"),
            entry((int) Character.MATH_SYMBOL, "Sm"),
            entry((int) Character.OTHER_SYMBOL, "So"),
            entry((int) Character.LINE_SEPARATOR, "Zl"),
            entry((int) Character.PARAGRAPH_SEPARATOR, "Zp"),
            entry((int) Character.SPACE_SEPARATOR, "Zs")
        );

        private static final Map<String, int[]> CACHE = new ConcurrentHashMap<>();

        static UnicodeTable lookup(String name) {
            String canonical = canonicalName(name);
            switch (canonical) {
                case "Any" -> {
                    return new UnicodeTable(ANY, false);
                }
                case "Ascii" -> {
                    return new UnicodeTable(new int[] { 0, 0x7F }, false);
                }
                case "Assigned" -> {
                    return new UnicodeTable(category("Cn"), true);
                }
                case "Lc" -> {
                    return new UnicodeTable(category("LC"), false);
                }
                default -> {
                    // a category, a script or a category alias, below
                }
            }
            if (CATEGORIES.contains(canonical)) {
                return new UnicodeTable(category(canonical), false);
            }
            if (SCRIPTS.contains(canonical)) {
                Character.UnicodeScript script = Character.UnicodeScript.valueOf(canonical.toUpperCase(Locale.ROOT));
                return new UnicodeTable(CACHE.computeIfAbsent(canonical, k -> ranges(c -> Character.UnicodeScript.of(c) == script)), false);
            }
            String alias = CATEGORY_ALIASES.get(canonical);
            return alias == null ? null : new UnicodeTable(category(alias), false);
        }

        /** A one-letter category is all of its two-letter ones, and LC is Lu, Ll and Lt. */
        private static int[] category(String name) {
            return CACHE.computeIfAbsent(name, k -> ranges(c -> {
                String category = TYPE_CATEGORIES.get(Character.getType(c));
                if (name.length() == 1) {
                    return category.charAt(0) == name.charAt(0);
                }
                if (name.equals("LC")) {
                    return category.equals("Lu") || category.equals("Ll") || category.equals("Lt");
                }
                return category.equals(name);
            }));
        }

        private static int[] ranges(IntPredicate member) {
            List<int[]> ranges = new ArrayList<>();
            int start = -1;
            for (int c = 0; c <= Character.MAX_CODE_POINT + 1; c++) {
                boolean in = c <= Character.MAX_CODE_POINT && member.test(c);
                if (in && start < 0) {
                    start = c;
                } else if (in == false && start >= 0) {
                    ranges.add(new int[] { start, c - 1 });
                    start = -1;
                }
            }
            return union(ranges);
        }

        /** Go's canonical name: an upper case first letter, the rest lower case, without spaces, underscores or hyphens. */
        private static String canonicalName(String name) {
            StringBuilder canonical = new StringBuilder(name.length());
            for (int i = 0; i < name.length(); i++) {
                char c = name.charAt(i);
                if (c == ' ' || c == '_' || c == '-') {
                    continue;
                }
                if (canonical.isEmpty()) {
                    canonical.append(c >= 'a' && c <= 'z' ? (char) (c - 'a' + 'A') : c);
                } else {
                    canonical.append(c >= 'A' && c <= 'Z' ? (char) (c - 'A' + 'a') : c);
                }
            }
            return canonical.toString();
        }
    }

    /**
     * Builds the automaton in two steps. A Thompson NFA, with epsilon and assertion transitions, then the Lucene
     * automaton, whose states pair an NFA state with what an assertion needs to know: the class of the code point
     * before the position, and the classes its passed assertions allow the next one to have.
     */
    private static final class Compiler {
        private static final int EPSILON = -1;
        private static final int CHARS = -2;

        // the classes of the code point on either side of a position; EDGE is before the first or after the last
        private static final int EDGE = 0;
        private static final int NEWLINE = 1;
        private static final int WORD_CHAR = 2;
        private static final int OTHER = 3;
        private static final int ALL_NEXT = 0b1111;

        /** The starts of the runs of code points of one class, and their classes; a run ends where the next starts. */
        private static final int[] RUN_STARTS = { 0, '\n', '\n' + 1, '0', '9' + 1, 'A', 'Z' + 1, '_', '_' + 1, 'a', 'z' + 1 };
        private static final int[] RUN_CLASSES = {
            OTHER,
            NEWLINE,
            OTHER,
            WORD_CHAR,
            OTHER,
            WORD_CHAR,
            OTHER,
            WORD_CHAR,
            OTHER,
            WORD_CHAR,
            OTHER };

        /**
         * The NFA's transitions by state, each {@code {kind, min, max, to}}: kind is an {@link Assert} ordinal,
         * {@link #EPSILON}, or {@link #CHARS} for the code points {@code [min, max]}.
         */
        private final List<List<int[]>> transitions = new ArrayList<>();
        private boolean hasAssertions;

        Automaton compile(Node root) {
            int start = newState();
            int accept = build(root, start);
            return toAutomaton(start, accept);
        }

        private int newState() {
            transitions.add(new ArrayList<>());
            return transitions.size() - 1;
        }

        private void add(int from, int kind, int min, int max, int to) {
            transitions.get(from).add(new int[] { kind, min, max, to });
        }

        /** Adds {@code node}'s NFA starting at {@code from}, returning the state it ends at. */
        private int build(Node node, int from) {
            return switch (node) {
                case CharClass c -> {
                    int to = newState();
                    for (int i = 0; i < c.ranges().length; i += 2) {
                        add(from, CHARS, c.ranges()[i], c.ranges()[i + 1], to);
                    }
                    yield to;
                }
                case Assertion a -> {
                    hasAssertions = true;
                    int to = newState();
                    add(from, a.kind().ordinal(), 0, 0, to);
                    yield to;
                }
                case Concat concat -> {
                    int state = from;
                    for (Node sub : concat.nodes()) {
                        state = build(sub, state);
                    }
                    yield state;
                }
                case Alternate alternate -> {
                    int to = newState();
                    for (Node sub : alternate.nodes()) {
                        int branch = newState();
                        add(from, EPSILON, 0, 0, branch);
                        add(build(sub, branch), EPSILON, 0, 0, to);
                    }
                    yield to;
                }
                case Repeat repeat -> {
                    int state = from;
                    for (int i = 0; i < repeat.min(); i++) {
                        state = build(repeat.node(), state);
                    }
                    if (repeat.max() < 0) {
                        int loop = newState();
                        add(state, EPSILON, 0, 0, loop);
                        add(build(repeat.node(), loop), EPSILON, 0, 0, loop);
                        yield loop;
                    }
                    int to = newState();
                    for (int i = repeat.min(); i < repeat.max(); i++) {
                        add(state, EPSILON, 0, 0, to);
                        state = build(repeat.node(), state);
                    }
                    add(state, EPSILON, 0, 0, to);
                    yield to;
                }
            };
        }

        private Automaton toAutomaton(int start, int accept) {
            Automaton.Builder builder = new Automaton.Builder();
            Map<Integer, Integer> states = new HashMap<>();
            ArrayDeque<Integer> pending = new ArrayDeque<>();
            stateOf(builder, states, pending, start, EDGE);
            while (pending.isEmpty() == false) {
                int config = pending.poll();
                int from = states.get(config);
                int previous = (config >> 4) & 0b11;
                BitSet seen = new BitSet();
                ArrayDeque<Integer> stack = new ArrayDeque<>();
                // the NFA state and the allowed next classes, packed as state * 16 + next
                stack.push((config >> 6) * 16 + (config & ALL_NEXT));
                while (stack.isEmpty() == false) {
                    int reached = stack.pop();
                    if (seen.get(reached)) {
                        continue;
                    }
                    seen.set(reached);
                    int state = reached >> 4;
                    int next = reached & ALL_NEXT;
                    if (state == accept && (next & (1 << EDGE)) != 0) {
                        builder.setAccept(from, true);
                    }
                    for (int[] t : transitions.get(state)) {
                        if (t[0] == EPSILON) {
                            stack.push(t[3] * 16 + next);
                        } else if (t[0] == CHARS) {
                            addChars(builder, states, pending, from, t[1], t[2], next, t[3]);
                        } else {
                            int allowed = next & allowedNext(Assert.values()[t[0]], previous);
                            if (allowed != 0) {
                                stack.push(t[3] * 16 + allowed);
                            }
                        }
                    }
                }
            }
            return builder.finish();
        }

        /** The automaton state for an NFA state reached after a code point of class {@code previous}. */
        private int stateOf(Automaton.Builder builder, Map<Integer, Integer> states, ArrayDeque<Integer> pending, int state, int previous) {
            // without assertions the class before is never asked, so one automaton state per NFA state will do
            int config = (state << 6) | ((hasAssertions ? previous : EDGE) << 4) | ALL_NEXT;
            Integer existing = states.get(config);
            if (existing != null) {
                return existing;
            }
            int created = builder.createState();
            states.put(config, created);
            pending.add(config);
            return created;
        }

        private void addChars(
            Automaton.Builder builder,
            Map<Integer, Integer> states,
            ArrayDeque<Integer> pending,
            int from,
            int min,
            int max,
            int next,
            int to
        ) {
            if (hasAssertions == false) {
                builder.addTransition(from, stateOf(builder, states, pending, to, EDGE), min, max);
                return;
            }
            for (int i = 0; i < RUN_STARTS.length; i++) {
                int runEnd = i + 1 < RUN_STARTS.length ? RUN_STARTS[i + 1] - 1 : Character.MAX_CODE_POINT;
                int lo = Math.max(min, RUN_STARTS[i]);
                int hi = Math.min(max, runEnd);
                if (lo <= hi && (next & (1 << RUN_CLASSES[i])) != 0) {
                    builder.addTransition(from, stateOf(builder, states, pending, to, RUN_CLASSES[i]), lo, hi);
                }
            }
        }

        /** The classes an assertion allows the next code point to have, given the previous one's; none if it fails. */
        private static int allowedNext(Assert kind, int previous) {
            int word = 1 << WORD_CHAR;
            return switch (kind) {
                case BEGIN_TEXT -> previous == EDGE ? ALL_NEXT : 0;
                case END_TEXT -> 1 << EDGE;
                case BEGIN_LINE -> previous == EDGE || previous == NEWLINE ? ALL_NEXT : 0;
                case END_LINE -> 1 << EDGE | 1 << NEWLINE;
                case WORD_BOUNDARY -> previous == WORD_CHAR ? ALL_NEXT & ~word : word;
                case NO_WORD_BOUNDARY -> previous == WORD_CHAR ? word : ALL_NEXT & ~word;
            };
        }
    }
}
