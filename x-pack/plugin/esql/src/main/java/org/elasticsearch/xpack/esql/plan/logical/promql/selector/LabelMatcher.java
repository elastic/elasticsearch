/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.selector;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.lucene.util.automaton.MinimizationOperations;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.NodeStringRenderable;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.esql.core.util.StringUtils.EMPTY;
import static org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher.Matcher.NEQ;
import static org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher.Matcher.NREG;

/**
 * PromQL label matcher between a label name, a value pattern and match type (=, !=, =~, !~).
 *
 * Examples:
 *   {job="api"}              -> [LabelMatcher("job", "api", EQ)]
 *   {status=~"5.."}          -> [LabelMatcher("status", "5..", REG)]
 *   {env!~"test|dev"}        -> [LabelMatcher("env", "test|dev", NREG)]
 *   {host=?_hosts}           -> [LabelMatcher("host", ["34.107.161.234", "140.248.133.94"], EQ)]
 */
public class LabelMatcher implements NodeStringRenderable {

    public static final String NAME = "__name__";

    /** RE2 flag {@code i}: letters match in any case. */
    private static final int CASE_INSENSITIVE = 1;
    /** RE2 flag {@code s} cleared: {@code .} does not match a newline. Prometheus sets {@code s} by default. */
    private static final int DOT_NO_NEWLINE = 2;
    private static final int[] NO_CODE_POINTS = new int[0];

    public enum Matcher {
        EQ("=", false),
        NEQ("!=", false),
        REG("=~", true),
        NREG("!~", true);

        public static Matcher from(String value) {
            switch (value) {
                case "=":
                    return EQ;
                case "!=":
                    return NEQ;
                case "=~":
                    return REG;
                case "!~":
                    return NREG;
                default:
                    return null;
            }
        }

        private final String value;
        private final boolean isRegex;

        Matcher(String value, boolean isRegex) {
            this.value = value;
            this.isRegex = isRegex;
        }

        public boolean isRegex() {
            return isRegex;
        }
    }

    private final String name;
    private final List<String> values;
    private final Matcher matcher;

    private Automaton automaton;

    public LabelMatcher(String name, String value, Matcher matcher) {
        this(name, List.of(value), matcher);
    }

    public LabelMatcher(String name, List<String> values, Matcher matcher) {
        this.name = name;
        this.values = values;
        this.matcher = matcher;
    }

    public String name() {
        return name;
    }

    /**
     * Returns the single value for this matcher.
     * For multi-value matchers, returns the first value.
     * Use {@link #values()} to get all values.
     */
    public String getFirstValue() {
        return values.getFirst();
    }

    /**
     * Returns all values for this matcher.
     * For single-value matchers, returns a singleton list.
     */
    public List<String> values() {
        return values;
    }

    /**
     * Returns true if this matcher has multiple values.
     */
    public boolean isMultiValue() {
        return values.size() > 1;
    }

    public Matcher matcher() {
        return matcher;
    }

    // TODO: externalize this to allow pluggable strategies (such as caching across labels/requests)
    public Automaton automaton() {
        if (automaton != null) {
            return automaton;
        }

        Automaton result;
        if (isMultiValue() && matcher.isRegex() == false) {
            // Multi-value exact match: union of all literal values
            List<Automaton> automata = values.stream().map(Automata::makeString).toList();
            result = Operations.union(automata);
        } else if (isMultiValue()) {
            // Multi-value regex: union of all regex patterns
            List<Automaton> automata = values.stream().map(LabelMatcher::regexAutomaton).toList();
            result = Operations.union(automata);
        } else {
            // Single value
            String v = getFirstValue();
            result = matcher.isRegex() ? regexAutomaton(v) : Automata.makeString(v);
        }
        result = MinimizationOperations.minimize(result, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        // negate if needed
        if (matcher == NEQ || matcher == NREG) {
            result = Operations.complement(result, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        }
        automaton = result;
        return automaton;
    }

    /**
     * Lowers a PromQL regex to an {@link RLikePattern}, so that the matcher's automaton and the {@code RLIKE} filter
     * agree on what it matches. PromQL regexes use RE2 syntax, where {@code " # & < @ ~} are literals, but
     * {@link RLikePattern} parses Lucene {@link RegExp} syntax, where they are operators (quoted string, empty language,
     * intersection, interval, any string, complement). They are escaped everywhere except inside a character class, the
     * bracket expression {@code [...]} that matches one character from a set ({@code [a-z#]}): there Lucene reads them
     * literally already, and an escaped character cannot start a range ({@code [#-@]}).
     * <p>
     * Lucene has no group modifiers either, so {@code (?:re)}, {@code (?P<name>re)} and {@code (?<name>re)} become plain
     * groups and the RE2 flags of {@code (?flags)} and {@code (?flags:re)} are applied here: {@code i} spells every
     * cased letter as the set of its cases ({@code (?i)ab} becomes {@code [aA][bB]}) and clearing {@code s} keeps
     * {@code .} off newlines.
     */
    public static RLikePattern convertRegExpToRE2(String regex) {
        StringBuilder sb = new StringBuilder(regex.length());
        // the flags in force where each enclosing group opened, innermost first
        Deque<Integer> enclosingFlags = new ArrayDeque<>();
        int flags = 0;
        int n = regex.length();
        int i = 0;
        while (i < n) {
            int c = regex.codePointAt(i);
            i += Character.charCount(c);
            if (c == '\\' && i < n) {
                int escaped = regex.codePointAt(i);
                i += Character.charCount(escaped);
                sb.append('\\').appendCodePoint(escaped);
            } else if (c == '[') {
                i = appendCharClass(regex, i, (flags & CASE_INSENSITIVE) != 0, sb);
            } else if (c == '(' && i < n && regex.charAt(i) == '?') {
                i++;
                if (i < n && (regex.charAt(i) == 'P' || regex.charAt(i) == '<')) {
                    // named group
                    int close = regex.indexOf('>', i);
                    if (close < 0) {
                        throw new IllegalArgumentException("invalid named capture at position " + i);
                    }
                    enclosingFlags.push(flags);
                    sb.append('(');
                    i = close + 1;
                } else {
                    int end = i;
                    while (end < n && regex.charAt(end) != ':' && regex.charAt(end) != ')') {
                        end++;
                    }
                    if (end == n || (end == i && regex.charAt(end) == ')')) {
                        throw new IllegalArgumentException("missing flags or group at position " + i);
                    }
                    int groupFlags = groupFlags(regex.substring(i, end), flags);
                    if (regex.charAt(end) == ':') {
                        // (?flags:re) and (?:re) open a group with their own flags
                        enclosingFlags.push(flags);
                        sb.append('(');
                    }
                    // (?flags) without a group sets the flags for the rest of the enclosing group
                    flags = groupFlags;
                    i = end + 1;
                }
            } else if (c == '(') {
                enclosingFlags.push(flags);
                sb.append('(');
            } else if (c == ')') {
                if (enclosingFlags.isEmpty() == false) {
                    flags = enclosingFlags.pop();
                }
                sb.append(')');
            } else if (c == '.' && (flags & DOT_NO_NEWLINE) != 0) {
                sb.append("[^\n]");
            } else {
                appendLiteral(c, (flags & CASE_INSENSITIVE) != 0, sb);
            }
        }
        return new RLikePattern(sb.toString());
    }

    /** Applies the RE2 flags of a {@code (?flags)} or {@code (?flags:re)} group, such as {@code i} or {@code i-s}. */
    private static int groupFlags(String spec, int flags) {
        boolean clear = false;
        for (int i = 0; i < spec.length(); i++) {
            char f = spec.charAt(i);
            switch (f) {
                case '-' -> {
                    if (clear || i == spec.length() - 1) {
                        throw new IllegalArgumentException("invalid flags [" + spec + "]");
                    }
                    clear = true;
                }
                case 'i' -> flags = clear ? flags & ~CASE_INSENSITIVE : flags | CASE_INSENSITIVE;
                // s lets '.' match a newline, as Prometheus does by default
                case 's' -> flags = clear ? flags | DOT_NO_NEWLINE : flags & ~DOT_NO_NEWLINE;
                // U only changes greediness, which cannot change what a full match accepts;
                // m only changes '^' and '$', which this conversion leaves alone
                case 'm', 'U' -> {
                }
                default -> throw new IllegalArgumentException("unknown flag [" + f + "] in [" + spec + "]");
            }
        }
        return flags;
    }

    /** Appends a character outside a character class; under {@code i} a cased letter becomes the set of its cases. */
    private static void appendLiteral(int c, boolean caseInsensitive, StringBuilder sb) {
        int[] otherCases = caseInsensitive ? otherCases(c) : NO_CODE_POINTS;
        if (otherCases.length > 0) {
            sb.append('[').appendCodePoint(c);
            for (int other : otherCases) {
                sb.appendCodePoint(other);
            }
            sb.append(']');
            return;
        }
        if (c == '"' || c == '#' || c == '&' || c == '<' || c == '@' || c == '~') {
            sb.append('\\');
        }
        sb.appendCodePoint(c);
    }

    /**
     * Appends a character class, the bracket expression {@code [...]} that matches one character from a set, starting
     * after its {@code '['}. Returns the position after its closing {@code ']'}. Under {@code i} every cased letter in
     * it, or in one of its ranges, adds its other cases to the set.
     */
    private static int appendCharClass(String regex, int i, boolean caseInsensitive, StringBuilder sb) {
        int n = regex.length();
        sb.append('[');
        if (i < n && regex.charAt(i) == '^') {
            sb.append('^');
            i++;
        }
        StringBuilder otherCases = new StringBuilder();
        // a ']' right after '[' or '[^' is a literal, not the end of the character class
        boolean first = true;
        while (i < n && (first || regex.charAt(i) != ']')) {
            first = false;
            int from = i;
            int lo = regex.codePointAt(i);
            i += Character.charCount(lo);
            if (lo == '\\' && i < n) {
                lo = regex.codePointAt(i);
                i += Character.charCount(lo);
                if (Character.isLetter(lo)) {
                    // \d, \s, \w and their negations are sets of their own, not range ends
                    sb.append(regex, from, i);
                    continue;
                }
            } else if (lo == '-') {
                // Lucene reads a '-' after any character as a range, even before the closing ']'
                sb.append("\\-");
                continue;
            }
            int hi = lo;
            if (i + 1 < n && regex.charAt(i) == '-' && regex.charAt(i + 1) != ']') {
                i++;
                hi = regex.codePointAt(i);
                i += Character.charCount(hi);
                if (hi == '\\' && i < n) {
                    hi = regex.codePointAt(i);
                    i += Character.charCount(hi);
                }
            } else if (i < n && regex.charAt(i) == '-') {
                // a '-' right before the closing ']' is a literal
                sb.append(regex, from, i).append("\\-");
                i++;
                from = i;
            }
            sb.append(regex, from, i);
            if (caseInsensitive) {
                for (int c = lo; c <= hi; c++) {
                    for (int other : otherCases(c)) {
                        if (other < lo || other > hi) {
                            otherCases.appendCodePoint(other);
                        }
                    }
                }
            }
        }
        sb.append(otherCases);
        // an unterminated class is left for Lucene to reject
        if (i < n) {
            sb.append(']');
            i++;
        }
        return i;
    }

    /** The code points other than {@code c} that RE2's {@code i} flag matches for it. */
    private static int[] otherCases(int c) {
        int upper = Character.toUpperCase(c);
        return IntStream.of(Character.toLowerCase(c), upper, Character.toTitleCase(c), Character.toLowerCase(upper))
            .filter(other -> other != c)
            .distinct()
            .toArray();
    }

    private static Automaton regexAutomaton(String regex) {
        try {
            return convertRegExpToRE2(regex).createAutomaton(false);
        } catch (IllegalArgumentException e) {
            throw new InvalidArgumentException(e, "Invalid regular expression [{}]: {}", regex, e.getMessage());
        }
    }

    public boolean matchesAll() {
        return Operations.isTotal(automaton());
    }

    public boolean matchesNone() {
        return Operations.isEmpty(automaton());
    }

    public boolean matchesEmpty() {
        return Operations.run(automaton(), EMPTY);
    }

    public boolean isNegation() {
        return matcher == NEQ || matcher == NREG;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LabelMatcher label = (LabelMatcher) o;
        return matcher == label.matcher && Objects.equals(name, label.name) && Objects.equals(values, label.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, values, matcher);
    }

    @Override
    public String toString() {
        return name + matcher.value + values;
    }

    /**
     * Routes the label name and every match value through the mapper (the match operator is
     * structural and stays). Reproduces {@code toString()}'s {@code name op [v1, v2]} shape so that
     * under {@link NodeStringMapper#IDENTITY} this equals {@link #toString()}; under an anonymizing
     * mapper the label name and the (potentially sensitive) values tokenize.
     */
    @Override
    public void nodeString(StringBuilder sb, Node.NodeStringFormat format, NodeStringMapper mapper) {
        sb.append(mapper.column(name)).append(matcher.value).append('[');
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(mapper.column(values.get(i)));
        }
        sb.append(']');
    }
}
