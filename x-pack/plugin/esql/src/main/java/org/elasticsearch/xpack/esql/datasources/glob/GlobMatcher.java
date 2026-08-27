/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Matches object keys against a glob pattern.
 *
 * <p>The pattern is parsed once into segments and tokens and matched directly. It is deliberately NOT translated
 * into a Java regular expression, which is what the previous implementation did and where its defects came from:
 * writing in two languages at once let regex meaning leak through the seam. {@code &&} inside a class became Java's
 * set-intersection operator and silently matched nothing; {@code [[]} — the exact shape
 * {@code GlobExpander.escapeGlobMeta} emits to escape a literal bracket — failed to compile; {@code [[:digit:]]}
 * matched the characters of the class name rather than digits; and a leading {@code **} became an unanchored
 * {@code .*} that could stop halfway through a filename, so {@code **}{@code /events.csv} also matched
 * {@code old_events.csv}. Matching directly removes the seam, so none of those are representable.
 *
 * <p>It also removes a denial of service. Repeated {@code **} compiled to nested optional {@code .*} groups, which
 * backtrack exponentially: ten of them took seconds on a short path and fifteen did not finish. Segment matching
 * here is a memoised walk over (pattern segment, path segment), so cost is bounded by their product.
 *
 * <p><b>The language.</b> ClickHouse is the reference — a pattern written for its s3 table function means the same
 * thing here — with two deliberate departures, both documented:
 * <ul>
 *   <li>character classes ({@code [abc]}, {@code [a-z]}, {@code [!abc]}) are supported; ClickHouse treats brackets
 *       as literals. Since {@code \} is a literal too, a one-character class is the only way to match a literal
 *       metacharacter, and Hadoop, Spark, git and bash all have classes.</li>
 *   <li>malformed patterns are rejected rather than silently reinterpreted.</li>
 * </ul>
 *
 * <p>Constructs: {@code *} (any run within one segment), {@code ?} (one character within one segment), {@code **}
 * (zero or more whole segments, only when it is a complete segment), {@code [...]} classes, {@code {a,b}}
 * alternation and {@code {N..M}} numeric ranges. {@code \} is a literal character, not an escape.
 */
final class GlobMatcher {

    private static final char PATH_SEP = StoragePath.PATH_SEPARATOR.charAt(0);
    private static final String DOUBLE_STAR = "**";

    /** Ceiling on one brace group, so a range such as {@code {1..1000000}} is refused rather than expanded. */
    private static final int MAX_BRACE_ALTERNATIVES = 1024;

    /** A parsed pattern segment: either the globstar, or the tokens of one name. */
    private record Segment(boolean globstar, List<Token> tokens) {}

    private sealed interface Token {}

    private record Literal(String text) implements Token {}

    /** {@code *} — any run of characters, never crossing a separator. */
    private record Star() implements Token {}

    /** {@code ?} — exactly one character, never a separator. */
    private record AnyChar() implements Token {}

    /**
     * A character class. {@code negated} additionally excludes the separator: a class is a single-character
     * construct and {@code *} does not cross a separator, so neither may a negated class — the previous
     * implementation let {@code x[!a]y} match {@code x/y}.
     */
    private record CharClass(boolean negated, String chars, char[] rangeLow, char[] rangeHigh) implements Token {
        boolean matches(char c) {
            if (c == PATH_SEP) {
                return false;
            }
            boolean hit = chars.indexOf(c) >= 0;
            for (int i = 0; hit == false && i < rangeLow.length; i++) {
                hit = c >= rangeLow[i] && c <= rangeHigh[i];
            }
            return hit != negated;
        }
    }

    /** {@code {a,b}} — alternatives, each a token sequence in its own right. Not nestable. */
    private record Alternation(List<List<Token>> alternatives) implements Token {}

    private final String glob;
    private final List<Segment> segments;
    private final boolean recursive;

    GlobMatcher(String glob) {
        if (glob == null) {
            throw new IllegalArgumentException("glob pattern cannot be null");
        }
        this.glob = glob;
        this.recursive = glob.contains(DOUBLE_STAR);
        this.segments = parse(glob);
    }

    boolean matches(String relativePath) {
        if (relativePath == null) {
            return false;
        }
        List<String> path = split(relativePath);
        return matchSegments(0, 0, path, new Byte[segments.size() + 1][path.size() + 1]);
    }

    boolean needsRecursion() {
        return recursive;
    }

    String glob() {
        return glob;
    }

    @Override
    public String toString() {
        return "GlobMatcher[" + glob + "]";
    }

    private static List<String> split(String path) {
        List<String> out = new ArrayList<>();
        int start = 0;
        for (int i = 0; i <= path.length(); i++) {
            if (i == path.length() || path.charAt(i) == PATH_SEP) {
                out.add(path.substring(start, i));
                start = i + 1;
            }
        }
        return out;
    }

    /**
     * Memoised walk over (pattern segment, path segment). The globstar consumes zero or more path segments, and
     * memoising on the pair is what keeps that from becoming the exponential search the regex engine performed.
     */
    private boolean matchSegments(int pi, int si, List<String> path, Byte[][] memo) {
        Byte cached = memo[pi][si];
        if (cached != null) {
            return cached == 1;
        }
        boolean result;
        if (pi == segments.size()) {
            result = si == path.size();
        } else {
            Segment segment = segments.get(pi);
            if (segment.globstar()) {
                result = false;
                for (int k = si; k <= path.size() && result == false; k++) {
                    result = matchSegments(pi + 1, k, path, memo);
                }
            } else {
                result = si < path.size() && matchName(segment.tokens(), 0, path.get(si), 0) && matchSegments(pi + 1, si + 1, path, memo);
            }
        }
        memo[pi][si] = (byte) (result ? 1 : 0);
        return result;
    }

    /** Matches one segment's tokens against one name. Names are short; the recursion is bounded by their product. */
    private static boolean matchName(List<Token> tokens, int ti, String name, int ni) {
        if (ti == tokens.size()) {
            return ni == name.length();
        }
        Token token = tokens.get(ti);
        if (token instanceof Literal literal) {
            return name.startsWith(literal.text(), ni) && matchName(tokens, ti + 1, name, ni + literal.text().length());
        }
        if (token instanceof AnyChar) {
            return ni < name.length() && name.charAt(ni) != PATH_SEP && matchName(tokens, ti + 1, name, ni + 1);
        }
        if (token instanceof CharClass charClass) {
            return ni < name.length() && charClass.matches(name.charAt(ni)) && matchName(tokens, ti + 1, name, ni + 1);
        }
        if (token instanceof Star) {
            for (int k = ni; k <= name.length(); k++) {
                if (matchName(tokens, ti + 1, name, k)) {
                    return true;
                }
            }
            return false;
        }
        Alternation alternation = (Alternation) token;
        for (List<Token> alternative : alternation.alternatives()) {
            BitSet ends = reachableEnds(alternative, 0, name, ni);
            for (int end = ends.nextSetBit(0); end >= 0; end = ends.nextSetBit(end + 1)) {
                if (matchName(tokens, ti + 1, name, end)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Every position one alternative could end at, so the tokens after the group can be tried from each. */
    private static BitSet reachableEnds(List<Token> tokens, int ti, String name, int ni) {
        BitSet ends = new BitSet(name.length() + 1);
        if (ti == tokens.size()) {
            ends.set(ni);
            return ends;
        }
        Token token = tokens.get(ti);
        if (token instanceof Literal literal) {
            if (name.startsWith(literal.text(), ni)) {
                ends.or(reachableEnds(tokens, ti + 1, name, ni + literal.text().length()));
            }
        } else if (token instanceof AnyChar) {
            if (ni < name.length() && name.charAt(ni) != PATH_SEP) {
                ends.or(reachableEnds(tokens, ti + 1, name, ni + 1));
            }
        } else if (token instanceof CharClass charClass) {
            if (ni < name.length() && charClass.matches(name.charAt(ni))) {
                ends.or(reachableEnds(tokens, ti + 1, name, ni + 1));
            }
        } else if (token instanceof Star) {
            for (int k = ni; k <= name.length(); k++) {
                ends.or(reachableEnds(tokens, ti + 1, name, k));
            }
        }
        return ends;
    }

    // -- parsing --

    private static List<Segment> parse(String glob) {
        List<Segment> out = new ArrayList<>();
        for (String raw : split(glob)) {
            out.add(DOUBLE_STAR.equals(raw) ? new Segment(true, List.of()) : new Segment(false, parseName(raw, glob)));
        }
        return out;
    }

    private static List<Token> parseName(String name, String whole) {
        List<Token> tokens = new ArrayList<>();
        StringBuilder literal = new StringBuilder();
        int i = 0;
        while (i < name.length()) {
            char c = name.charAt(i);
            if (c == '*' || c == '?' || c == '[' || c == '{') {
                if (literal.isEmpty() == false) {
                    tokens.add(new Literal(literal.toString()));
                    literal.setLength(0);
                }
            }
            switch (c) {
                case '*' -> {
                    // A run of stars inside a segment is a plain star: only a segment that is exactly `**` is the
                    // globstar. Matches ClickHouse, which gives zero-level semantics to a whole path component only.
                    while (i < name.length() && name.charAt(i) == '*') {
                        i++;
                    }
                    tokens.add(new Star());
                }
                case '?' -> {
                    tokens.add(new AnyChar());
                    i++;
                }
                case '[' -> i = parseClass(name, i, tokens, whole);
                case '{' -> i = parseBraces(name, i, tokens, whole);
                default -> {
                    literal.append(c);
                    i++;
                }
            }
        }
        if (literal.isEmpty() == false) {
            tokens.add(new Literal(literal.toString()));
        }
        return tokens;
    }

    private static int parseClass(String name, int open, List<Token> tokens, String whole) {
        int i = open + 1;
        boolean negated = false;
        if (i < name.length() && (name.charAt(i) == '!' || name.charAt(i) == '^')) {
            negated = true;
            i++;
        }
        // POSIX syntax is an inner [:name:] inside the class, so the marker is "[:" here, not ":".
        if (name.startsWith("[:", i)) {
            throw new IllegalArgumentException(
                "Invalid glob pattern [" + whole + "]: POSIX character classes such as [[:digit:]] are not supported"
            );
        }
        StringBuilder chars = new StringBuilder();
        List<Character> low = new ArrayList<>();
        List<Character> high = new ArrayList<>();
        boolean first = true;
        while (i < name.length() && (name.charAt(i) != ']' || first)) {
            char c = name.charAt(i);
            first = false;
            if (i + 2 < name.length() && name.charAt(i + 1) == '-' && name.charAt(i + 2) != ']') {
                char hi = name.charAt(i + 2);
                if (c > hi) {
                    throw new IllegalArgumentException("Invalid glob pattern [" + whole + "]: reversed range [" + c + "-" + hi + "]");
                }
                low.add(c);
                high.add(hi);
                i += 3;
            } else {
                chars.append(c);
                i++;
            }
        }
        if (i >= name.length()) {
            throw new IllegalArgumentException("Invalid glob pattern [" + whole + "]: unterminated character class, missing ']'");
        }
        char[] lo = new char[low.size()];
        char[] hi = new char[high.size()];
        for (int k = 0; k < low.size(); k++) {
            lo[k] = low.get(k);
            hi[k] = high.get(k);
        }
        tokens.add(new CharClass(negated, chars.toString(), lo, hi));
        return i + 1;
    }

    private static int parseBraces(String name, int open, List<Token> tokens, String whole) {
        int close = name.indexOf('}', open + 1);
        if (close < 0) {
            throw new IllegalArgumentException("Invalid glob pattern [" + whole + "]: unterminated brace group, missing '}'");
        }
        String body = name.substring(open + 1, close);
        if (body.indexOf('{') >= 0) {
            throw new IllegalArgumentException("Invalid glob pattern [" + whole + "]: nested brace groups are not supported");
        }
        String[] spellings = BraceExpander.expandBraceContent(body, MAX_BRACE_ALTERNATIVES);
        if (spellings == null) {
            throw new IllegalArgumentException(
                "Invalid glob pattern [" + whole + "]: brace group expands to more than " + MAX_BRACE_ALTERNATIVES + " alternatives"
            );
        }
        List<List<Token>> alternatives = new ArrayList<>(spellings.length);
        for (String spelling : spellings) {
            // Each alternative is itself a mini-glob: a metacharacter inside one used to leak into the emitted
            // regex, so `{a*,b}` matched the empty string and not `ax`.
            alternatives.add(parseName(spelling, whole));
        }
        tokens.add(new Alternation(alternatives));
        return close + 1;
    }
}
