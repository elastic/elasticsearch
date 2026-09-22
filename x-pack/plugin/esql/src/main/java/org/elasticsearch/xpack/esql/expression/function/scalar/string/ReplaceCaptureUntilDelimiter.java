/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Idiom-detected byte-scan fast path for {@link Replace}, for regexes of the shape
 * {@code ^prefix(?opt-prefix)*([^C]+)C.*$} with replacement {@code litPrefix$1litSuffix} -- e.g.
 * "extract the substring up to the first occurrence of a delimiter, after skipping a fixed
 * (possibly optional) literal prefix". ClickBench's {@code REPLACE(Referer,
 * "^https?://(?:www\.)?([^/]+)/.*$", "$1")} (extract URL host) is the motivating case.
 * <p>
 * {@link #extract} detects the shape at plan time (when {@code regex} and {@code newStr} are both
 * constant) and compiles it into an {@link Idiom}; {@link Replace#toEvaluator} substitutes the
 * generated {@code ReplaceCaptureUntilDelimiterEvaluator} (built from {@link #process} below) in place
 * of the regex-engine-based evaluators when detection succeeds. {@link #process} then executes the
 * idiom with a hand-written byte scan over the raw UTF-8 bytes that never converts to {@link String},
 * counts codepoints, or invokes the regex engine on the common path -- see that method for why this
 * matters.
 */
final class ReplaceCaptureUntilDelimiter {
    private ReplaceCaptureUntilDelimiter() {}

    /**
     * Compiled fast-path plan produced by {@link #extract}. {@code originalPattern}/{@code
     * originalNewStr} are kept only for the rare per-row fallback to the real regex engine, documented
     * on {@link #process}.
     */
    record Idiom(
        // A plain array, not a List: avoids Iterator allocation/indirection in the per-row hot loop in
        // process -- a for-each over an array compiles to a simple indexed loop.
        PrefixPart[] prefix,
        byte delimiter,
        byte[] replacementPrefix,
        byte[] replacementSuffix,
        boolean tailAnchored,
        boolean tailDotAll,
        Pattern originalPattern,
        BytesRef originalNewStr
    ) {
        /** One segment of the literal prefix walk: a run of literal UTF-8 bytes, required or optional. */
        record PrefixPart(byte[] literal, boolean optional) {}
    }

    // Bounds the per-row backtrack search (2^n combinations) in process to a handful of attempts;
    // real-world prefixes (e.g. "s?", "(?:www\.)?") have at most 1-2.
    private static final int MAX_OPTIONAL_PREFIX_PARTS = 4;

    /**
     * Detects the {@link Idiom} shape in a constant {@code regex}/{@code newStr} pair, or returns
     * {@code null} if it doesn't fit. Conservative by design, mirroring {@link Replace#extractLiteralPrefix}:
     * bails on anything that would make the byte-scan's behavior diverge from the actual regex engine,
     * rather than trying to approximate it.
     */
    static Idiom extract(Pattern pattern, BytesRef newStrRef) {
        int flags = pattern.flags();
        int disqualifying = Pattern.MULTILINE | Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.COMMENTS | Pattern.CANON_EQ
            | Pattern.LITERAL;
        if ((flags & disqualifying) != 0) {
            return null;
        }
        boolean dotAll = (flags & Pattern.DOTALL) != 0;

        String regex = pattern.pattern();
        int n = regex.length();
        if (n == 0) {
            return null;
        }

        int i = 0;
        if (regex.charAt(i) == '^') {
            i++;
        } else if (regex.startsWith("\\A", i)) {
            i += 2;
        } else {
            return null;
        }
        if (Replace.containsUnquotedAlternation(regex, i)) {
            return null;
        }

        List<Idiom.PrefixPart> parts = new ArrayList<>();
        StringBuilder run = new StringBuilder();
        int lastAtomStart = -1;

        while (i < n) {
            char c = regex.charAt(i);

            // (?:literal)? -- a whole literal chunk that is optional as one unit (or required, if no
            // trailing '?', e.g. "(?:foo)bar" is just "foobar").
            if (regex.startsWith("(?:", i)) {
                int close = matchingParen(regex, i + 2);
                if (close < 0) {
                    return null;
                }
                byte[] body = parsePureLiteral(regex, i + 3, close);
                if (body == null) {
                    return null;
                }
                if (run.length() > 0) {
                    parts.add(new Idiom.PrefixPart(run.toString().getBytes(StandardCharsets.UTF_8), false));
                    run.setLength(0);
                    lastAtomStart = -1;
                }
                i = close + 1;
                boolean optional = i < n && regex.charAt(i) == '?';
                if (optional) {
                    i++;
                }
                parts.add(new Idiom.PrefixPart(body, optional));
                continue;
            }

            // Our target capturing group -- flush the pending literal run and hand off.
            if (c == '(') {
                if (run.length() > 0) {
                    parts.add(new Idiom.PrefixPart(run.toString().getBytes(StandardCharsets.UTF_8), false));
                }
                return finishCaptureGroup(pattern, regex, i, n, parts, dotAll, newStrRef);
            }

            // \Q...\E literal section; a trailing quantifier (handled below) drops only its last codepoint.
            if (c == '\\' && i + 1 < n && regex.charAt(i + 1) == 'Q') {
                int end = regex.indexOf("\\E", i + 2);
                int quoteEnd = (end < 0) ? n : end;
                if (quoteEnd > i + 2) {
                    int chunkStart = run.length();
                    run.append(regex, i + 2, quoteEnd);
                    lastAtomStart = run.offsetByCodePoints(run.length(), -1);
                    if (lastAtomStart < chunkStart) {
                        lastAtomStart = chunkStart;
                    }
                }
                if (end < 0) {
                    return null; // unterminated \Q...\E with no capture group after it -- not our shape.
                }
                i = end + 2;
                continue;
            }

            if (c == '\\') {
                if (i + 1 >= n || Replace.isEscapedLiteral(regex.charAt(i + 1)) == false) {
                    return null; // backreferences, \w/\d/\s/\b/\A/\z/\Z, etc. -- not our shape.
                }
                lastAtomStart = run.length();
                run.append(regex.charAt(i + 1));
                i += 2;
                continue;
            }

            // Trailing '?' on the immediately-preceding single atom: split it off as its own optional part.
            if (c == '?') {
                if (lastAtomStart < 0) {
                    return null;
                }
                if (lastAtomStart > 0) {
                    parts.add(new Idiom.PrefixPart(run.substring(0, lastAtomStart).getBytes(StandardCharsets.UTF_8), false));
                }
                parts.add(new Idiom.PrefixPart(run.substring(lastAtomStart).getBytes(StandardCharsets.UTF_8), true));
                run.setLength(0);
                lastAtomStart = -1;
                i++;
                continue;
            }

            // Repeat quantifiers and any other meta construct: not modeled by this idiom, bail.
            if (c == '*' || c == '{' || c == '+' || Replace.isRegexMeta(c)) {
                return null;
            }

            // Plain literal code point (possibly a surrogate pair).
            lastAtomStart = run.length();
            if (Character.isHighSurrogate(c) && i + 1 < n && Character.isLowSurrogate(regex.charAt(i + 1))) {
                run.append(c).append(regex.charAt(i + 1));
                i += 2;
            } else {
                run.append(c);
                i++;
            }
        }
        return null; // ran off the end of the pattern without finding a capturing group.
    }

    /**
     * Index of the {@code )} matching the {@code (} at {@code from - 1} (i.e. {@code from} is the index
     * right after {@code "(?:"}), or -1 if unterminated. Skips escapes and {@code \Q...\E} sections so it
     * doesn't mistake an escaped/quoted paren for a real one.
     */
    private static int matchingParen(String regex, int from) {
        int depth = 1;
        int n = regex.length();
        int i = from;
        while (i < n) {
            char c = regex.charAt(i);
            if (c == '\\' && i + 1 < n) {
                if (regex.charAt(i + 1) == 'Q') {
                    int end = regex.indexOf("\\E", i + 2);
                    i = (end < 0) ? n : end + 2;
                    continue;
                }
                i += 2;
                continue;
            }
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
            i++;
        }
        return -1;
    }

    /**
     * UTF-8 bytes of {@code regex[from, to)} if it's entirely literal (plain chars, escaped meta chars,
     * {@code \Q...\E} content), or {@code null} if it contains anything else (nested groups, classes,
     * quantifiers, alternation, ...) or is empty.
     */
    private static byte[] parsePureLiteral(String regex, int from, int to) {
        StringBuilder out = new StringBuilder();
        int i = from;
        while (i < to) {
            char c = regex.charAt(i);
            if (c == '\\' && i + 1 < to && regex.charAt(i + 1) == 'Q') {
                int end = regex.indexOf("\\E", i + 2);
                int quoteEnd = (end < 0 || end > to) ? to : end;
                out.append(regex, i + 2, quoteEnd);
                i = (end < 0 || end > to) ? to : end + 2;
                continue;
            }
            if (c == '\\') {
                if (i + 1 >= to || Replace.isEscapedLiteral(regex.charAt(i + 1)) == false) {
                    return null;
                }
                out.append(regex.charAt(i + 1));
                i += 2;
                continue;
            }
            if (Replace.isRegexMeta(c)) {
                return null;
            }
            out.append(c);
            i++;
        }
        if (out.isEmpty()) {
            return null;
        }
        return out.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Parses {@code ([^C]+)C.*$?} starting at the {@code (} found at index {@code i}, and pairs it with
     * a {@code prefix$1suffix}-shaped {@code newStrRef}, to complete an {@link Idiom}. Returns
     * {@code null} if anything doesn't match this exact shape.
     */
    private static Idiom finishCaptureGroup(
        Pattern pattern,
        String regex,
        int i,
        int n,
        List<Idiom.PrefixPart> parts,
        boolean dotAll,
        BytesRef newStrRef
    ) {
        // Must be the pattern's ONLY capturing group, so `$1` in the replacement is unambiguous and no
        // other group affects matching.
        if (pattern.matcher("").groupCount() != 1) {
            return null;
        }
        // Reject "(?" variants (non-capturing was already handled above; named groups, lookarounds, etc.
        // are not our shape).
        if (i + 1 < n && regex.charAt(i + 1) == '?') {
            return null;
        }
        i++; // consume '('

        if (i + 1 >= n || regex.charAt(i) != '[' || regex.charAt(i + 1) != '^') {
            return null;
        }
        i += 2;

        int delimiterCp;
        if (i < n && regex.charAt(i) == '\\') {
            if (i + 1 >= n || Replace.isEscapedLiteral(regex.charAt(i + 1)) == false) {
                return null;
            }
            delimiterCp = regex.charAt(i + 1);
            i += 2;
        } else if (i < n && Replace.isRegexMeta(regex.charAt(i)) == false) {
            delimiterCp = regex.charAt(i);
            i++;
        } else {
            return null;
        }
        if (delimiterCp > 0x7F) {
            return null; // the byte scan only supports a single-byte (ASCII) delimiter.
        }
        if (i >= n || regex.charAt(i) != ']') {
            return null; // more than one char in the negated class -- not our shape.
        }
        i++;

        if (i >= n || regex.charAt(i) != '+') {
            return null;
        }
        i++;
        if (i < n && regex.charAt(i) == '?') {
            i++; // lazy `+?` converges to the same single valid split point as greedy `+` here.
        }
        if (i >= n || regex.charAt(i) != ')') {
            return null;
        }
        i++; // consume ')'

        // The literal delimiter must repeat immediately after the group, and be the SAME character:
        // that's what guarantees "first occurrence of C" is the unique valid split point.
        int litCp;
        int afterLit;
        if (i < n && regex.charAt(i) == '\\') {
            if (i + 1 >= n || Replace.isEscapedLiteral(regex.charAt(i + 1)) == false) {
                return null;
            }
            litCp = regex.charAt(i + 1);
            afterLit = i + 2;
        } else if (i < n && Replace.isRegexMeta(regex.charAt(i)) == false) {
            litCp = regex.charAt(i);
            afterLit = i + 1;
        } else {
            return null;
        }
        if (litCp != delimiterCp) {
            return null;
        }
        i = afterLit;

        if (i + 1 >= n || regex.charAt(i) != '.' || regex.charAt(i + 1) != '*') {
            return null;
        }
        i += 2;

        boolean tailAnchored = false;
        if (i < n && regex.charAt(i) == '$') {
            tailAnchored = true;
            i++;
        }
        if (i != n) {
            return null; // trailing content after the tail wildcard -- not our shape.
        }

        int numOptional = 0;
        for (Idiom.PrefixPart p : parts) {
            if (p.optional()) {
                numOptional++;
            }
        }
        if (numOptional > MAX_OPTIONAL_PREFIX_PARTS) {
            return null;
        }

        byte[][] replacementParts = extractLiteralReplacementParts(newStrRef);
        if (replacementParts == null) {
            return null;
        }

        return new Idiom(
            parts.toArray(new Idiom.PrefixPart[0]),
            (byte) delimiterCp,
            replacementParts[0],
            replacementParts[1],
            tailAnchored,
            dotAll,
            pattern,
            newStrRef
        );
    }

    /**
     * Splits a replacement string into {@code [literalPrefixBytes, literalSuffixBytes]} if it has the
     * exact shape {@code literal + "$1" + literal} (one group reference, no other {@code $} or
     * backslash escapes), or returns {@code null} otherwise.
     */
    private static byte[][] extractLiteralReplacementParts(BytesRef newStrRef) {
        String s = newStrRef.utf8ToString();
        if (s.indexOf('\\') >= 0) {
            return null; // avoid re-implementing Matcher.appendReplacement's backslash-escaping rules.
        }
        int dollarIdx = s.indexOf('$');
        if (dollarIdx < 0 || dollarIdx + 1 >= s.length() || s.charAt(dollarIdx + 1) != '1') {
            return null;
        }
        if (dollarIdx + 2 < s.length() && Character.isDigit(s.charAt(dollarIdx + 2))) {
            return null; // e.g. "$10" -- Matcher's max-munch group parsing wouldn't mean group 1 alone.
        }
        if (s.indexOf('$', dollarIdx + 2) >= 0) {
            return null; // more than one group reference.
        }
        String prefix = s.substring(0, dollarIdx);
        String suffix = s.substring(dollarIdx + 2);
        return new byte[][] { prefix.getBytes(StandardCharsets.UTF_8), suffix.getBytes(StandardCharsets.UTF_8) };
    }

    /**
     * Hand-written byte scan equivalent to {@link Replace#process(BytesRef, Pattern, byte[], BytesRef)}
     * for regexes matching {@link Idiom}'s shape. Unlike the regex engine, this never converts
     * {@code str} to a {@link String} or counts codepoints -- it walks {@code str}'s raw UTF-8 bytes
     * directly, which is safe because every literal compared against is itself a complete UTF-8 byte
     * sequence (same reasoning as {@link Replace#startsWith}).
     * <p>
     * The common case is a single deterministic, greedy pass over the prefix segments -- peeking at each
     * optional segment's bytes and consuming it only if actually present, exactly like Java regex's
     * first (most-greedy) attempt at a {@code ?} quantifier -- so it costs no more than a hand-unrolled
     * check per segment, with no combinatorial branching. Only if that greedy pass leaves an empty
     * capture (an optional segment happened to consume right up to the delimiter, e.g. {@code "www./x"}
     * against {@code (?:www\.)?([^/]+)/}) does {@link #processWithBacktracking} take over, brute-forcing
     * all 2^n combinations of the optional segments (bounded by {@link #MAX_OPTIONAL_PREFIX_PARTS}),
     * most-greedy-first, to reproduce Java regex's backtracking order.
     * <p>
     * {@code tailAnchored} patterns (i.e. ending in {@code $}, not just a bare trailing {@code .*}) rely
     * on {@code .*$} always matching the remainder as a single run through to the true end of input --
     * true unless the remainder contains a line terminator anywhere, including as its last character(s)
     * (default, non-{@link Pattern#DOTALL} semantics; see {@link #hasLineTerminator} for why even a
     * trailing one disqualifies the fast path). That case is rare (URLs/log fields essentially never embed
     * raw newlines) but is checked per-row via {@link #hasLineTerminator}, which word-scans the remainder
     * rather than paying a per-byte cost; on the rare hit, this defers to the real regex engine via
     * {@code idiom.originalPattern()}/{@code idiom.originalNewStr()} rather than risk an incorrect result.
     */
    @Evaluator(warnExceptions = IllegalArgumentException.class)
    static BytesRef process(BytesRef str, @Fixed(includeInToString = false) Idiom idiom) {
        if (str == null) {
            return null;
        }
        // Mostly inlined (no helper-method calls) so the JIT sees one flat method for the hot path --
        // as cheap as a hand-unrolled version. processWithBacktracking, hasLineTerminator and
        // Replace.safeReplace (all rare) are left as real calls since each does enough work per
        // invocation that call overhead is negligible in comparison.
        byte[] b = str.bytes;
        int off = str.offset;
        int len = str.length;
        Idiom.PrefixPart[] parts = idiom.prefix();

        int pos = 0;
        for (int pi = 0; pi < parts.length; pi++) {
            Idiom.PrefixPart part = parts[pi];
            byte[] lit = part.literal();
            int litLen = lit.length;
            boolean present = pos + litLen <= len;
            if (present) {
                for (int k = 0; k < litLen; k++) {
                    if (b[off + pos + k] != lit[k]) {
                        present = false;
                        break;
                    }
                }
            }
            if (present) {
                pos += litLen;
            } else if (part.optional() == false) {
                return str; // required literal missing: no match, no combination can help.
            }
        }

        int hostStart = pos;
        byte delimiter = idiom.delimiter();
        int delimIdx = -1;
        for (int j = hostStart; j < len; j++) {
            if (b[off + j] == delimiter) {
                delimIdx = j;
                break;
            }
        }
        if (delimIdx <= hostStart) {
            // [^C]+ needs >=1 char; the greedy pass above consumed an optional segment right up to the
            // delimiter (rare) -- fall back to trying every prefix combination.
            return processWithBacktracking(str, idiom, parts, b, off, len);
        }

        if (idiom.tailAnchored() && idiom.tailDotAll() == false && hasLineTerminator(b, off, delimIdx + 1, len)) {
            // A line terminator anywhere in the remainder (even trailing) disqualifies the fast path --
            // see hasLineTerminator -- so defer to the real regex engine for this one row.
            return Replace.safeReplace(str, idiom.originalPattern(), idiom.originalNewStr());
        }

        byte[] pre = idiom.replacementPrefix();
        byte[] suf = idiom.replacementSuffix();
        int hostLen = delimIdx - hostStart;
        if (pre.length == 0 && suf.length == 0) {
            return new BytesRef(b, off + hostStart, hostLen);
        }
        byte[] out = new byte[pre.length + hostLen + suf.length];
        System.arraycopy(pre, 0, out, 0, pre.length);
        System.arraycopy(b, off + hostStart, out, pre.length, hostLen);
        System.arraycopy(suf, 0, out, pre.length + hostLen, suf.length);
        return new BytesRef(out);
    }

    /**
     * Rare fallback from {@link #process}: brute-forces all 2^n combinations of the optional prefix
     * segments, most-greedy-first (mask bits ordered MSB-to-LSB in encounter order), so the first
     * success found is the same one Java regex's backtracking would find.
     */
    private static BytesRef processWithBacktracking(BytesRef str, Idiom idiom, Idiom.PrefixPart[] parts, byte[] b, int off, int len) {
        int numOptional = 0;
        for (Idiom.PrefixPart part : parts) {
            if (part.optional()) {
                numOptional++;
            }
        }
        for (int mask = (1 << numOptional) - 1; mask >= 0; mask--) {
            int pos = 0;
            int bit = numOptional - 1;
            boolean ok = true;
            for (Idiom.PrefixPart part : parts) {
                byte[] lit = part.literal();
                if (part.optional()) {
                    boolean take = ((mask >> bit) & 1) != 0;
                    bit--;
                    if (take == false) {
                        continue; // regex backtrack: this '?' matched zero occurrences.
                    }
                }
                if (literalMatches(b, off, pos, len, lit) == false) {
                    ok = false;
                    break;
                }
                pos += lit.length;
            }
            if (ok == false) {
                continue;
            }
            int hostStart = pos;
            int delimIdx = findDelimiter(b, off, len, hostStart, idiom.delimiter());
            if (delimIdx > hostStart) {
                return finishCaptureMatch(str, idiom, b, off, len, hostStart, delimIdx);
            }
        }
        return str; // no combination matched: REPLACE returns the input unchanged (no regex match).
    }

    /**
     * Whether {@code lit} occurs at {@code b[off+pos, off+pos+lit.length)}. A manual loop rather than
     * {@link java.util.Arrays#equals(byte[], int, int, byte[], int, int)}: {@code lit} is always tiny (a
     * handful of bytes), so the call/setup overhead of the general-purpose (possibly-vectorized) library
     * method outweighs just comparing the bytes directly here.
     */
    private static boolean literalMatches(byte[] b, int off, int pos, int len, byte[] lit) {
        if (pos + lit.length > len) {
            return false;
        }
        for (int k = 0; k < lit.length; k++) {
            if (b[off + pos + k] != lit[k]) {
                return false;
            }
        }
        return true;
    }

    private static int findDelimiter(byte[] b, int off, int len, int from, byte delimiter) {
        for (int j = from; j < len; j++) {
            if (b[off + j] == delimiter) {
                return j;
            }
        }
        return -1;
    }

    private static BytesRef finishCaptureMatch(BytesRef str, Idiom idiom, byte[] b, int off, int len, int hostStart, int delimIdx) {
        if (idiom.tailAnchored() && idiom.tailDotAll() == false && hasLineTerminator(b, off, delimIdx + 1, len)) {
            return Replace.safeReplace(str, idiom.originalPattern(), idiom.originalNewStr());
        }
        return buildCaptureReplacement(idiom, b, off, hostStart, delimIdx);
    }

    private static BytesRef buildCaptureReplacement(Idiom idiom, byte[] b, int off, int hostStart, int hostEnd) {
        byte[] pre = idiom.replacementPrefix();
        byte[] suf = idiom.replacementSuffix();
        int hostLen = hostEnd - hostStart;
        if (pre.length == 0 && suf.length == 0) {
            return new BytesRef(b, off + hostStart, hostLen);
        }
        byte[] out = new byte[pre.length + hostLen + suf.length];
        System.arraycopy(pre, 0, out, 0, pre.length);
        System.arraycopy(b, off + hostStart, out, pre.length, hostLen);
        System.arraycopy(suf, 0, out, pre.length + hostLen, suf.length);
        return new BytesRef(out);
    }

    // Lead bytes of the UTF-8 encoding of every Pattern-default line terminator (\n, \r, and the lead
    // byte shared by U+0085's 2-byte and U+2028/U+2029's 3-byte encodings). A branch-free lookup keeps
    // hasLineTerminator's per-byte cost to one array read for the overwhelmingly common case
    // (real data has none of these bytes at all) instead of up to 4 sequential comparisons.
    private static final boolean[] MAYBE_LINE_TERMINATOR_LEAD_BYTE = new boolean[256];
    static {
        MAYBE_LINE_TERMINATOR_LEAD_BYTE['\n'] = true;
        MAYBE_LINE_TERMINATOR_LEAD_BYTE['\r'] = true;
        MAYBE_LINE_TERMINATOR_LEAD_BYTE[0xC2] = true; // U+0085 NEL
        MAYBE_LINE_TERMINATOR_LEAD_BYTE[0xE2] = true; // U+2028 / U+2029
    }

    // SWAR ("SIMD within a register") constants for hasLineTerminator's word-at-a-time scan:
    // XOR-ing a word against one of these broadcast-a-byte-to-all-8-lanes patterns turns every lane
    // holding that target byte into 0x00, and the classic "has a zero byte" trick then detects it --
    // same technique already used by SimdJsonDirectWalker.resolveFieldName for quote/backslash scanning.
    private static final long SWAR_LO = 0x0101010101010101L;
    private static final long SWAR_HI = 0x8080808080808080L;
    private static final long SWAR_NL = 0x0A0A0A0A0A0A0A0AL; // '\n'
    private static final long SWAR_CR = 0x0D0D0D0D0D0D0D0DL; // '\r'
    private static final long SWAR_C2 = 0xC2C2C2C2C2C2C2C2L; // U+0085 NEL lead byte
    private static final long SWAR_E2 = 0xE2E2E2E2E2E2E2E2L; // U+2028 / U+2029 lead byte

    /**
     * Whether {@code b[off+from, off+len)} contains a default-mode {@link Pattern} line terminator
     * (matching Pattern's own set: {@code \n}, {@code \r}, {@code \u0085}, {@code \u2028}, {@code \u2029}),
     * anywhere -- including as the very last character(s).
     * <p>
     * A terminator anywhere disqualifies the byte-scan fast path here, even a trailing one: {@code $}
     * (without {@link Pattern#MULTILINE}) matches just <em>before</em> a trailing terminator, so the
     * terminator itself falls outside the regex match and {@code replaceAll} leaves it untouched in the
     * output -- but this fast path's replacement-building always treats {@code .*$} as consuming through
     * the true end of input. Reproducing "matched everything except a possibly-multi-byte trailing
     * terminator" correctly isn't worth the complexity for a case real URLs/log fields essentially never
     * hit; deferring to the real regex engine is simpler and still correct.
     * <p>
     * Scans 8 bytes at a time via {@link BitUtil#VH_LE_LONG} + SWAR instead of
     * {@link #MAYBE_LINE_TERMINATOR_LEAD_BYTE}'s one-array-read-per-byte check, since real data
     * essentially never contains any of these bytes -- the per-byte lookup only runs for the rare word
     * that flags a candidate, or the final &lt;8-byte remainder.
     */
    private static boolean hasLineTerminator(byte[] b, int off, int from, int len) {
        int j = from;
        int wordLimit = len - 8; // last offset (relative to off) at which a full 8-byte word fits
        for (; j <= wordLimit; j += 8) {
            long word = (long) BitUtil.VH_LE_LONG.get(b, off + j);
            long xnl = word ^ SWAR_NL;
            long xcr = word ^ SWAR_CR;
            long xc2 = word ^ SWAR_C2;
            long xe2 = word ^ SWAR_E2;
            long hit = ((xnl - SWAR_LO) & ~xnl) | ((xcr - SWAR_LO) & ~xcr) | ((xc2 - SWAR_LO) & ~xc2) | ((xe2 - SWAR_LO) & ~xe2);
            if ((hit & SWAR_HI) == 0) {
                continue; // none of these 8 bytes can start a line terminator.
            }
            for (int k = 0; k < 8; k++) {
                if (MAYBE_LINE_TERMINATOR_LEAD_BYTE[b[off + j + k] & 0xFF] && lineTerminatorLengthAt(b, off, j + k, len) > 0) {
                    return true;
                }
            }
        }
        for (; j < len; j++) {
            if (MAYBE_LINE_TERMINATOR_LEAD_BYTE[b[off + j] & 0xFF] && lineTerminatorLengthAt(b, off, j, len) > 0) {
                return true;
            }
        }
        return false;
    }

    /** Length in bytes of a line terminator (see {@link #hasLineTerminator}) at index {@code j}, or 0. */
    private static int lineTerminatorLengthAt(byte[] b, int off, int j, int len) {
        byte c = b[off + j];
        if (c == '\n' || c == '\r') {
            return 1;
        }
        if (c == (byte) 0xC2 && j + 1 < len && b[off + j + 1] == (byte) 0x85) {
            return 2; // U+0085 NEL
        }
        if (c == (byte) 0xE2
            && j + 2 < len
            && b[off + j + 1] == (byte) 0x80
            && (b[off + j + 2] == (byte) 0xA8 || b[off + j + 2] == (byte) 0xA9)) {
            return 3; // U+2028 / U+2029
        }
        return 0;
    }
}
