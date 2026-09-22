/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

public class Replace extends EsqlScalarFunction implements AnyNullIsNull {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Replace", Replace::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Replace.class).ternary(Replace::new).name("replace");
    private static final TransportVersion ESQL_SERIALIZE_SOURCE_FUNCTIONS_WARNINGS = TransportVersion.fromName(
        "esql_serialize_source_functions_warnings"
    );

    private final Expression str;
    private final Expression regex;
    private final Expression newStr;

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA) },
        returnType = "keyword",
        briefSummary = "Replaces regular expression matches in a string with a replacement string.",
        description = """
            The function substitutes in the string `str` any match of the regular expression `regex`
            with the replacement string `newStr`.""",
        examples = {
            @Example(
                file = "string",
                tag = "replaceString",
                description = "This example replaces any occurrence of the word \"World\" with the word \"Universe\":"
            ),
            @Example(file = "string", tag = "replaceRegex", description = "This example removes all spaces:") }
    )
    public Replace(
        Source source,
        @Param(name = "string", type = { "keyword", "text" }, description = "String expression.") Expression str,
        @Param(name = "regex", type = { "keyword", "text" }, description = "Regular expression.") Expression regex,
        @Param(name = "newString", type = { "keyword", "text" }, description = "Replacement string.") Expression newStr
    ) {
        super(source, Arrays.asList(str, regex, newStr));
        this.str = str;
        this.regex = regex;
        this.newStr = newStr;
    }

    private Replace(StreamInput in) throws IOException {
        this(
            in.getTransportVersion().supports(ESQL_SERIALIZE_SOURCE_FUNCTIONS_WARNINGS)
                ? Source.readFrom((PlanStreamInput) in)
                : Source.EMPTY,
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(ESQL_SERIALIZE_SOURCE_FUNCTIONS_WARNINGS)) {
            source().writeTo(out);
        }
        out.writeNamedWriteable(str);
        out.writeNamedWriteable(regex);
        out.writeNamedWriteable(newStr);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isString(str, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isString(regex, sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }

        return isString(newStr, sourceText(), THIRD);
    }

    @Override
    public boolean foldable() {
        return str.foldable() && regex.foldable() && newStr.foldable();
    }

    /**
     * Empty literal-prefix sentinel: when the regex has no extractable anchored literal prefix,
     * the constant evaluator is built with this array and the byte-level fast-path check is skipped.
     */
    static final byte[] NO_LITERAL_PREFIX = new byte[0];

    @Evaluator(extraName = "Constant", warnExceptions = IllegalArgumentException.class)
    static BytesRef process(BytesRef str, @Fixed Pattern regex, @Fixed(includeInToString = false) byte[] literalPrefix, BytesRef newStr) {
        if (str == null || regex == null || newStr == null) {
            return null;
        }
        if (literalPrefix.length > 0 && startsWith(str, literalPrefix) == false) {
            return str;
        }
        return safeReplace(str, regex, newStr);
    }

    @Evaluator(warnExceptions = IllegalArgumentException.class)
    static BytesRef process(BytesRef str, BytesRef regex, BytesRef newStr) {
        if (str == null) {
            return null;
        }
        if (regex == null || newStr == null) {
            return str;
        }
        return safeReplace(str, Pattern.compile(regex.utf8ToString()), newStr);
    }

    /**
     * Byte-level prefix check on a {@link BytesRef}. Cheaper than {@link BytesRef#utf8ToString()}
     * + {@link String#startsWith(String)} because it avoids the UTF-8 to UTF-16 conversion and
     * the {@link String} allocation. Safe for UTF-8 because the prefix bytes are themselves a
     * complete UTF-8 sequence (produced by {@link String#getBytes(java.nio.charset.Charset)}).
     */
    static boolean startsWith(BytesRef ref, byte[] prefix) {
        if (ref.length < prefix.length) {
            return false;
        }
        return Arrays.equals(ref.bytes, ref.offset, ref.offset + prefix.length, prefix, 0, prefix.length);
    }

    /**
     * Extract a literal UTF-8 byte prefix from a constant regex pattern, when safe to do so.
     * <p>
     * Returns {@link #NO_LITERAL_PREFIX} unless the pattern is anchored at input start and begins
     * with at least one literal character whose match position can be statically determined.
     * Conservative by design: any construct that could shift the effective anchor (multiline mode,
     * top-level alternation, look-arounds, etc.) results in no prefix. When a prefix is returned,
     * any input that does not start with these bytes is guaranteed not to match the pattern, so the
     * caller can short-circuit the row without invoking the regex engine.
     * <p>
     * Handled constructs:
     * <ul>
     *   <li>Anchors {@code ^} and {@code \A} at the start of the pattern.</li>
     *   <li>Plain literal characters (ASCII and non-ASCII).</li>
     *   <li>Escaped meta characters (e.g. {@code \.}, {@code \?}).</li>
     *   <li>{@code \Q...\E} literal sections.</li>
     * </ul>
     * Walk semantics for quantifiers attached to a preceding literal:
     * <ul>
     *   <li>{@code ?}, {@code *}, {@code {n,m}}: drop the preceding literal (it could be absent) and stop.</li>
     *   <li>{@code +}: keep the preceding literal but stop the walk (anything after it sits at an unknown offset).</li>
     * </ul>
     * Bails on top-level alternation, groups, character classes, anchors elsewhere, inline flags, and any other meta construct.
     */
    static byte[] extractLiteralPrefix(Pattern pattern) {
        // Patterns compiled with these flags can match `^` after newlines, treat the input
        // case-insensitively / loosely, or change which characters are literal in the pattern source,
        // any of which would invalidate a byte-level prefix check on the raw input.
        // - MULTILINE / inline (?m): `^` matches after newlines, not just input start.
        // - CASE_INSENSITIVE / UNICODE_CASE / inline (?i): the prefix bytes need not equal the input bytes.
        // - COMMENTS / inline (?x): whitespace and `#…` in the pattern are ignored, so our literal walk would
        // include characters that the engine treats as comments / whitespace.
        // - CANON_EQ: two distinct UTF-8 byte sequences can be considered equivalent by the matcher.
        // - LITERAL: pattern source is treated as a literal string, not a regex (so leading `^` is not an anchor).
        int flags = pattern.flags();
        int disqualifying = Pattern.MULTILINE | Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.COMMENTS | Pattern.CANON_EQ
            | Pattern.LITERAL;
        if ((flags & disqualifying) != 0) {
            return NO_LITERAL_PREFIX;
        }
        String regex = pattern.pattern();
        int n = regex.length();
        if (n == 0) {
            return NO_LITERAL_PREFIX;
        }

        int i = 0;
        if (regex.charAt(i) == '^') {
            i++;
        } else if (regex.startsWith("\\A", i)) {
            i += 2;
        } else {
            return NO_LITERAL_PREFIX;
        }

        // Any alternation invalidates anchoring: `^a|b` is `(^a)|(b)`, so an input starting with `b` would
        // still match — and even `^foo(a|b)` is rejected by this coarse check, see below. Java regex hex /
        // unicode / control / octal escapes always produce literal characters (they cannot encode a meta
        // `|`), so a simple literal-`|` scan outside `\Q...\E` is enough.
        if (containsUnquotedAlternation(regex, i)) {
            return NO_LITERAL_PREFIX;
        }

        StringBuilder prefix = new StringBuilder();
        // Track the last position in `prefix` where a single literal code point was appended,
        // so a following `?`, `*` or `{...}` quantifier can drop it.
        int lastLiteralStart = -1;

        while (i < n) {
            char c = regex.charAt(i);

            // \Q...\E quoted literal section.
            if (c == '\\' && i + 1 < n && regex.charAt(i + 1) == 'Q') {
                int end = regex.indexOf("\\E", i + 2);
                int quoteEnd = (end < 0) ? n : end;
                // The whole \Q...\E is one literal chunk; treat the last code point inside it
                // as the "last literal" so a following quantifier (after \E) can drop just that char.
                if (quoteEnd > i + 2) {
                    int chunkStart = prefix.length();
                    prefix.append(regex, i + 2, quoteEnd);
                    // Set lastLiteralStart to the last code point of the chunk.
                    lastLiteralStart = prefix.offsetByCodePoints(prefix.length(), -1);
                    if (lastLiteralStart < chunkStart) {
                        lastLiteralStart = chunkStart;
                    }
                }
                if (end < 0) {
                    // Unterminated \Q…; rest of the pattern is literal, we are done.
                    break;
                }
                i = end + 2;
                continue;
            }

            if (c == '\\') {
                if (i + 1 >= n) {
                    // Trailing backslash — malformed; stop conservatively.
                    break;
                }
                char next = regex.charAt(i + 1);
                if (isEscapedLiteral(next)) {
                    lastLiteralStart = prefix.length();
                    prefix.append(next);
                    i += 2;
                    continue;
                }
                // Backreferences (\1), character classes (\w, \d, \s, \b, \B, \A, \z, \Z), etc. — bail.
                break;
            }

            // Quantifiers attaching to the last literal.
            if (c == '?' || c == '*') {
                if (lastLiteralStart < 0) {
                    break;
                }
                prefix.setLength(lastLiteralStart);
                break;
            }
            if (c == '{') {
                // `{n,m}` — `{0,…}` makes the prior char optional; we conservatively drop it.
                if (lastLiteralStart < 0) {
                    break;
                }
                prefix.setLength(lastLiteralStart);
                break;
            }
            if (c == '+') {
                // One-or-more keeps the preceding literal at position `lastLiteralStart`, but anything
                // after the quantifier sits at an unknown offset, so we stop the walk here without dropping.
                break;
            }

            // Any other meta character terminates the literal prefix.
            // This includes: `.` `(` `[` `|` `$` `)` `]` and the unused-here `^`.
            if (isRegexMeta(c)) {
                break;
            }

            // Plain literal code point (possibly a UTF-16 surrogate pair).
            lastLiteralStart = prefix.length();
            if (Character.isHighSurrogate(c) && i + 1 < n && Character.isLowSurrogate(regex.charAt(i + 1))) {
                prefix.append(c);
                prefix.append(regex.charAt(i + 1));
                i += 2;
            } else {
                prefix.append(c);
                i++;
            }
        }

        if (prefix.isEmpty()) {
            return NO_LITERAL_PREFIX;
        }
        return prefix.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static boolean isEscapedLiteral(char c) {
        // Characters that, when preceded by `\`, denote themselves as a literal in Java regex syntax.
        // We deliberately exclude letters/digits because those introduce special meaning
        // (\d, \w, \s, \b, \A, \z, \Z, \n, \t, \r, \1, etc.).
        return switch (c) {
            case '.', '\\', '/', '(', ')', '[', ']', '{', '}', '*', '+', '?', '|', '^', '$', '-', '"', '\'', '#', ' ', ':', '=', '!', ',',
                '@', '&', '~', '`', '<', '>', '%' -> true;
            default -> false;
        };
    }

    private static boolean isRegexMeta(char c) {
        return switch (c) {
            case '.', '(', ')', '[', ']', '{', '}', '|', '$', '^', '?', '*', '+', '\\' -> true;
            default -> false;
        };
    }

    /**
     * Returns {@code true} if a literal {@code |} appears outside a {@code \Q...\E} block in the pattern
     * starting at {@code from}. Despite the term "alternation" usually referring to a top-level construct,
     * this check is deliberately coarse: any {@code |} (including those nested in a group) disqualifies
     * the pattern. It keeps the analysis simple at the cost of giving up on patterns like {@code ^a(b|c)}
     * where a prefix could still be extracted.
     * <p>
     * Hex / unicode / control / octal escapes in Java regex always produce a literal character (never an
     * unescaped meta), so they cannot smuggle in a hidden alternation.
     */
    private static boolean containsUnquotedAlternation(String regex, int from) {
        int n = regex.length();
        for (int i = from; i < n; i++) {
            char c = regex.charAt(i);
            if (c == '\\') {
                if (i + 1 >= n) {
                    break;
                }
                if (regex.charAt(i + 1) == 'Q') {
                    int end = regex.indexOf("\\E", i + 2);
                    i = (end < 0) ? n - 1 : end + 1;
                } else {
                    i++;
                }
                continue;
            }
            if (c == '|') {
                return true;
            }
        }
        return false;
    }

    /**
     * Compiled fast-path plan for regexes of the shape
     * {@code ^prefix(?opt-prefix)*([^C]+)C.*$} with replacement {@code litPrefix$1litSuffix} -- e.g.
     * "extract the substring up to the first occurrence of a delimiter, after skipping a fixed
     * (possibly optional) literal prefix". ClickBench's {@code REPLACE(Referer,
     * "^https?://(?:www\.)?([^/]+)/.*$", "$1")} (extract URL host) is the motivating case.
     * <p>
     * Detected by {@link #extractCaptureUntilDelimiterIdiom} and executed by
     * {@link #processCaptureUntilDelimiter}, a hand-written byte scan over the raw UTF-8 bytes that
     * never converts to {@link String}, counts codepoints, or invokes the regex engine on the common
     * path -- see that method for why this matters. {@link #originalPattern}/{@link #originalNewStr}
     * are kept only for the rare per-row fallback documented there.
     */
    record CaptureUntilDelimiterIdiom(
        // A plain array, not a List: avoids Iterator allocation/indirection in the per-row hot loop in
        // processCaptureUntilDelimiter -- a for-each over an array compiles to a simple indexed loop.
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

    // Bounds the per-row backtrack search (2^n combinations) in processCaptureUntilDelimiter to a
    // handful of attempts; real-world prefixes (e.g. "s?", "(?:www\.)?") have at most 1-2.
    private static final int MAX_OPTIONAL_PREFIX_PARTS = 4;

    /**
     * Detects the {@link CaptureUntilDelimiterIdiom} shape in a constant {@code regex}/{@code newStr}
     * pair, or returns {@code null} if it doesn't fit. Conservative by design, mirroring
     * {@link #extractLiteralPrefix}: bails on anything that would make the byte-scan's behavior diverge
     * from the actual regex engine, rather than trying to approximate it.
     */
    static CaptureUntilDelimiterIdiom extractCaptureUntilDelimiterIdiom(Pattern pattern, BytesRef newStrRef) {
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
        if (containsUnquotedAlternation(regex, i)) {
            return null;
        }

        List<CaptureUntilDelimiterIdiom.PrefixPart> parts = new ArrayList<>();
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
                    parts.add(new CaptureUntilDelimiterIdiom.PrefixPart(run.toString().getBytes(StandardCharsets.UTF_8), false));
                    run.setLength(0);
                    lastAtomStart = -1;
                }
                i = close + 1;
                boolean optional = i < n && regex.charAt(i) == '?';
                if (optional) {
                    i++;
                }
                parts.add(new CaptureUntilDelimiterIdiom.PrefixPart(body, optional));
                continue;
            }

            // Our target capturing group -- flush the pending literal run and hand off.
            if (c == '(') {
                if (run.length() > 0) {
                    parts.add(new CaptureUntilDelimiterIdiom.PrefixPart(run.toString().getBytes(StandardCharsets.UTF_8), false));
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
                if (i + 1 >= n || isEscapedLiteral(regex.charAt(i + 1)) == false) {
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
                    parts.add(
                        new CaptureUntilDelimiterIdiom.PrefixPart(run.substring(0, lastAtomStart).getBytes(StandardCharsets.UTF_8), false)
                    );
                }
                parts.add(new CaptureUntilDelimiterIdiom.PrefixPart(run.substring(lastAtomStart).getBytes(StandardCharsets.UTF_8), true));
                run.setLength(0);
                lastAtomStart = -1;
                i++;
                continue;
            }

            // Repeat quantifiers and any other meta construct: not modeled by this idiom, bail.
            if (c == '*' || c == '{' || c == '+' || isRegexMeta(c)) {
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
                if (i + 1 >= to || isEscapedLiteral(regex.charAt(i + 1)) == false) {
                    return null;
                }
                out.append(regex.charAt(i + 1));
                i += 2;
                continue;
            }
            if (isRegexMeta(c)) {
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
     * a {@code prefix$1suffix}-shaped {@code newStrRef}, to complete a {@link CaptureUntilDelimiterIdiom}.
     * Returns {@code null} if anything doesn't match this exact shape.
     */
    private static CaptureUntilDelimiterIdiom finishCaptureGroup(
        Pattern pattern,
        String regex,
        int i,
        int n,
        List<CaptureUntilDelimiterIdiom.PrefixPart> parts,
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
            if (i + 1 >= n || isEscapedLiteral(regex.charAt(i + 1)) == false) {
                return null;
            }
            delimiterCp = regex.charAt(i + 1);
            i += 2;
        } else if (i < n && isRegexMeta(regex.charAt(i)) == false) {
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
            if (i + 1 >= n || isEscapedLiteral(regex.charAt(i + 1)) == false) {
                return null;
            }
            litCp = regex.charAt(i + 1);
            afterLit = i + 2;
        } else if (i < n && isRegexMeta(regex.charAt(i)) == false) {
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
        for (CaptureUntilDelimiterIdiom.PrefixPart p : parts) {
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

        return new CaptureUntilDelimiterIdiom(
            parts.toArray(new CaptureUntilDelimiterIdiom.PrefixPart[0]),
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
     * Hand-written byte scan equivalent to {@link #process(BytesRef, Pattern, byte[], BytesRef)} for
     * regexes matching {@link CaptureUntilDelimiterIdiom}'s shape. Unlike the regex engine, this never
     * converts {@code str} to a {@link String} or counts codepoints -- it walks {@code str}'s raw UTF-8
     * bytes directly, which is safe because every literal compared against is itself a complete UTF-8
     * byte sequence (same reasoning as {@link #startsWith}).
     * <p>
     * The common case is a single deterministic, greedy pass over the prefix segments -- peeking at each
     * optional segment's bytes and consuming it only if actually present, exactly like Java regex's
     * first (most-greedy) attempt at a {@code ?} quantifier -- so it costs no more than a hand-unrolled
     * check per segment, with no combinatorial branching. Only if that greedy pass leaves an empty
     * capture (an optional segment happened to consume right up to the delimiter, e.g. {@code "www./x"}
     * against {@code (?:www\.)?([^/]+)/}) does {@link #processCaptureUntilDelimiterWithBacktracking} take
     * over, brute-forcing all 2^n combinations of the optional segments (bounded by
     * {@link #MAX_OPTIONAL_PREFIX_PARTS}), most-greedy-first, to reproduce Java regex's backtracking
     * order.
     * <p>
     * {@code tailAnchored} patterns (i.e. ending in {@code $}, not just a bare trailing {@code .*}) rely
     * on {@code .*$} always matching the remainder -- true unless the remainder contains a line
     * terminator that isn't the very last character (default, non-{@link Pattern#DOTALL} semantics). That
     * case is rare (URLs/log fields essentially never embed raw newlines) but is checked per-row via
     * {@link #hasNonTrailingLineTerminator}; on the rare hit, this defers to the real regex engine via
     * {@code idiom.originalPattern()}/{@code idiom.originalNewStr()} rather than risk an incorrect result.
     */
    @Evaluator(extraName = "CaptureUntilDelimiter", warnExceptions = IllegalArgumentException.class)
    static BytesRef processCaptureUntilDelimiter(BytesRef str, @Fixed(includeInToString = false) CaptureUntilDelimiterIdiom idiom) {
        if (str == null) {
            return null;
        }
        // Fully inlined (no helper-method calls) so the JIT sees exactly one flat method for the hot
        // path -- as cheap as a hand-unrolled version. processCaptureUntilDelimiterWithBacktracking and
        // safeReplace (both rare) duplicate a little of this logic themselves rather than factoring it
        // out, for the same reason.
        byte[] b = str.bytes;
        int off = str.offset;
        int len = str.length;
        CaptureUntilDelimiterIdiom.PrefixPart[] parts = idiom.prefix();

        int pos = 0;
        for (int pi = 0; pi < parts.length; pi++) {
            CaptureUntilDelimiterIdiom.PrefixPart part = parts[pi];
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
            return processCaptureUntilDelimiterWithBacktracking(str, idiom, parts, b, off, len);
        }

        if (idiom.tailAnchored() && idiom.tailDotAll() == false) {
            for (int j = delimIdx + 1; j < len; j++) {
                if (MAYBE_LINE_TERMINATOR_LEAD_BYTE[b[off + j] & 0xFF] == false) {
                    continue;
                }
                int termLen = lineTerminatorLengthAt(b, off, j, len);
                if (termLen > 0 && j + termLen != len) {
                    // `.*$` (no DOTALL) can't cross this non-trailing terminator -- defer to the real
                    // regex engine for this one row rather than risk an incorrect result.
                    return safeReplace(str, idiom.originalPattern(), idiom.originalNewStr());
                }
            }
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
     * Rare fallback from {@link #processCaptureUntilDelimiter}: brute-forces all 2^n combinations of the
     * optional prefix segments, most-greedy-first (mask bits ordered MSB-to-LSB in encounter order), so
     * the first success found is the same one Java regex's backtracking would find.
     */
    private static BytesRef processCaptureUntilDelimiterWithBacktracking(
        BytesRef str,
        CaptureUntilDelimiterIdiom idiom,
        CaptureUntilDelimiterIdiom.PrefixPart[] parts,
        byte[] b,
        int off,
        int len
    ) {
        int numOptional = 0;
        for (CaptureUntilDelimiterIdiom.PrefixPart part : parts) {
            if (part.optional()) {
                numOptional++;
            }
        }
        for (int mask = (1 << numOptional) - 1; mask >= 0; mask--) {
            int pos = 0;
            int bit = numOptional - 1;
            boolean ok = true;
            for (CaptureUntilDelimiterIdiom.PrefixPart part : parts) {
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
     * {@link Arrays#equals(byte[], int, int, byte[], int, int)}: {@code lit} is always tiny (a handful of
     * bytes), so the call/setup overhead of the general-purpose (possibly-vectorized) library method
     * outweighs just comparing the bytes directly here.
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

    private static BytesRef finishCaptureMatch(
        BytesRef str,
        CaptureUntilDelimiterIdiom idiom,
        byte[] b,
        int off,
        int len,
        int hostStart,
        int delimIdx
    ) {
        if (idiom.tailAnchored() && idiom.tailDotAll() == false && hasNonTrailingLineTerminator(b, off, delimIdx + 1, len)) {
            return safeReplace(str, idiom.originalPattern(), idiom.originalNewStr());
        }
        return buildCaptureReplacement(idiom, b, off, hostStart, delimIdx);
    }

    private static BytesRef buildCaptureReplacement(CaptureUntilDelimiterIdiom idiom, byte[] b, int off, int hostStart, int hostEnd) {
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
    // hasNonTrailingLineTerminator's per-byte cost to one array read for the overwhelmingly common case
    // (real data has none of these bytes at all) instead of up to 4 sequential comparisons.
    private static final boolean[] MAYBE_LINE_TERMINATOR_LEAD_BYTE = new boolean[256];
    static {
        MAYBE_LINE_TERMINATOR_LEAD_BYTE['\n'] = true;
        MAYBE_LINE_TERMINATOR_LEAD_BYTE['\r'] = true;
        MAYBE_LINE_TERMINATOR_LEAD_BYTE[0xC2] = true; // U+0085 NEL
        MAYBE_LINE_TERMINATOR_LEAD_BYTE[0xE2] = true; // U+2028 / U+2029
    }

    /**
     * Whether {@code b[off+from, off+len)} contains a default-mode {@link Pattern} line terminator
     * (matching Pattern's own set: {@code \n}, {@code \r}, {@code \u0085}, {@code \u2028}, {@code \u2029})
     * that is not exactly the last character -- the case where {@code .*$} (without {@link
     * Pattern#DOTALL}) can fail to match the remainder.
     */
    private static boolean hasNonTrailingLineTerminator(byte[] b, int off, int from, int len) {
        for (int j = from; j < len; j++) {
            if (MAYBE_LINE_TERMINATOR_LEAD_BYTE[b[off + j] & 0xFF] == false) {
                continue;
            }
            int termLen = lineTerminatorLengthAt(b, off, j, len);
            if (termLen > 0 && j + termLen != len) {
                return true;
            }
        }
        return false;
    }

    /** Length in bytes of a line terminator (see {@link #hasNonTrailingLineTerminator}) at index {@code j}, or 0. */
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

    /**
     * Executes a Replace without surpassing the memory limit.
     */
    private static BytesRef safeReplace(BytesRef strBytesRef, Pattern regex, BytesRef newStrBytesRef) {
        try {
            return doReplace(strBytesRef, regex, newStrBytesRef);
        } catch (StackOverflowError e) {
            throw new IllegalArgumentException("Pattern nesting is too deep to evaluate", e);
        }
    }

    private static BytesRef doReplace(BytesRef strBytesRef, Pattern regex, BytesRef newStrBytesRef) {
        String str = strBytesRef.utf8ToString();
        Matcher m = regex.matcher(str);
        if (false == m.find()) {
            return strBytesRef;
        }
        String newStr = newStrBytesRef.utf8ToString();

        // Count potential groups (E.g. "$1") used in the replacement
        int constantReplacementLength = newStr.length();
        int groupsInReplacement = 0;
        for (int i = 0; i < newStr.length(); i++) {
            if (newStr.charAt(i) == '$') {
                groupsInReplacement++;
                constantReplacementLength -= 2;
                i++;
            }
        }

        // Initialize the buffer with an approximate size for the first replacement
        StringBuilder result = new StringBuilder(str.length() + newStr.length() + 8);
        do {
            int matchSize = m.end() - m.start();
            int potentialReplacementSize = constantReplacementLength + groupsInReplacement * matchSize;
            int remainingStr = str.length() - m.end();
            if (result.length() + potentialReplacementSize + remainingStr > MAX_BYTES_REF_RESULT_SIZE) {
                throw new IllegalArgumentException(
                    "Creating strings with more than [" + MAX_BYTES_REF_RESULT_SIZE + "] bytes is not supported"
                );
            }

            m.appendReplacement(result, newStr);
        } while (m.find());
        m.appendTail(result);
        return new BytesRef(result.toString());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Replace(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Replace::new, str, regex, newStr);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var strEval = toEvaluator.apply(str);
        var newStrEval = toEvaluator.apply(newStr);

        if (regex.foldable() && regex.dataType() == DataType.KEYWORD) {
            Pattern regexPattern;
            try {
                regexPattern = Pattern.compile(BytesRefs.toString(regex.fold(toEvaluator.foldCtx())));
            } catch (PatternSyntaxException | StackOverflowError e) {
                // warnExceptions only wraps process(), so throwing here would fail the query.
                // Fall through to the per-row evaluator, which turns these into a warning and null.
                regexPattern = null;
            }
            if (regexPattern != null) {
                byte[] literalPrefix = extractLiteralPrefix(regexPattern);
                if (newStr.foldable() && newStr.dataType() == DataType.KEYWORD) {
                    // Both regex and newStr are constants: use the dictionary-aware evaluator that applies
                    // REPLACE once per dictionary entry on OrdinalBytesRefBlock inputs.
                    BytesRef constantNewStr = BytesRefs.toBytesRef(newStr.fold(toEvaluator.foldCtx()));
                    if (constantNewStr != null) {
                        CaptureUntilDelimiterIdiom idiom = extractCaptureUntilDelimiterIdiom(regexPattern, constantNewStr);
                        if (idiom != null) {
                            // Idiom-matched: a hand-written byte scan replaces the regex engine entirely (no
                            // UTF-8 decode, no codepoint counting) -- see extractCaptureUntilDelimiterIdiom.
                            return new ReplaceCaptureUntilDelimiterEvaluator.Factory(source(), strEval, idiom);
                        }
                        return new ReplaceConstantOrdinalEvaluator.Factory(source(), strEval, regexPattern, literalPrefix, constantNewStr);
                    }
                }
                return new ReplaceConstantEvaluator.Factory(source(), strEval, regexPattern, literalPrefix, newStrEval);
            }
        }

        var regexEval = toEvaluator.apply(regex);
        return new ReplaceEvaluator.Factory(source(), strEval, regexEval, newStrEval);
    }

    Expression str() {
        return str;
    }

    Expression regex() {
        return regex;
    }

    Expression newStr() {
        return newStr;
    }
}
