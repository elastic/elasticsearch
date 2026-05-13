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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

public class Replace extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Replace", Replace::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Replace.class).ternary(Replace::new).name("replace");
    private static final TransportVersion ESQL_SERIALIZE_SOURCE_FUNCTIONS_WARNINGS = TransportVersion.fromName(
        "esql_serialize_source_functions_warnings"
    );

    private final Expression str;
    private final Expression regex;
    private final Expression newStr;

    @FunctionInfo(
        returnType = "keyword",
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

        // Top-level alternation invalidates anchoring: `^a|b` is `(^a)|(b)`, so an input starting with `b`
        // would still match. Java regex hex / unicode / control / octal escapes always produce literal
        // characters (they cannot encode a meta `|`), so a simple literal-`|` scan outside `\Q...\E` is enough.
        if (containsTopLevelAlternation(regex, i)) {
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
     * starting at {@code from}. This is a deliberately coarse check: any {@code |} (including those nested
     * in a group) disqualifies the pattern, which keeps the analysis simple at the cost of giving up on
     * patterns like {@code ^a(b|c)} where a prefix could still be extracted.
     * <p>
     * Hex / unicode / control / octal escapes in Java regex always produce a literal character (never an
     * unescaped meta), so they cannot smuggle in a hidden top-level alternation.
     */
    private static boolean containsTopLevelAlternation(String regex, int from) {
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
     * Executes a Replace without surpassing the memory limit.
     */
    private static BytesRef safeReplace(BytesRef strBytesRef, Pattern regex, BytesRef newStrBytesRef) {
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
            } catch (PatternSyntaxException pse) {
                // TODO this is not right (inconsistent). See also https://github.com/elastic/elasticsearch/issues/100038
                // this should generate a header warning and return null (as do the rest of this functionality in evaluators),
                // but for the moment we let the exception through
                throw pse;
            }
            byte[] literalPrefix = extractLiteralPrefix(regexPattern);
            if (newStr.foldable() && newStr.dataType() == DataType.KEYWORD) {
                // Both regex and newStr are constants: use the dictionary-aware evaluator that applies
                // REPLACE once per dictionary entry on OrdinalBytesRefBlock inputs.
                BytesRef constantNewStr = BytesRefs.toBytesRef(newStr.fold(toEvaluator.foldCtx()));
                if (constantNewStr != null) {
                    return new ReplaceConstantOrdinalEvaluator.Factory(source(), strEval, regexPattern, literalPrefix, constantNewStr);
                }
            }
            return new ReplaceConstantEvaluator.Factory(source(), strEval, regexPattern, literalPrefix, newStrEval);
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
