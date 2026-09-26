/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Internal, non-user-callable scalar implementing the value derivation for PromQL {@code label_replace}. For each series it
 * matches the (fully anchored) {@code regex} against the {@code src} label value and, on a match, expands {@code replacement}
 * using Go's {@code Expand} semantics ({@code $1}, {@code ${1}}, {@code $name}, {@code ${name}}). RE2/J is used for
 * byte-for-byte Prometheus parity (RE2 syntax, {@code (?P<name>)}, no backreferences).
 * <p>
 * The result encodes the three Prometheus outcomes so the caller can materialize the destination label from them:
 * <ul>
 *     <li><b>set</b> - a non-empty expansion becomes the destination label value;</li>
 *     <li><b>delete</b> - a matched-but-empty expansion is returned as the empty string, removing the label;</li>
 *     <li><b>no-op</b> - on no match the position is {@code null}, leaving any existing destination label untouched
 *         (Prometheus never sets it to {@code src}).</li>
 * </ul>
 * The caller is responsible for materializing an absent {@code src} as the empty string (via {@code COALESCE(src, "")} during
 * translation): the evaluator short-circuits all-null positions to {@code null}, so a genuinely null {@code src} would otherwise
 * become a spurious no-op instead of matching against {@code ""}.
 * <p>
 * If the label is multivalue - the function would raise a {@code "single-value function encountered multi-value"} warning and emit
 * {@code null} (a no-op) for that row.
 */
public final class RegexExpand extends EsqlScalarFunction implements VersionedNamedWriteable {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "RegexExpand",
        RegexExpand::new
    );

    /**
     * Transport version gating the PromQL label-function internal scalar {@link RegexExpand}.
     */
    public static final TransportVersion PROMQL_LABEL_FUNCTIONS = TransportVersion.fromName("promql_label_functions");

    private final Expression src;
    private final Expression regex;
    private final Expression replacement;

    public RegexExpand(Source source, Expression src, Expression regex, Expression replacement) {
        super(source, List.of(src, regex, replacement));
        this.src = src;
        this.regex = regex;
        this.replacement = replacement;
    }

    private RegexExpand(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(src);
        out.writeNamedWriteable(regex);
        out.writeNamedWriteable(replacement);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return PROMQL_LABEL_FUNCTIONS;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    /**
     * {@code null} is a first-class output of this function, not a consequence of nullable inputs: a no-match position is
     * emitted as {@code null} so that the enclosing {@code COALESCE} can leave the destination label untouched, matching
     * Prometheus {@code label_replace}. It is therefore nullable regardless of whether its children are, and callers must
     * not treat its result as non-null.
     */
    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isString(src, sourceText(), FIRST).and(isString(regex, sourceText(), SECOND))
            .and(isString(replacement, sourceText(), THIRD));
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new RegexExpand(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, RegexExpand::new, src, regex, replacement);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (regex.foldable() == false || replacement.foldable() == false) {
            throw new IllegalArgumentException("[regex] and [replacement] must be constants");
        }
        Pattern pattern = compilePattern(BytesRefs.toString(regex.fold(toEvaluator.foldCtx())));
        // Bind the replacement template to the pattern's capture-group metadata once, holding it as UTF-8 bytes. Expansion then
        // walks the template per row exactly as Prometheus/Go's Regexp.Expand does (see Replacement), rather than pre-parsing it.
        Replacement template = Replacement.of(BytesRefs.toString(replacement.fold(toEvaluator.foldCtx())), pattern);
        // A dictionary-aware evaluator: on a dense, single-valued OrdinalBytesRefBlock it matches and expands once per distinct
        // dictionary entry rather than once per row (see RegexExpandOrdinalEvaluator); other blocks take the per-row path. The
        // compiled pattern and bound template are shared across driver threads, while the per-thread Matcher and reused output
        // buffers are created per DriverContext inside the evaluator.
        return new RegexExpandOrdinalEvaluator.Factory(source(), toEvaluator.apply(src), pattern, template);
    }

    /**
     * Compiles {@code regex} into the RE2/J {@link Pattern} used to match a source label value, anchored exactly as
     * Prometheus's {@code label_replace} does: {@code ^(?s:regex)$}. The {@code (?s:...)} flag makes {@code .} also match
     * newlines and the anchors force a whole-string match. This is the single place that anchoring is applied, so the
     * evaluator (which matches) and the analyzer (which validates the user-supplied pattern via {@link #validateRegex})
     * stay in lockstep.
     *
     * @throws PatternSyntaxException if {@code regex} is not a valid RE2 pattern
     */
    private static Pattern compilePattern(String regex) {
        return Pattern.compile("^(?s:" + regex + ")$");
    }

    /**
     * Validates a user-supplied {@code label_replace} regular expression by compiling it exactly as {@link #toEvaluator}
     * will, so an invalid pattern is rejected during analysis rather than surfacing later during execution. Returns
     * {@code null} when {@code regex} compiles, otherwise the RE2/J syntax-error message. Returning the message rather than
     * throwing keeps the RE2/J dependency contained to this class: the analyzer need not depend on {@code com.google.re2j}.
     */
    public static String validateRegex(String regex) {
        try {
            compilePattern(regex);
            return null;
        } catch (PatternSyntaxException e) {
            return e.getMessage();
        }
    }

    /**
     * Derives the destination value for a single position and appends it to {@code builder}: {@code null} on no match (the
     * no-op sentinel), an empty {@link BytesRef} on a matched-but-empty expansion (the delete sentinel), or the expansion
     * otherwise. Throws {@link IllegalArgumentException} on a multivalued position; the calling {@link RegexExpandOrdinalEvaluator}
     * turns that into the ES|QL "single-value function encountered multi-value" warning and a {@code null} result. The
     * {@code matcher} and the {@code out}/{@code outValue} buffers are caller-owned and reused across positions.
     */
    static void process(
        BytesRefBlock.Builder builder,
        int p,
        BytesRefBlock srcBlock,
        Matcher matcher,
        BytesRef scratch,
        BytesRefBuilder out,
        BytesRef outValue,
        Replacement template
    ) {
        // A single label has a single value, so a multivalued source has no defined value to match. Follow the ES|QL
        // single-value contract (the same one label_join inherits from Concat): the calling evaluator turns this exception into
        // a "single-value function encountered multi-value" warning and a null (no-op) result, leaving the destination untouched.
        if (srcBlock.getValueCount(p) > 1) {
            throw new IllegalArgumentException("single-value function encountered multi-value");
        }
        BytesRef result = matchAndExpand(inputBytes(srcBlock, p, scratch), matcher, out, outValue, template);
        if (result == null) {
            // No match: leave the destination label untouched (never set it to src).
            builder.appendNull();
        } else {
            // A match with an empty expansion is the delete sentinel (empty BytesRef); a non-empty expansion sets the label.
            builder.appendBytesRef(result);
        }
    }

    /**
     * Matches {@code input} - the exact-size UTF-8 bytes of a single source value - against the anchored pattern and expands the
     * replacement template, returning the (possibly empty, i.e. delete-sentinel) expansion, or {@code null} for the no-op
     * sentinel when the pattern does not match. Matching stays on the raw UTF-8 bytes because RE2/J matches an identical rune
     * stream in UTF-8 and UTF-16 mode, which avoids a decode-to-String/re-encode round-trip and lets the expansion slice capture
     * groups straight out of {@code input} (capture-group offsets are byte offsets in this mode). The returned {@link BytesRef}
     * is a view over {@code out}, valid only until the next call, so callers copy it before reusing the buffer. Shared by the
     * per-row {@link #process} and the dictionary fast path in {@link RegexExpandOrdinalEvaluator}.
     */
    static BytesRef matchAndExpand(byte[] input, Matcher matcher, BytesRefBuilder out, BytesRef outValue, Replacement template) {
        matcher.reset(input);
        if (matcher.matches() == false) {
            return null;
        }
        return template.expand(matcher, input, out, outValue);
    }

    /**
     * The single-valued string value at position {@code p} as an exact-size UTF-8 {@code byte[]} (see {@link #toExactBytes}), or
     * {@link BytesRef#EMPTY_BYTES} when the position has no value. Null positions never reach here (the evaluator short-circuits
     * them) and multivalued positions are rejected by {@link #process} before this is called, so this only guards the empty
     * (zero-value) case; the caller coalesces an absent source label to {@code ""} upstream. {@code scratch} is a caller-owned
     * read buffer that holds nothing past this call and may be reused.
     */
    private static byte[] inputBytes(BytesRefBlock block, int p, BytesRef scratch) {
        if (block.getValueCount(p) == 0) {
            return BytesRef.EMPTY_BYTES;
        }
        return toExactBytes(block.getBytesRef(block.getFirstValueIndex(p), scratch));
    }

    /**
     * Copies {@code value}'s bytes into an exact-size array. RE2/J's {@link Matcher#reset(byte[])} matches the whole array (it
     * has no offset/length form), while a block hands back a {@link BytesRef} view into shared storage with an arbitrary offset
     * and an over-sized backing array, so a fresh copy is required before matching. Shared by the per-row path and the
     * dictionary fast path.
     */
    static byte[] toExactBytes(BytesRef value) {
        return Arrays.copyOfRange(value.bytes, value.offset, value.offset + value.length);
    }

    /**
     * The {@code label_replace} replacement template, bound once (in {@link #toEvaluator}) to the source pattern's
     * capture-group metadata and expanded per row by {@link #expand}. This is a direct port of the Go standard library
     * {@code regexp} package - the implementation Prometheus's {@code label_replace} itself uses - specifically
     * {@code Regexp.expand} and its {@code extract} helper in {@code src/regexp/regexp.go} (The Go Authors, BSD-3-Clause;
     * https://cs.opensource.google/go/go). Like Go, the template is walked on every expansion rather than pre-parsed into
     * segments; it is held as UTF-8 bytes (Go strings are already UTF-8) so literal runs are appended verbatim and capture
     * groups are sliced straight from the matched input, with no per-row allocation for the common numeric-reference templates.
     * <p>
     * The expansion is hand-rolled rather than delegated to RE2/J's or the JDK's {@code Matcher.replaceAll}, whose Java
     * replacement grammar diverges from Go's on inputs valid PromQL produces: Java escapes a literal {@code $} as {@code \$}
     * whereas Go uses {@code $$}; an out-of-range numeric reference throws instead of expanding to {@code ""}; an unmatched
     * named reference expands to the literal text {@code "null"} instead of {@code ""}; and {@code $1x}/{@code $01} bind to
     * group 1 (plus trailing text) instead of the longest-name references {@code 1x}/{@code 01}. Matching Go is required for
     * Prometheus parity, and no JDK or library helper provides these semantics. Reference names are matched as ASCII letters,
     * digits, and underscores, which covers RE2 capture-group names (Go additionally admits non-ASCII Unicode name chars, so a
     * reference name with a non-ASCII character terminates one byte earlier here; such a name never matches a real group and
     * so expands to {@code ""} either way).
     */
    static final class Replacement {
        private final byte[] template;
        private final int groupCount;
        private final Map<String, Integer> namedGroups;

        private Replacement(byte[] template, int groupCount, Map<String, Integer> namedGroups) {
            this.template = template;
            this.groupCount = groupCount;
            this.namedGroups = namedGroups;
        }

        /** Binds {@code template} to {@code pattern}'s group metadata, holding it as UTF-8 bytes for per-row expansion. */
        static Replacement of(String template, Pattern pattern) {
            return new Replacement(template.getBytes(StandardCharsets.UTF_8), pattern.groupCount(), pattern.namedGroups());
        }

        /**
         * Expands the template against the current match into {@code out}, returning a {@link BytesRef} view over the result.
         * A direct port of Go's {@code Regexp.expand} ({@code regexp.go}): literal runs are appended verbatim; {@code $$} is a
         * literal {@code $}; a {@code $name}/{@code ${name}} reference expands to the matching capture group (a numeric index,
         * or failing that a same-named group), or to nothing when the reference is unknown, out of range, or an unmatched
         * optional group ({@code start < 0}); a {@code $} not followed by a valid reference is literal. Group text is sliced
         * straight from {@code input} (the exact-size UTF-8 bytes that were matched, in which capture offsets are byte
         * offsets). {@code out} is cleared first, so the returned view is valid only until the next call and callers must copy
         * it (as {@code BytesRefBlock.Builder#appendBytesRef} does) before reuse.
         */
        BytesRef expand(Matcher matcher, byte[] input, BytesRefBuilder out, BytesRef outValue) {
            out.clear();
            byte[] t = template;
            int n = t.length;
            int i = 0;
            while (i < n) {
                int dollar = indexOfDollar(t, i);
                if (dollar < 0) {
                    out.append(t, i, n - i);
                    break;
                }
                if (dollar > i) {
                    out.append(t, i, dollar - i);
                }
                int j = dollar + 1;
                if (j < n && t[j] == '$') {
                    // Go treats $$ as a literal $.
                    out.append((byte) '$');
                    i = j + 1;
                    continue;
                }
                // Go's extract: an optional '{', then the longest run of name bytes; a braced form must close with '}'.
                boolean braced = j < n && t[j] == '{';
                int nameStart = braced ? j + 1 : j;
                int nameEnd = nameStart;
                while (nameEnd < n && isNameByte(t[nameEnd])) {
                    nameEnd++;
                }
                int rest = nameEnd;
                boolean malformed = nameEnd == nameStart; // empty name
                if (braced && malformed == false) {
                    if (nameEnd < n && t[nameEnd] == '}') {
                        rest = nameEnd + 1;
                    } else {
                        malformed = true; // missing closing brace
                    }
                }
                if (malformed) {
                    // A '$' not followed by a valid reference is a literal '$'; resume scanning right after it.
                    out.append((byte) '$');
                    i = dollar + 1;
                    continue;
                }
                i = rest;
                int num = numericGroupIndex(t, nameStart, nameEnd);
                if (num >= 0) {
                    // An out-of-range numeric reference expands to "", i.e. contributes nothing.
                    if (num <= groupCount) {
                        appendGroup(matcher, input, out, num);
                    }
                } else {
                    Integer group = namedGroups.get(new String(t, nameStart, nameEnd - nameStart, StandardCharsets.UTF_8));
                    // An unknown named reference expands to "", i.e. contributes nothing.
                    if (group != null) {
                        appendGroup(matcher, input, out, group);
                    }
                }
            }
            outValue.bytes = out.bytes();
            outValue.offset = 0;
            outValue.length = out.length();
            return outValue;
        }
    }

    /** Appends capture group {@code g}'s bytes (sliced from {@code input}) to {@code out}, or nothing if the group did not match. */
    private static void appendGroup(Matcher matcher, byte[] input, BytesRefBuilder out, int g) {
        int start = matcher.start(g);
        if (start >= 0) {
            out.append(input, start, matcher.end(g) - start);
        }
    }

    /** The index of the next {@code '$'} byte in {@code t} at or after {@code from}, or {@code -1} if there is none. */
    private static int indexOfDollar(byte[] t, int from) {
        for (int i = from; i < t.length; i++) {
            if (t[i] == '$') {
                return i;
            }
        }
        return -1;
    }

    private static boolean isNameByte(byte b) {
        return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_';
    }

    /**
     * The numeric capture-group index the template byte range {@code [from, to)} denotes, or {@code -1} if it is not a numeric
     * index and must be resolved as a named group. Mirrors Go's {@code regexp.extract}: numeric only when it is all ASCII
     * digits, does not overflow (Go caps accumulation at {@code 1e8}), and is not a multi-digit value with a leading zero. This
     * makes {@code $01} and {@code $00} named lookups rather than indices 1 and 0, matching Prometheus.
     */
    private static int numericGroupIndex(byte[] t, int from, int to) {
        if (from >= to) {
            return -1;
        }
        int num = 0;
        for (int i = from; i < to; i++) {
            byte b = t[i];
            if (b < '0' || b > '9' || num >= 100_000_000) {
                return -1;
            }
            num = num * 10 + (b - '0');
        }
        if (t[from] == '0' && to - from > 1) {
            return -1;
        }
        return num;
    }
}
