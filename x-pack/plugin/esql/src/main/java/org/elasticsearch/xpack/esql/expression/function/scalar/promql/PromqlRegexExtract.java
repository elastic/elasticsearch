/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.promql;

import com.google.re2j.Matcher;
import com.google.re2j.Pattern;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
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
 * The result encodes the three Prometheus outcomes so the downstream identity rewrite ({@code setLabel}) can act on them:
 * <ul>
 *     <li><b>set</b> - a non-empty expansion becomes the destination label value;</li>
 *     <li><b>delete</b> - a matched-but-empty expansion is returned as the empty string, removing the label;</li>
 *     <li><b>no-op</b> - on no match the position is {@code null}, leaving any existing destination label untouched
 *         (Prometheus never sets it to {@code src}).</li>
 * </ul>
 * The caller is responsible for materializing an absent {@code src} as the empty string (via {@code COALESCE(src, "")} during
 * translation): the generated evaluator short-circuits all-null positions to {@code null} before {@link #process} runs, so a
 * genuinely null {@code src} would otherwise become a spurious no-op instead of matching against {@code ""}.
 */
public final class PromqlRegexExtract extends EsqlScalarFunction implements VersionedNamedWriteable {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "PromqlRegexExtract",
        PromqlRegexExtract::new
    );

    /**
     * Transport version gating the PromQL label-function internal scalars ({@link PromqlRegexExtract}, {@link PromqlSetLabel}).
     */
    public static final TransportVersion PROMQL_LABEL_FUNCTIONS = TransportVersion.fromName("promql_label_functions");

    private final Expression src;
    private final Expression regex;
    private final Expression replacement;

    public PromqlRegexExtract(Source source, Expression src, Expression regex, Expression replacement) {
        super(source, List.of(src, regex, replacement));
        this.src = src;
        this.regex = regex;
        this.replacement = replacement;
    }

    private PromqlRegexExtract(StreamInput in) throws IOException {
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
        return new PromqlRegexExtract(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, PromqlRegexExtract::new, src, regex, replacement);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (regex.foldable() == false || replacement.foldable() == false) {
            throw new IllegalArgumentException("[regex] and [replacement] must be constants");
        }
        // Compile the pattern anchored exactly as Prometheus does (funcLabelReplace): ^(?s:regex)$, so `.` also matches newlines.
        Pattern pattern = Pattern.compile("^(?s:" + BytesRefs.toString(regex.fold(toEvaluator.foldCtx())) + ")$");
        BytesRef replacementValue = BytesRefs.toBytesRef(replacement.fold(toEvaluator.foldCtx()));
        return new PromqlRegexExtractEvaluator.Factory(source(), toEvaluator.apply(src), pattern, replacementValue);
    }

    @Evaluator
    static void process(
        BytesRefBlock.Builder builder,
        @Position int p,
        BytesRefBlock srcBlock,
        @Fixed Pattern pattern,
        @Fixed BytesRef replacement
    ) {
        String src = valueAt(srcBlock, p);
        Matcher matcher = pattern.matcher(src);
        if (matcher.matches() == false) {
            // No match: leave the destination label untouched (never set it to src).
            builder.appendNull();
            return;
        }
        // A match with an empty expansion is the delete sentinel (empty BytesRef); a non-empty expansion sets the label.
        builder.appendBytesRef(new BytesRef(expand(replacement.utf8ToString(), matcher, pattern.namedGroups())));
    }

    /**
     * The value of a single-valued string block at position {@code p}, or the empty string when the position has no value.
     * Null positions never reach here (the generated evaluator short-circuits them), so this only guards the empty-multivalue
     * case; the caller coalesces an absent source label to {@code ""} upstream.
     */
    private static String valueAt(BytesRefBlock block, int p) {
        if (block.getValueCount(p) == 0) {
            return "";
        }
        return block.getBytesRef(block.getFirstValueIndex(p), new BytesRef()).utf8ToString();
    }

    /**
     * Expands {@code template} against the successful {@code matcher}, following Go's {@code Regexp.Expand} rules (which
     * Prometheus uses): {@code $$} is a literal {@code $}; {@code $name}/{@code ${name}} reference a group where {@code name}
     * is the longest run of ASCII letters, digits, and underscores; an all-digit name is a group index; an unknown or
     * out-of-range reference expands to the empty string; a {@code $} not followed by a valid reference is literal.
     */
    static String expand(String template, Matcher matcher, Map<String, Integer> namedGroups) {
        StringBuilder out = new StringBuilder(template.length());
        int i = 0;
        int n = template.length();
        while (i < n) {
            char c = template.charAt(i);
            if (c != '$') {
                out.append(c);
                i++;
                continue;
            }
            if (i + 1 < n && template.charAt(i + 1) == '$') {
                out.append('$');
                i += 2;
                continue;
            }
            int j = i + 1;
            boolean braced = j < n && template.charAt(j) == '{';
            if (braced) {
                j++;
            }
            int nameStart = j;
            while (j < n && isNameChar(template.charAt(j))) {
                j++;
            }
            String name = template.substring(nameStart, j);
            if (braced) {
                if (j < n && template.charAt(j) == '}') {
                    j++;
                } else {
                    // Unterminated ${...}: treat the '$' as a literal and continue after it.
                    out.append('$');
                    i++;
                    continue;
                }
            }
            if (name.isEmpty()) {
                // '$' not followed by a valid reference: literal '$'.
                out.append('$');
                i++;
                continue;
            }
            out.append(resolveGroup(name, matcher, namedGroups));
            i = j;
        }
        return out.toString();
    }

    private static String resolveGroup(String name, Matcher matcher, Map<String, Integer> namedGroups) {
        if (isAllDigits(name)) {
            int num;
            try {
                num = Integer.parseInt(name);
            } catch (NumberFormatException e) {
                return "";
            }
            if (num >= 0 && num <= matcher.groupCount()) {
                String group = matcher.group(num);
                return group == null ? "" : group;
            }
            return "";
        }
        Integer index = namedGroups.get(name);
        if (index == null) {
            return "";
        }
        String group = matcher.group(index);
        return group == null ? "" : group;
    }

    private static boolean isNameChar(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
    }

    private static boolean isAllDigits(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) < '0' || s.charAt(i) > '9') {
                return false;
            }
        }
        return true;
    }
}
