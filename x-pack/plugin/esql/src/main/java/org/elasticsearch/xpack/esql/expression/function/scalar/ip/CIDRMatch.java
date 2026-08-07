/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.ip;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.network.CIDRUtils;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.capabilities.TransportVersionAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.TermsQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.fromIndex;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isIPAndExact;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isStringAndExact;

/**
 * This function takes a first parameter of type IP, followed by one or more parameters evaluated to a CIDR specification:
 * <ul>
 * <li>a string literal;</li>
 * <li>a field of type keyword;</li>
 * <li>a function outputting a keyword.</li>
 * </ul><p>
 * Returns {@code true} if <em>any</em> value of the IP parameter falls in <em>any</em> of the provided CIDR blocks
 * (multivalue OR). Missing/empty IP or CIDR arguments yield {@code false}, never {@code null}.
 * <p>
 * Foldable blocks are lowered onto {@link MvInRange} when the cluster's transport version supports it, so Lucene
 * pushdown inherits any-value range semantics. Non-foldable blocks keep this node and run the block evaluator —
 * still any-value / two-valued.
 * <p>
 * Example: {@code | eval cidr="10.0.0.0/8" | where cidr_match(ip_field, "127.0.0.1/30", cidr)}
 */
public class CIDRMatch extends EsqlScalarFunction implements TranslationAware, TransportVersionAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "CIDRMatch",
        CIDRMatch::new
    );

    /**
     * Transport version from which foldable {@code CIDR_MATCH} lowers onto {@link MvInRange} for Lucene range
     * pushdown over multivalue {@code ip} fields.
     */
    public static final TransportVersion ESQL_CIDR_MATCH_MV_IN_RANGE = TransportVersion.fromName("esql_cidr_match_mv_in_range");

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(CIDRMatch.class)
        .unaryVariadic(CIDRMatch::new)
        // Any-value (MV OR) + two-valued boolean; foldable form also lowers onto MV_IN_RANGE for pushdown.
        .capabilities("mv_in_range_lowering")
        .name("cidr_match");

    private final Expression ipField;
    private final List<Expression> matches;

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA) },
        returnType = "boolean",
        briefSummary = "Returns true if the provided IP is contained in one of the provided CIDR blocks.",
        description = "Returns true if the provided IP is contained in one of the provided CIDR blocks.",
        detailedDescription = """
            {applies_to}`stack: ga 9.6.0`
            Returns `true` if any value of the provided IP is contained in one of the provided CIDR
            blocks. A missing or empty IP (or CIDR) yields `false`, never `null`.
            """,
        examples = @Example(file = "ip", tag = "cdirMatchMultipleArgs")
    )
    public CIDRMatch(
        Source source,
        @Param(
            name = "ip",
            type = { "ip" },
            description = "IP address of type `ip` (both IPv4 and IPv6 are supported)."
        ) Expression ipField,
        @Param(name = "blockX", type = { "keyword", "text" }, description = "CIDR block to test the IP against.") List<Expression> matches
    ) {
        super(source, CollectionUtils.combine(singletonList(ipField), matches));
        this.ipField = ipField;
        this.matches = matches;
    }

    private CIDRMatch(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        assert children().size() > 1;
        out.writeNamedWriteable(children().get(0));
        out.writeNamedWriteableCollection(children().subList(1, children().size()));
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression ipField() {
        return ipField;
    }

    public List<Expression> matches() {
        return matches;
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (matches.size() == 1) {
            return new CIDRMatchEvaluator.Factory(
                source(),
                toEvaluator.apply(ipField),
                toEvaluator.apply(matches.get(0)),
                context -> new BytesRef(),
                context -> new BytesRef()
            );
        }
        // Variadic: OR of single-block CIDR_MATCH so the generated evaluator stays binary (ip + one CIDR block).
        List<Expression> parts = new ArrayList<>(matches.size());
        for (Expression match : matches) {
            parts.add(new CIDRMatch(source(), ipField, List.of(match)));
        }
        return ((EvaluatorMapper) Predicates.combineOr(parts)).toEvaluator(toEvaluator);
    }

    /**
     * Any-value over {@code ip}: {@code true} if any IP value falls in any value of the CIDR block.
     * Null/empty IP or CIDR → {@code false}. Multivalue CIDR blocks also use any-value (OR) semantics.
     * <p>
     * Uses {@code @Evaluator} with block parameters (same pattern as {@link MvInRange}), not {@code @MvEvaluator},
     * because this is a multi-argument predicate rather than a unary multivalue reducer.
     */
    @Evaluator(allNullsIsNull = false)
    static boolean process(
        @Position int position,
        BytesRefBlock ip,
        BytesRefBlock cidr,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BytesRef ipScratch,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BytesRef cidrScratch
    ) {
        int cidrCount = cidr.getValueCount(position);
        if (cidrCount == 0) {
            return false;
        }
        int ipCount = ip.getValueCount(position);
        if (ipCount == 0) {
            return false;
        }
        int ipStart = ip.getFirstValueIndex(position);
        int cidrStart = cidr.getFirstValueIndex(position);
        for (int i = ipStart; i < ipStart + ipCount; i++) {
            BytesRef addr = ip.getBytesRef(i, ipScratch);
            byte[] bytes = Arrays.copyOfRange(addr.bytes, addr.offset, addr.offset + addr.length);
            for (int j = cidrStart; j < cidrStart + cidrCount; j++) {
                BytesRef block = cidr.getBytesRef(j, cidrScratch);
                if (CIDRUtils.isInRange(bytes, block.utf8ToString())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isIPAndExact(ipField, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        int i = 1;
        for (var m : matches) {
            resolution = isStringAndExact(m, sourceText(), fromIndex(i++));
            if (resolution.unresolved()) {
                return resolution;
            }
        }

        return resolution;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new CIDRMatch(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, CIDRMatch::new, children().get(0), children().subList(1, children().size()));
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        // Foldable blocks are rewritten to MvInRange (bare any-value range). The residual form still pushes a
        // TermsQuery of CIDR strings — Lucene matches any value of a multivalue field by default.
        // Not SingleValueTranslationAware: MV OR must not wrap the Lucene query in SingleValueQuery.
        return pushdownPredicates.isPushableFieldAttribute(ipField) && Expressions.foldable(matches) ? Translatable.YES : Translatable.NO;
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        var fa = LucenePushdownPredicates.checkIsFieldAttribute(ipField);
        Check.isTrue(Expressions.foldable(matches), "Expected foldable matches, but got [{}]", matches);

        String targetFieldName = handler.nameOf(fa.exactAttribute());
        Set<Object> set = new LinkedHashSet<>(matches.stream().map(Foldables::literalValueOf).toList());

        return new TermsQuery(source(), targetFieldName, set);
    }

    /**
     * When the cluster supports {@link #ESQL_CIDR_MATCH_MV_IN_RANGE} and every CIDR block is foldable, rewrite into
     * {@link MvInRange} (or an {@code OR} of them) with inclusive IP bounds for range pushdown. Non-foldable blocks
     * keep this node and run the block evaluator (still any-value / two-valued).
     */
    @Override
    public Expression forTransportVersion(TransportVersion minTransportVersion) {
        if (minTransportVersion.supports(ESQL_CIDR_MATCH_MV_IN_RANGE) == false) {
            return null;
        }
        if (Expressions.foldable(matches) == false) {
            return null;
        }
        List<Expression> ranges = new ArrayList<>(matches.size());
        for (Expression match : matches) {
            Expression range = toMvInRange(match);
            if (range == null) {
                return null;
            }
            ranges.add(range);
        }
        return Predicates.combineOr(ranges);
    }

    /**
     * Folds {@code match} to a CIDR string / bare address and builds an inclusive {@link MvInRange} over
     * {@link #ipField}. Returns {@code null} if the fold or parse fails (keep the block evaluator so a malformed
     * block still fails at evaluation time, matching today's behaviour).
     */
    private Expression toMvInRange(Expression match) {
        Object folded;
        try {
            folded = match.fold(FoldContext.small());
        } catch (Exception e) {
            return null;
        }
        if (folded == null) {
            return null;
        }
        String cidr = BytesRefs.toString(folded);
        try {
            byte[] lower;
            byte[] upper;
            if (cidr.contains("/")) {
                Tuple<byte[], byte[]> bounds = CIDRUtils.getLowerUpper(InetAddresses.parseCidr(cidr));
                lower = bounds.v1();
                upper = bounds.v2();
            } else {
                // Bare address → point range, mirroring CIDRUtils.isInRange.
                lower = InetAddresses.forString(cidr).getAddress();
                upper = lower;
            }
            Literal lo = new Literal(source(), new BytesRef(CIDRUtils.encode(lower)), DataType.IP);
            Literal hi = new Literal(source(), new BytesRef(CIDRUtils.encode(upper)), DataType.IP);
            return new MvInRange(source(), ipField, lo, hi);
        } catch (IllegalArgumentException | UnsupportedOperationException e) {
            return null;
        }
    }
}
