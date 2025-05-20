/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.ip;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.CIDRUtils;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.TermsQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonList;
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
 * The function will match if the IP parameter is within any (not all) of the ranges defined by the provided CIDR specs.
 * <p>
 * Example: `| eval cidr="10.0.0.0/8" | where cidr_match(ip_field, "127.0.0.1/30", cidr)`
 */
public class CIDRMatch extends EsqlScalarFunction implements TranslationAware.SingleValueTranslationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "CIDRMatch",
        CIDRMatch::new
    );

    private final Expression ipField;
    private final List<Expression> matches;

    @FunctionInfo(
        returnType = "boolean",
        description = "Returns true if the provided IP is contained in one of the provided CIDR blocks.",
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
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var ipEvaluatorSupplier = toEvaluator.apply(ipField);
        return new CIDRMatchEvaluator.Factory(
            source(),
            ipEvaluatorSupplier,
            matches.stream().map(toEvaluator::apply).toArray(EvalOperator.ExpressionEvaluator.Factory[]::new)
        );
    }

    @Evaluator
    static boolean process(BytesRef ip, BytesRef[] cidrs) {
        for (var cidr : cidrs) {
            // simple copy is safe, Java uses big-endian, same as network order
            if (CIDRUtils.isInRange(Arrays.copyOfRange(ip.bytes, ip.offset, ip.offset + ip.length), cidr.utf8ToString())) {
                return true;
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
        return pushdownPredicates.isPushableFieldAttribute(ipField) && Expressions.foldable(matches) ? Translatable.YES : Translatable.NO;
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        var fa = LucenePushdownPredicates.checkIsFieldAttribute(ipField);
        Check.isTrue(Expressions.foldable(matches), "Expected foldable matches, but got [{}]", matches);

        String targetFieldName = handler.nameOf(fa.exactAttribute());
        Set<Object> set = new LinkedHashSet<>(Expressions.fold(FoldContext.small() /* TODO remove me */, matches));

        return new TermsQuery(source(), targetFieldName, set);
    }

    @Override
    public Expression singleValueField() {
        return ipField;
    }
}
