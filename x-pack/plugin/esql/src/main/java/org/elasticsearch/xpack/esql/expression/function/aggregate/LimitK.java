/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndHistogram;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.expression.Foldables.TypeResolutionValidator.forPostOptimizationValidation;
import static org.elasticsearch.xpack.esql.expression.Foldables.TypeResolutionValidator.forPreOptimizationValidation;
import static org.elasticsearch.xpack.esql.expression.Foldables.resolveTypeLimit;

/**
 * ES|QL aggregate analogue of PromQL {@code limitk(k, v)}: collects an arbitrary sample of {@code k} values per group.
 * <p>
 * This is a thin surrogate over {@link Sample}. Like {@code SAMPLE}, the selection is a random sample rather than
 * storage-order first-k; both are arbitrary subsets rather than value-ranked selections, unlike {@code TOPK}.
 */
public class LimitK extends AggregateFunction implements SurrogateExpression, PostOptimizationVerificationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "LimitK", LimitK::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(LimitK.class)
        .binary(LimitK::new)
        .capabilities("flattened")
        .name("limitk");

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA) },
        returnType = {
            "boolean",
            "cartesian_point",
            "cartesian_shape",
            "date",
            "date_nanos",
            "double",
            "flattened",
            "geo_point",
            "geo_shape",
            "geohash",
            "geotile",
            "geohex",
            "integer",
            "ip",
            "keyword",
            "long",
            "unsigned_long",
            "version" },
        briefSummary = "Collects sample values for a field.",
        description = "Collects an arbitrary sample of up to `k` values for a field. "
            + "Shorthand for `SAMPLE(field, k)`, and the ES|QL aggregate analogue of PromQL `limitk(k, v)`.",
        type = FunctionType.AGGREGATE,
        examples = @Example(file = "stats_topk", tag = "limitk")
    )
    public LimitK(
        Source source,
        @Param(
            name = "field",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date",
                "date_nanos",
                "double",
                "geo_point",
                "geo_shape",
                "geohash",
                "geotile",
                "geohex",
                "integer",
                "ip",
                "keyword",
                "long",
                "unsigned_long",
                "text",
                "flattened",
                "version" },
            description = "The field to collect sample values for."
        ) Expression field,
        @Param(
            name = "k",
            type = { "integer" },
            hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT),
            description = "The maximum number of values to collect."
        ) Expression limit
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW, limit, new Literal(Source.EMPTY, Randomness.get().nextLong(), DataType.LONG));
    }

    public LimitK(Source source, Expression field, Expression filter, Expression window, Expression limit, Expression uuid) {
        super(source, field, filter, window, List.of(limit, uuid));
    }

    private LimitK(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public LimitK withFilter(Expression filter) {
        return new LimitK(source(), field(), filter, window(), limitField(), uuid());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    Expression limitField() {
        return parameters().get(0);
    }

    Expression uuid() {
        return parameters().get(1);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        var typeResolution = isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndHistogram(field(), sourceText(), FIRST).and(
            isNotNull(limitField(), sourceText(), SECOND)
        ).and(isType(limitField(), dt -> dt == DataType.INTEGER, sourceText(), SECOND, "integer"));
        if (typeResolution.unresolved()) {
            return typeResolution;
        }
        return resolveTypeLimit(limitField(), sourceText(), forPreOptimizationValidation(limitField()));
    }

    @Override
    public void postOptimizationVerification(Failures failures) {
        resolveTypeLimit(limitField(), sourceText(), forPostOptimizationValidation(limitField(), failures));
    }

    @Override
    public DataType dataType() {
        return field().dataType().noText();
    }

    @Override
    protected NodeInfo<LimitK> info() {
        return NodeInfo.create(this, LimitK::new, field(), filter(), window(), limitField(), uuid());
    }

    @Override
    public LimitK replaceChildren(List<Expression> newChildren) {
        return new LimitK(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3), newChildren.get(4));
    }

    @Override
    public Expression surrogate() {
        return new Sample(source(), field(), filter(), window(), limitField(), uuid());
    }
}
