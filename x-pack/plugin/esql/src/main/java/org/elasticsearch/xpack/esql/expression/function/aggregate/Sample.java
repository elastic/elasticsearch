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
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SampleBooleanAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SampleBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SampleDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SampleIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SampleLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isRepresentableExceptCountersDenseVectorAndAggregateMetricDouble;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.expression.Foldables.TypeResolutionValidator.forPostOptimizationValidation;
import static org.elasticsearch.xpack.esql.expression.Foldables.TypeResolutionValidator.forPreOptimizationValidation;
import static org.elasticsearch.xpack.esql.expression.Foldables.resolveTypeLimit;

public class Sample extends AggregateFunction implements ToAggregator, PostOptimizationVerificationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Sample", Sample::new);

    @FunctionInfo(
        returnType = {
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
            "version" },
        description = "Collects sample values for a field.",
        type = FunctionType.AGGREGATE,
        examples = @Example(file = "stats_sample", tag = "doc"),
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.1.0") }

    )
    public Sample(
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
                "version" },
            description = "The field to collect sample values for."
        ) Expression field,
        @Param(name = "limit", type = { "integer" }, description = "The maximum number of values to collect.") Expression limit
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW, limit, new Literal(Source.EMPTY, Randomness.get().nextLong(), DataType.LONG));
    }

    /**
     * The query "FROM data | STATS s1=SAMPLE(x,N), s2=SAMPLE(x,N)" should give two different
     * samples of size N. The uuid is used to ensure that the optimizer does not optimize both
     * expressions to one, resulting in identical samples.
     */
    public Sample(Source source, Expression field, Expression filter, Expression window, Expression limit, Expression uuid) {
        super(source, field, filter, window, List.of(limit, uuid));
    }

    private Sample(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        var typeResolution = isRepresentableExceptCountersDenseVectorAndAggregateMetricDouble(field(), sourceText(), FIRST).and(
            isNotNull(limitField(), sourceText(), SECOND)
        ).and(isType(limitField(), dt -> dt == DataType.INTEGER, sourceText(), SECOND, "integer"));
        if (typeResolution.unresolved()) {
            return typeResolution;
        }
        TypeResolution result = resolveTypeLimit(limitField(), sourceText(), forPreOptimizationValidation(limitField()));
        if (result.equals(TypeResolution.TYPE_RESOLVED) == false) {
            return result;
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return field().dataType().noText();
    }

    @Override
    protected NodeInfo<Sample> info() {
        return NodeInfo.create(this, Sample::new, field(), filter(), window(), limitField(), uuid());
    }

    @Override
    public Sample replaceChildren(List<Expression> newChildren) {
        return new Sample(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3), newChildren.get(4));
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        return switch (PlannerUtils.toElementType(field().dataType())) {
            case BOOLEAN -> new SampleBooleanAggregatorFunctionSupplier(limitValue());
            case BYTES_REF -> new SampleBytesRefAggregatorFunctionSupplier(limitValue());
            case DOUBLE -> new SampleDoubleAggregatorFunctionSupplier(limitValue());
            case INT -> new SampleIntAggregatorFunctionSupplier(limitValue());
            case LONG -> new SampleLongAggregatorFunctionSupplier(limitValue());
            default -> throw EsqlIllegalArgumentException.illegalDataType(field().dataType());
        };
    }

    @Override
    public Sample withFilter(Expression filter) {
        return new Sample(source(), field(), filter, window(), limitField(), uuid());
    }

    Expression limitField() {
        return parameters().get(0);
    }

    private int limitValue() {
        return Foldables.limitValue(limitField(), sourceText());
    }

    Expression uuid() {
        return parameters().get(1);
    }

    @Override
    public void postOptimizationVerification(Failures failures) {
        Foldables.resolveTypeLimit(limitField(), sourceText(), forPostOptimizationValidation(limitField(), failures));
    }
}
