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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNullAndFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class Sample extends AggregateFunction implements ToAggregator {
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
            "integer",
            "ip",
            "keyword",
            "long",
            "version" },
        description = "Collects sample values for a field.",
        type = FunctionType.AGGREGATE,
        examples = @Example(file = "stats_sample", tag = "doc")
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
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "version" },
            description = "The field to collect sample values for."
        ) Expression field,
        @Param(name = "limit", type = { "integer" }, description = "The maximum number of values to collect.") Expression limit
    ) {
        this(source, field, Literal.TRUE, limit);
    }

    public Sample(Source source, Expression field, Expression filter, Expression limit) {
        this(source, field, filter, limit, new Literal(Source.EMPTY, Randomness.get().nextLong(), DataType.LONG));
    }

    /**
     * The query "FROM data | STATS s1=SAMPLE(x,N), s2=SAMPLE(x,N)" should give two different
     * samples of size N. The uuid is used to ensure that the optimizer does not optimize both
     * expressions to one, resulting in identical samples.
     */
    public Sample(Source source, Expression field, Expression filter, Expression limit, Expression uuid) {
        super(source, field, filter, List.of(limit, uuid));
    }

    private Sample(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        var typeResolution = isType(field(), dt -> dt != DataType.UNSIGNED_LONG, sourceText(), FIRST, "any type except unsigned_long").and(
            isNotNullAndFoldable(limitField(), sourceText(), SECOND)
        ).and(isType(limitField(), dt -> dt == DataType.INTEGER, sourceText(), SECOND, "integer"));
        if (typeResolution.unresolved()) {
            return typeResolution;
        }
        int limit = limitValue();
        if (limit <= 0) {
            return new TypeResolution(format(null, "Limit must be greater than 0 in [{}], found [{}]", sourceText(), limit));
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
        return NodeInfo.create(this, Sample::new, field(), filter(), limitField(), uuid());
    }

    @Override
    public Sample replaceChildren(List<Expression> newChildren) {
        return new Sample(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
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
        return new Sample(source(), field(), filter, limitField(), uuid());
    }

    Expression limitField() {
        return parameters().get(0);
    }

    private int limitValue() {
        return (int) limitField().fold(FoldContext.small() /* TODO remove me */);
    }

    Expression uuid() {
        return parameters().get(1);
    }

}
