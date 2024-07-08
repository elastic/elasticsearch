/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesBooleanAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;

public class Values extends AggregateFunction implements ToAggregator, SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Values", Values::new);

    @FunctionInfo(
        returnType = { "boolean", "date", "double", "integer", "ip", "keyword", "long", "text", "version" },
        preview = true,
        description = "Returns all values in a group as a multivalued field. The order of the returned values isn't guaranteed. "
            + "If you need the values returned in order use <<esql-mv_sort>>.",
        appendix = """
            [WARNING]
            ====
            This can use a significant amount of memory and ES|QL doesn't yet
            grow aggregations beyond memory. So this aggregation will work until
            it is used to collect more values than can fit into memory. Once it
            collects too many values it will fail the query with
            a <<circuit-breaker-errors, Circuit Breaker Error>>.
            ====""",
        isAggregation = true,
        examples = @Example(file = "string", tag = "values-grouped")
    )
    public Values(
        Source source,
        @Param(name = "field", type = { "boolean", "date", "double", "integer", "ip", "keyword", "long", "text", "version" }) Expression v
    ) {
        super(source, v);
    }

    private Values(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Values> info() {
        return NodeInfo.create(this, Values::new, field());
    }

    @Override
    public Values replaceChildren(List<Expression> newChildren) {
        return new Values(source(), newChildren.get(0));
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    protected TypeResolution resolveType() {
        return TypeResolutions.isType(
            field(),
            dt -> DataType.isSpatial(dt) == false && dt != UNSIGNED_LONG,
            sourceText(),
            DEFAULT,
            "any type except unsigned_long and spatial types"
        );
    }

    @Override
    public AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        DataType type = field().dataType();
        if (type == DataType.INTEGER) {
            return new ValuesIntAggregatorFunctionSupplier(inputChannels);
        }
        if (type == DataType.LONG || type == DataType.DATETIME) {
            return new ValuesLongAggregatorFunctionSupplier(inputChannels);
        }
        if (type == DataType.DOUBLE) {
            return new ValuesDoubleAggregatorFunctionSupplier(inputChannels);
        }
        if (DataType.isString(type) || type == DataType.IP || type == DataType.VERSION) {
            return new ValuesBytesRefAggregatorFunctionSupplier(inputChannels);
        }
        if (type == DataType.BOOLEAN) {
            return new ValuesBooleanAggregatorFunctionSupplier(inputChannels);
        }
        // TODO cartesian_point, geo_point
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }

    @Override
    public Expression surrogate() {
        return field().foldable() ? field() : null;
    }
}
