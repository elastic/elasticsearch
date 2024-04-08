/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.planner.ToAggregator;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

public abstract class NumericAggregate extends AggregateFunction implements ToAggregator {

    NumericAggregate(Source source, Expression field, List<Expression> parameters) {
        super(source, field, parameters);
    }

    NumericAggregate(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected TypeResolution resolveType() {
        if (supportsDates()) {
            return TypeResolutions.isType(
                this,
                e -> e == DataTypes.DATETIME || e.isNumeric() && e != DataTypes.UNSIGNED_LONG,
                sourceText(),
                DEFAULT,
                "datetime",
                "numeric except unsigned_long"
            );
        }
        return isType(
            field(),
            dt -> dt.isNumeric() && dt != DataTypes.UNSIGNED_LONG,
            sourceText(),
            DEFAULT,
            "numeric except unsigned_long"
        );
    }

    protected boolean supportsDates() {
        return false;
    }

    @Override
    public DataType dataType() {
        return DataTypes.DOUBLE;
    }

    @Override
    public final AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        DataType type = field().dataType();
        if (supportsDates() && type == DataTypes.DATETIME) {
            return longSupplier(inputChannels);
        }
        if (type == DataTypes.LONG) {
            return longSupplier(inputChannels);
        }
        if (type == DataTypes.INTEGER) {
            return intSupplier(inputChannels);
        }
        if (type == DataTypes.DOUBLE) {
            return doubleSupplier(inputChannels);
        }
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }

    protected abstract AggregatorFunctionSupplier longSupplier(List<Integer> inputChannels);

    protected abstract AggregatorFunctionSupplier intSupplier(List<Integer> inputChannels);

    protected abstract AggregatorFunctionSupplier doubleSupplier(List<Integer> inputChannels);
}
