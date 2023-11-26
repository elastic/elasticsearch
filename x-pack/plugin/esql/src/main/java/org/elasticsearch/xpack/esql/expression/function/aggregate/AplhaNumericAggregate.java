/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.util.BigArrays;
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
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;

public abstract class AplhaNumericAggregate extends AggregateFunction implements ToAggregator {

    AplhaNumericAggregate(Source source, Expression field, List<Expression> parameters) {
        super(source, field, parameters);
    }

    AplhaNumericAggregate(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected TypeResolution resolveType() {
        if (supportsDates()) {
            return TypeResolutions.isType(
                this,
                e -> e.isNumeric() || e == DataTypes.DATETIME || e == DataTypes.KEYWORD,
                sourceText(),
                DEFAULT,
                "numeric",
                "datetime",
                "keyword"
            );
        }
        return TypeResolutions.isType(
            this,
            e -> e.isNumeric() || e == DataTypes.KEYWORD,
            sourceText(),
            DEFAULT,
            "numeric",
            "keyword"
        );
    }

    protected boolean supportsDates() {
        return false;
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    public final AggregatorFunctionSupplier supplier(BigArrays bigArrays, List<Integer> inputChannels) {
        DataType type = field().dataType();
        if (supportsDates() && type == DataTypes.DATETIME) {
            return longSupplier(bigArrays, inputChannels);
        }
        if (type == DataTypes.LONG) {
            return longSupplier(bigArrays, inputChannels);
        }
        if (type == DataTypes.INTEGER) {
            return intSupplier(bigArrays, inputChannels);
        }
        if (type == DataTypes.DOUBLE) {
            return doubleSupplier(bigArrays, inputChannels);
        }
        if (type == DataTypes.KEYWORD) {
            return BytesRefSupplier(bigArrays, inputChannels);
        }
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }

    protected abstract AggregatorFunctionSupplier longSupplier(BigArrays bigArrays, List<Integer> inputChannels);

    protected abstract AggregatorFunctionSupplier intSupplier(BigArrays bigArrays, List<Integer> inputChannels);

    protected abstract AggregatorFunctionSupplier doubleSupplier(BigArrays bigArrays, List<Integer> inputChannels);

    protected abstract AggregatorFunctionSupplier BytesRefSupplier(BigArrays bigArrays, List<Integer> inputChannels);
}
