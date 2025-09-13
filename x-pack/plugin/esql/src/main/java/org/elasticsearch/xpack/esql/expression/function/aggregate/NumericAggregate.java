/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.AtomType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.AtomType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.AtomType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.AtomType.LONG;
import static org.elasticsearch.xpack.esql.core.type.AtomType.UNSIGNED_LONG;

/**
 * Aggregate function that receives a numeric, signed field, and returns a single double value.
 * <p>
 *     Implement the supplier methods to return the correct {@link AggregatorFunctionSupplier}.
 * </p>
 * <p>
 *     Some methods can be optionally overridden to support different variations:
 * </p>
 * <ul>
 *     <li>
 *         {@link #supportsDates}: override to also support dates. Defaults to false.
 *     </li>
 *     <li>
 *         {@link #resolveType}: override to support different parameters.
 *         Call {@code super.resolveType()} to add extra checks.
 *     </li>
 *     <li>
 *         {@link #dataType}: override to return a different datatype.
 *         You can return {@code field().dataType()} to propagate the parameter type.
 *     </li>
 * </ul>
 */
public abstract class NumericAggregate extends AggregateFunction implements ToAggregator {

    NumericAggregate(Source source, Expression field, List<Expression> parameters) {
        super(source, field, parameters);
    }

    NumericAggregate(Source source, Expression field, Expression filter, List<Expression> parameters) {
        super(source, field, filter, parameters);
    }

    NumericAggregate(Source source, Expression field) {
        super(source, field);
    }

    NumericAggregate(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected TypeResolution resolveType() {
        if (supportsDates()) {
            return TypeResolutions.isType(
                this,
                e -> e.atom() == DATETIME || e.atom().isNumeric() && e.atom() != UNSIGNED_LONG,
                sourceText(),
                DEFAULT,
                "datetime",
                "numeric except unsigned_long or counter types"
            );
        }
        return isType(
            field(),
            dt -> dt.atom().isNumeric() && dt.atom() != UNSIGNED_LONG,
            sourceText(),
            DEFAULT,
            "numeric except unsigned_long or counter types"
        );
    }

    protected boolean supportsDates() {
        return false;
    }

    @Override
    public DataType dataType() {
        return DOUBLE.type();
    }

    @Override
    public final AggregatorFunctionSupplier supplier() {
        DataType type = field().dataType();
        if (supportsDates() && type.atom() == DATETIME) {
            return longSupplier();
        }
        if (type.atom() == LONG) {
            return longSupplier();
        }
        if (type.atom() == INTEGER) {
            return intSupplier();
        }
        if (type.atom() == DOUBLE) {
            return doubleSupplier();
        }
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }

    protected abstract AggregatorFunctionSupplier longSupplier();

    protected abstract AggregatorFunctionSupplier intSupplier();

    protected abstract AggregatorFunctionSupplier doubleSupplier();
}
