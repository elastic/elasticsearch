/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Casts the keyword {@code _timeseries} - the source of a series' dimensions, as nodes without typed metadata read it - to
 * typed metadata, so a plan exposes one {@code _timeseries} type whichever nodes read it. Dimensions under objects are
 * flattened to their dotted field names and named the way the typed reader names them. Values are their source text, so
 * they can differ from a typed read where the mapping coerces the source value.
 * <p>
 * Internal: PromQL resolution places it on the coordinator, above every aggregate, where the typed type is supported.
 */
public final class TimeSeriesFromKeyword extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "TimeSeriesFromKeyword",
        TimeSeriesFromKeyword::new
    );

    public TimeSeriesFromKeyword(Source source, Expression field) {
        super(source, field);
    }

    private TimeSeriesFromKeyword(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isType(field(), type -> type == DataType.KEYWORD, sourceText(), ParamOrdinal.DEFAULT, "keyword");
    }

    @Override
    public DataType dataType() {
        return DataType.TIMESERIES_DATA_TYPE;
    }

    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return TimeSeriesEvaluator.fromKeywordFactory(toEvaluator.apply(field()));
    }

    @Override
    public TimeSeriesFromKeyword replaceChildren(List<Expression> newChildren) {
        return new TimeSeriesFromKeyword(source(), newChildren.getFirst());
    }

    @Override
    protected NodeInfo<TimeSeriesFromKeyword> info() {
        return NodeInfo.create(this, TimeSeriesFromKeyword::new, field());
    }
}
