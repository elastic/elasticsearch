/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.TimeSeriesMetadata;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

/**
 * Casts the keyword {@code _timeseries} - the source of a series' dimensions, as nodes without typed metadata read it - to
 * typed metadata, so a plan exposes one {@code _timeseries} type whichever nodes read it. Dimensions under objects are
 * flattened to their dotted field names and named the way the typed reader names them. Values are their source text, so
 * they can differ from a typed read where the mapping coerces the source value.
 * <p>
 * Internal: PromQL resolution places it on the coordinator, above every aggregate, where the typed type is supported.
 */
public final class ToTimeSeries extends AbstractConvertFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToTimeSeries",
        ToTimeSeries::new
    );

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.of(
        KEYWORD,
        (source, field) -> new ToTimeSeriesFromKeywordEvaluator.Factory(
            source,
            field,
            context -> new BreakingBytesRefBuilder(context.breaker(), "to_timeseries")
        )
    );

    public ToTimeSeries(Source source, Expression field) {
        super(source, field);
    }

    private ToTimeSeries(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return DataType.TIMESERIES_DATA_TYPE;
    }

    @Override
    public boolean foldable() {
        return false;
    }

    /** The source form of a series' dimensions, encoded as typed metadata in the breaker-accounted {@code scratch}. */
    @ConvertEvaluator(extraName = "FromKeyword")
    static BytesRef fromKeyword(BytesRef in, @Fixed(includeInToString = false, scope = THREAD_LOCAL) BreakingBytesRefBuilder scratch) {
        scratch.clear();
        try {
            TimeSeriesMetadata.readSource(in, PromqlLabels::labelName).writeTo(new TimeSeriesMetadataOutputStream(scratch));
        } catch (IOException e) {
            throw new UncheckedIOException("Invalid time-series metadata", e);
        }
        return scratch.bytesRefView();
    }

    @Override
    public ToTimeSeries replaceChildren(List<Expression> newChildren) {
        return new ToTimeSeries(source(), newChildren.getFirst());
    }

    @Override
    protected NodeInfo<ToTimeSeries> info() {
        return NodeInfo.create(this, ToTimeSeries::new, field());
    }
}
