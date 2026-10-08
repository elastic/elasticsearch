/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Unsets dimensions in a {@code _timeseries} value, the time-series metadata identifying a series. {@link TimeSeriesWithout}
 * excludes dimensions while the source loads that value; this removes them from a value already loaded, where the series'
 * identity changes further up the plan.
 * <p>
 * Dimensions are named by field name and must be constant. A dimension the value doesn't carry is left alone; with no
 * dimensions the value is only rewritten in its canonical form. The encoding is {@link TimeSeriesMetadataSupport}'s.
 * <p>
 * Internal to PromQL translation: not a user-facing function.
 */
public class TimeSeriesUnset extends EsqlScalarFunction implements AnyNullIsNull, VersionedNamedWriteable {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "TimeSeriesUnset",
        TimeSeriesUnset::new
    );

    /** The first version whose nodes can run {@link TimeSeriesUnset}. */
    public static final TransportVersion ESQL_TIMESERIES_METADATA_UNSET = TransportVersion.fromName("esql_timeseries_metadata_unset");

    private final Expression timeseries;
    private final List<Expression> dimensions;

    public TimeSeriesUnset(Source source, Expression timeseries, List<Expression> dimensions) {
        super(source, Stream.concat(Stream.of(timeseries), dimensions.stream()).toList());
        this.timeseries = timeseries;
        this.dimensions = dimensions;
    }

    private TimeSeriesUnset(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (supportsVersion(out.getTransportVersion()) == false) {
            throw new IOException("time-series metadata operations are not supported by the recipient");
        }
        source().writeTo(out);
        out.writeNamedWriteable(timeseries);
        out.writeNamedWriteableCollection(dimensions);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ESQL_TIMESERIES_METADATA_UNSET;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression timeseries() {
        return timeseries;
    }

    public List<Expression> dimensions() {
        return dimensions;
    }

    /** The field names of the dimensions to unset. */
    public List<String> dimensionNames() {
        return dimensions.stream().map(dimension -> BytesRefs.toString(dimension.fold(FoldContext.small()))).toList();
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        TypeResolution resolution = isString(timeseries, sourceText(), ParamOrdinal.FIRST);
        for (int i = 0; i < dimensions.size() && resolution.resolved(); i++) {
            Expression dimension = dimensions.get(i);
            ParamOrdinal ordinal = ParamOrdinal.fromIndex(i + 1);
            resolution = isString(dimension, sourceText(), ordinal).and(isFoldable(dimension, sourceText(), ordinal));
            if (resolution.resolved() && dimension.fold(FoldContext.small()) == null) {
                resolution = new TypeResolution(
                    format(
                        null,
                        "{} argument of [{}] cannot be null, received [{}]",
                        ordinal.name().toLowerCase(Locale.ROOT),
                        sourceText(),
                        Expressions.name(dimension)
                    )
                );
            }
        }
        return resolution;
    }

    @Override
    public boolean foldable() {
        return timeseries.foldable();
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new TimeSeriesUnsetEvaluator.Factory(
            source(),
            toEvaluator.apply(timeseries),
            TimeSeriesMetadataSupport.unset(dimensionNames())
        );
    }

    @Evaluator(warnExceptions = { IllegalArgumentException.class, IOException.class })
    static BytesRef process(BytesRef timeseries, @Fixed TimeSeriesMetadataSupport.Unset unset) throws IOException {
        return unset.apply(timeseries);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new TimeSeriesUnset(source(), newChildren.getFirst(), newChildren.subList(1, newChildren.size()));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, TimeSeriesUnset::new, timeseries, dimensions);
    }
}
