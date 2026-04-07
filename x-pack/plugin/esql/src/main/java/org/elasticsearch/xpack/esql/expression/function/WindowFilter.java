/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class WindowFilter extends EsqlScalarFunction implements TimestampAware, VersionedNamedWriteable {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "WindowFilter",
        WindowFilter::new
    );

    public static final TransportVersion TIME_SERIES_WINDOW_FILTER = TransportVersion.fromName("time_series_window_filter");

    private final Expression window, bucket, timestamp;

    public WindowFilter(Source source, Expression window, Expression bucket, Expression timestamp) {
        super(source, List.of(window, bucket, timestamp));
        this.window = window;
        this.bucket = bucket;
        this.timestamp = timestamp;
    }

    private WindowFilter(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(window);
        out.writeNamedWriteable(bucket);
        out.writeNamedWriteable(timestamp);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TIME_SERIES_WINDOW_FILTER;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isType(window, DataType::isTimeDuration, sourceText(), FIRST, "time duration only").and(
            isType(bucket, dt -> dt == DataType.DATETIME || dt == DataType.DATE_NANOS, sourceText(), SECOND, "date_nanos or datetime")
        ).and(isType(timestamp, dt -> dt == DataType.DATETIME || dt == DataType.DATE_NANOS, sourceText(), THIRD, "date_nanos or datetime"));
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new WindowFilter(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, WindowFilter::new, window, bucket, timestamp);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        Bucket bucketBucket = (Bucket) bucket;
        if (window.foldable() == false) {
            throw new IllegalArgumentException("Window should be foldable");
        }
        Duration foldedWindow = (Duration) window.fold(toEvaluator.foldCtx());
        Rounding.Prepared preparedRounding = bucketBucket.getDateRoundingOrNull(toEvaluator.foldCtx());
        var timestampFactory = toEvaluator.apply(timestamp);
        return new WindowFilterEvaluator.Factory(
            source(),
            foldedWindow.toMillis(),
            preparedRounding,
            driverContext -> new HashMap<>(),
            timestampFactory
        );
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }

    public Expression window() {
        return window;
    }

    protected Expression bucket() {
        return bucket;
    }

    @Evaluator
    static boolean process(
        @Fixed long window,
        @Fixed Rounding.Prepared bucket,
        @Fixed(scope = Fixed.Scope.THREAD_LOCAL) Map<Long, Long> nextTimestamps,
        long timestamp
    ) {
        long bucketStart = bucket.round(timestamp);
        long bucketEnd = nextTimestamps.computeIfAbsent(bucketStart, bucket::nextRoundingValue);
        return timestamp >= bucketEnd - window;
    }
}
