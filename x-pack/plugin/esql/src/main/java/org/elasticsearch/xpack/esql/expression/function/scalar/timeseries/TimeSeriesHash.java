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
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

/**
 * Builds a 128-bit fingerprint of the complete normalized label set using the ingest TSID hash.
 * The fingerprint is independent of the metadata encoding and the stored TSID layout.
 */
public final class TimeSeriesHash extends TimeSeriesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "TimeSeriesHash",
        TimeSeriesHash::new
    );

    public TimeSeriesHash(Source source, Expression metadata, List<Expression> labels) {
        super(source, Stream.concat(Stream.of(metadata), labels.stream()).toList());
    }

    private TimeSeriesHash(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.TSID_DATA_TYPE;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return TimeSeriesEvaluator.factory(TimeSeriesEvaluator.Operation.HASH, children(), toEvaluator);
    }

    @Override
    protected TypeResolution resolveArguments() {
        return namedValues();
    }

    @Override
    public TimeSeriesHash replaceChildren(List<Expression> children) {
        return new TimeSeriesHash(source(), children.getFirst(), children.subList(1, children.size()));
    }

    @Override
    protected NodeInfo<TimeSeriesHash> info() {
        return NodeInfo.create(this, TimeSeriesHash::new, metadata(), children().subList(1, children().size()));
    }
}
