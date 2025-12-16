/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ToTDigest extends AbstractConvertFunction {

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(DataType.TDIGEST, (source, field) -> field),
        Map.entry(DataType.HISTOGRAM, ToTDigestFromHistogramEvaluator.Factory::new)
    );

    @FunctionInfo(
        returnType = "tdigest",
        description = "Converts an untyped histogram to a TDigest, assuming the values are centroids"
        // TODO: examples
    )
    public ToTDigest(
        Source source,
        @Param(
            name = "field",
            type = { "histogram" },
            description = "The histogram value to be converted")
        Expression field) {
        super(source, field);
    }

    protected ToTDigest(StreamInput in) throws IOException {
        super(in);
    }

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToTDigest",
        ToTDigest::new
    );

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return Map.of();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return null;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return null;
    }

    @Override
    public String getWriteableName() {
        return "";
    }

    @ConvertEvaluator(extraName = "FromHistogram", warnExceptions =  { IllegalArgumentException.class })
    static TDigestHolder fromHistogram(BytesRef in) {
        if (in.length > ByteSizeUnit.MB.toBytes(2)) {
            throw new IllegalArgumentException("Histogram length is greater than 2MB");
        }
        // even though the encoded format is the same, we need to decode here to compute the summary data
        List<Double> centroids = new ArrayList<>();
        List<Long> counts = new ArrayList<>();
        ByteArrayStreamInput streamInput = new ByteArrayStreamInput();
        streamInput.reset(in.bytes, in.offset, in.length);
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        double sum = 0;
        long totalCount = 0;
        try {
            while (streamInput.available() > 0) {
                long count = streamInput.readVLong();
                double value = Double.longBitsToDouble(streamInput.readLong());
                min = Math.min(min, value);
                max = Math.max(max, value);
                sum += value * count;
                totalCount += count;
                centroids.add(value);
                counts.add(count);
            }
            return new TDigestHolder(centroids, counts, min, max, sum, totalCount);
        } catch (IOException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

}
