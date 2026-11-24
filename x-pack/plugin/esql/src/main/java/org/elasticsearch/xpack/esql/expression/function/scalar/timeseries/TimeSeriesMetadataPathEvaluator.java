/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Evaluates {@link TimeSeriesMetadataPath}: the values of one key, null when the key is absent. */
final class TimeSeriesMetadataPathEvaluator extends AbstractTimeSeriesMetadataFunction.AbstractEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TimeSeriesMetadataPathEvaluator.class);

    private final ExpressionEvaluator metadata;
    private final String key;

    TimeSeriesMetadataPathEvaluator(DriverContext driverContext, ExpressionEvaluator metadata, String key) {
        super(driverContext, metadata);
        this.metadata = metadata;
        this.key = key;
    }

    @Override
    protected void eval(
        @Nullable BytesRef container,
        Block[] arguments,
        int position,
        BytesRefBlock.Builder result,
        BreakingBytesRefBuilder scratch
    ) throws IOException {
        if (container != null) {
            try (XContentParser parser = parser(container)) {
                if (find(parser, "", key)) {
                    // A null or empty value reads as absent, as TS_SET writes an absent label.
                    if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                        List<BytesRef> values = new ArrayList<>();
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                                String value = parser.text();
                                if (value.isEmpty() == false) {
                                    values.add(new BytesRef(value));
                                }
                            }
                        }
                        if (values.size() == 1) {
                            result.appendBytesRef(values.getFirst());
                            return;
                        }
                        if (values.size() > 1) {
                            result.beginPositionEntry();
                            for (BytesRef value : values) {
                                result.appendBytesRef(value);
                            }
                            result.endPositionEntry();
                            return;
                        }
                    } else if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                        String value = parser.text();
                        if (value.isEmpty() == false) {
                            result.appendBytesRef(new BytesRef(value));
                            return;
                        }
                    }
                }
            }
        }
        result.appendNull();
    }

    @Override
    protected long shallowRamBytesUsed() {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(key);
    }

    @Override
    public String toString() {
        return "TimeSeriesMetadataPathEvaluator[metadata=" + metadata + ", key=" + key + "]";
    }

    record Factory(ExpressionEvaluator.Factory metadata, String key) implements ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new TimeSeriesMetadataPathEvaluator(context, metadata.get(context), key);
        }

        @Override
        public String toString() {
            return "TimeSeriesMetadataPathEvaluator[metadata=" + metadata + ", key=" + key + "]";
        }
    }
}
