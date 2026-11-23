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

/** Evaluates {@link TimeSeriesMetadataPath}: the values of one label, null when the label is absent. */
final class TimeSeriesMetadataPathEvaluator extends AbstractTimeSeriesMetadataFunction.AbstractEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TimeSeriesMetadataPathEvaluator.class);

    private final ExpressionEvaluator labelContainer;
    private final String label;

    TimeSeriesMetadataPathEvaluator(DriverContext driverContext, ExpressionEvaluator labelContainer, String label) {
        super(driverContext, labelContainer);
        this.labelContainer = labelContainer;
        this.label = label;
    }

    @Override
    protected void eval(
        @Nullable BytesRef labels,
        Block[] arguments,
        int position,
        BytesRefBlock.Builder result,
        BreakingBytesRefBuilder scratch
    ) throws IOException {
        if (labels != null) {
            try (XContentParser parser = parser(labels)) {
                if (find(parser, "", label)) {
                    if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                        result.beginPositionEntry();
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            result.appendBytesRef(new BytesRef(parser.text()));
                        }
                        result.endPositionEntry();
                        return;
                    }
                    if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                        result.appendBytesRef(new BytesRef(parser.text()));
                        return;
                    }
                }
            }
        }
        result.appendNull();
    }

    @Override
    protected long shallowRamBytesUsed() {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(label);
    }

    @Override
    public String toString() {
        return "TimeSeriesMetadataPathEvaluator[labelContainer=" + labelContainer + ", label=" + label + "]";
    }

    record Factory(ExpressionEvaluator.Factory labelContainer, String label) implements ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new TimeSeriesMetadataPathEvaluator(context, labelContainer.get(context), label);
        }

        @Override
        public String toString() {
            return "TimeSeriesMetadataPathEvaluator[labelContainer=" + labelContainer + ", label=" + label + "]";
        }
    }
}
