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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Evaluates {@link TimeSeriesMetadataSet}: the label container with every label but one copied, and that one written with the
 * value at its sorted position; no value removes it.
 */
final class TimeSeriesMetadataSetEvaluator extends AbstractTimeSeriesMetadataFunction.AbstractEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TimeSeriesMetadataSetEvaluator.class);

    private final ExpressionEvaluator labelContainer;
    private final String label;
    private final ExpressionEvaluator value;

    TimeSeriesMetadataSetEvaluator(
        DriverContext driverContext,
        ExpressionEvaluator labelContainer,
        String label,
        ExpressionEvaluator value
    ) {
        super(driverContext, labelContainer, value);
        this.labelContainer = labelContainer;
        this.label = label;
        this.value = value;
    }

    @Override
    protected void eval(
        @Nullable BytesRef container,
        Block[] arguments,
        int position,
        BytesRefBlock.Builder result,
        BreakingBytesRefBuilder scratch
    ) throws IOException {
        List<String> values = written((BytesRefBlock) arguments[0], position);
        scratch.clear();
        try (XContentBuilder builder = jsonBuilder(scratch)) {
            builder.startObject();
            boolean written = values.isEmpty();
            if (container != null) {
                try (XContentParser parser = parser(container)) {
                    while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                        String name = parser.currentName();
                        parser.nextToken();
                        if (written == false && name.compareTo(label) >= 0) {
                            write(builder, label, values);
                            written = true;
                        }
                        if (name.equals(label)) {
                            parser.skipChildren();
                        } else {
                            builder.field(name);
                            builder.copyCurrentStructure(parser);
                        }
                    }
                    end(parser);
                }
            }
            if (written == false) {
                write(builder, label, values);
            }
            builder.endObject();
        }
        result.appendBytesRef(scratch.bytesRefView());
    }

    /** The non-empty values at {@code position}: PromQL has no empty label, so an empty or missing value writes none. */
    private static List<String> written(BytesRefBlock values, int position) {
        if (values.isNull(position)) {
            return List.of();
        }
        BytesRef scratch = new BytesRef();
        int first = values.getFirstValueIndex(position);
        var written = new ArrayList<String>(values.getValueCount(position));
        for (int i = first; i < first + values.getValueCount(position); i++) {
            BytesRef value = values.getBytesRef(i, scratch);
            if (value.length > 0) {
                written.add(value.utf8ToString());
            }
        }
        return written;
    }

    @Override
    protected long shallowRamBytesUsed() {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(label);
    }

    @Override
    public String toString() {
        return describe(labelContainer, label, value);
    }

    private static String describe(Object labelContainer, String label, Object value) {
        return "TimeSeriesMetadataSetEvaluator[labelContainer=" + labelContainer + ", label=" + label + ", value=" + value + "]";
    }

    record Factory(ExpressionEvaluator.Factory labelContainer, String label, ExpressionEvaluator.Factory value)
        implements
            ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new TimeSeriesMetadataSetEvaluator(context, labelContainer.get(context), label, value.get(context));
        }

        @Override
        public String toString() {
            return describe(labelContainer, label, value);
        }
    }
}
