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
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Evaluates {@link TimeSeriesMetadataSet}: the metadata with every field but the key copied, and the key written with
 * the value at its path, at its sorted position in every object on the way; no value removes it.
 */
final class TimeSeriesMetadataSetEvaluator extends AbstractTimeSeriesMetadataFunction.AbstractEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TimeSeriesMetadataSetEvaluator.class);

    private final ExpressionEvaluator metadata;
    private final String key;
    private final List<String> path;
    private final ExpressionEvaluator value;

    TimeSeriesMetadataSetEvaluator(
        DriverContext driverContext,
        ExpressionEvaluator metadata,
        String key,
        List<String> path,
        ExpressionEvaluator value
    ) {
        super(driverContext, metadata, value);
        this.metadata = metadata;
        this.key = key;
        this.path = path;
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
            MetadataWriter writer = new MetadataWriter(builder);
            if (container != null) {
                try (XContentParser parser = parser(container)) {
                    copy(parser, "", 0, writer, values);
                    end(parser);
                }
            } else if (values.isEmpty() == false) {
                write(writer, 0, values);
            }
            builder.endObject();
        }
        result.appendBytesRef(scratch.bytesRefView());
    }

    /**
     * Copies the object {@code parser} is in, at {@code prefix}, leaving the key out wherever a field holds it and writing it
     * at its path. {@code depth} is the number of path keys leading to this object, or -1 when the object is off the path.
     */
    private void copy(XContentParser parser, String prefix, int depth, MetadataWriter writer, List<String> values) throws IOException {
        boolean write = depth >= 0 && values.isEmpty() == false;
        while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
            String name = parser.currentName();
            String field = prefix + name;
            XContentParser.Token token = parser.nextToken();
            int order = depth >= 0 ? name.compareTo(path.get(depth)) : -1;
            if (order == 0 && (token == XContentParser.Token.START_OBJECT) != (depth + 1 < path.size())) {
                throw new IllegalArgumentException("time-series dimension [" + field + "] conflicts with key path " + path);
            }
            if (write && order > 0) {
                write(writer, depth, values);
                write = false;
            }
            if (token == XContentParser.Token.START_OBJECT) {
                writer.startObject(name);
                copy(parser, field + ".", order == 0 ? depth + 1 : -1, writer, values);
                writer.endObject();
                write &= order != 0;
            } else if (PromqlLabels.labelName(field).equals(key)) {
                parser.skipChildren();
            } else {
                writer.field(name).copyCurrentStructure(parser);
            }
        }
        if (write) {
            write(writer, depth, values);
        }
    }

    /** Writes the key at the rest of its path, from {@code depth} on. */
    private void write(MetadataWriter writer, int depth, List<String> values) throws IOException {
        for (int i = depth; i < path.size() - 1; i++) {
            writer.startObject(path.get(i));
        }
        writer.field(path.getLast(), values);
        for (int i = depth; i < path.size() - 1; i++) {
            writer.endObject();
        }
    }

    /** The non-empty values at {@code position}: PromQL reads an empty value as an absent key, so an empty or missing value writes none. */
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
        long bytes = BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(key) + RamUsageEstimator.shallowSizeOf(path);
        for (String part : path) {
            bytes += RamUsageEstimator.sizeOf(part);
        }
        return bytes;
    }

    @Override
    public String toString() {
        return describe(metadata, key, path, value);
    }

    private static String describe(Object metadata, String key, List<String> path, Object value) {
        return "TimeSeriesMetadataSetEvaluator[metadata=" + metadata + ", key=" + key + ", path=" + path + ", value=" + value + "]";
    }

    record Factory(ExpressionEvaluator.Factory metadata, String key, List<String> path, ExpressionEvaluator.Factory value)
        implements
            ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new TimeSeriesMetadataSetEvaluator(context, metadata.get(context), key, path, value.get(context));
        }

        @Override
        public String toString() {
            return describe(metadata, key, path, value);
        }
    }
}
