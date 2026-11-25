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
import java.util.List;
import java.util.Set;

/**
 * Evaluates {@link TimeSeriesMetadataFilter}: the metadata of the listed keys, or of every other key when
 * {@code exclude}, in their order.
 */
final class TimeSeriesMetadataFilterEvaluator extends AbstractTimeSeriesMetadataFunction.AbstractEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TimeSeriesMetadataFilterEvaluator.class);

    private final ExpressionEvaluator metadata;
    private final List<String> keys;
    private final Set<String> keySet;
    private final boolean exclude;
    private final Set<String> roots;

    TimeSeriesMetadataFilterEvaluator(
        DriverContext driverContext,
        ExpressionEvaluator metadata,
        List<String> keys,
        boolean exclude,
        Set<String> roots
    ) {
        super(driverContext, metadata);
        this.metadata = metadata;
        this.keys = keys;
        this.keySet = Set.copyOf(keys);
        this.exclude = exclude;
        this.roots = roots;
    }

    @Override
    protected void eval(
        @Nullable BytesRef container,
        Block[] arguments,
        int position,
        BytesRefBlock.Builder result,
        BreakingBytesRefBuilder scratch
    ) throws IOException {
        scratch.clear();
        try (XContentBuilder builder = jsonBuilder(scratch)) {
            builder.startObject();
            if (container != null) {
                try (XContentParser parser = parser(container)) {
                    copy(parser, "", "", new MetadataWriter(builder));
                    end(parser);
                }
            }
            builder.endObject();
        }
        result.appendBytesRef(scratch.bytesRefView());
    }

    /**
     * Copies the object {@code parser} is in, at path {@code prefix} with the label name prefix {@code label}, with the keys
     * this filter keeps.
     */
    private void copy(XContentParser parser, String prefix, String label, MetadataWriter writer) throws IOException {
        while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
            String name = parser.currentName();
            if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
                String object = prefix + name;
                writer.startObject(name);
                copy(parser, object + ".", labelPrefix(object, label + name, roots), writer);
                writer.endObject();
            } else if (keySet.contains(label + name) != exclude) {
                writer.field(name).copyCurrentStructure(parser);
            } else {
                parser.skipChildren();
            }
        }
    }

    @Override
    protected long shallowRamBytesUsed() {
        long bytes = BASE_RAM_BYTES_USED + RamUsageEstimator.shallowSizeOf(keys);
        for (String key : keys) {
            bytes += RamUsageEstimator.sizeOf(key);
        }
        return bytes;
    }

    @Override
    public String toString() {
        return describe(metadata, keys, exclude);
    }

    private static String describe(Object metadata, List<String> keys, boolean exclude) {
        return "TimeSeriesMetadataFilterEvaluator[metadata=" + metadata + ", keys=" + keys + ", exclude=" + exclude + "]";
    }

    record Factory(ExpressionEvaluator.Factory metadata, List<String> keys, boolean exclude, Set<String> roots)
        implements
            ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new TimeSeriesMetadataFilterEvaluator(context, metadata.get(context), keys, exclude, roots);
        }

        @Override
        public String toString() {
            return describe(metadata, keys, exclude);
        }
    }
}
