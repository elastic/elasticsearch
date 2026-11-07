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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.util.TimeSeriesMetadata;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper.ToEvaluator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Evaluates reads and removals of labels in typed time-series metadata. Labels are folded once; the metadata is decoded per
 * row, with its scratch accounted to the circuit breaker.
 */
public final class TimeSeriesEvaluator implements ExpressionEvaluator {
    enum Operation {
        GET,
        UNSET
    }

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TimeSeriesEvaluator.class);

    private final Operation operation;
    private final ExpressionEvaluator metadata;
    private final List<String> names;
    private final DriverContext context;

    private TimeSeriesEvaluator(Operation operation, ExpressionEvaluator metadata, List<String> names, DriverContext context) {
        this.operation = operation;
        this.metadata = metadata;
        this.names = names;
        this.context = context;
    }

    static Factory factory(Operation operation, List<Expression> arguments, ToEvaluator toEvaluator) {
        String[] names = new String[arguments.size() - 1];
        for (int i = 0; i < names.length; i++) {
            names[i] = ((BytesRef) arguments.get(i + 1).fold(toEvaluator.foldCtx())).utf8ToString();
        }
        return new Factory(operation, toEvaluator.apply(arguments.getFirst()), List.of(names));
    }

    @Override
    public Block eval(Page page) {
        try (Block block = metadata.eval(page)) {
            return eval(page.getPositionCount(), block);
        }
    }

    private Block eval(int positions, Block block) {
        BytesRef scratch = new BytesRef();
        try (
            BytesRefBlock.Builder result = context.blockFactory().newBytesRefBlockBuilder(positions);
            BreakingBytesRefBuilder buffer = new BreakingBytesRefBuilder(context.breaker(), "time_series_metadata")
        ) {
            for (int p = 0; p < positions; p++) {
                long reservation = scratchBytes(block, p, scratch);
                context.breaker().addEstimateBytesAndMaybeBreak(reservation, "time_series_metadata");
                try {
                    BytesRef encoded = block.isNull(p) ? null : ((BytesRefBlock) block).getBytesRef(block.getFirstValueIndex(p), scratch);
                    TimeSeriesMetadata labels = TimeSeriesMetadata.read(encoded);
                    switch (operation) {
                        case GET -> {
                            List<String> values = labels.get(names.getFirst());
                            if (values.isEmpty()) {
                                result.appendNull();
                            } else if (values.size() == 1) {
                                result.appendBytesRef(new BytesRef(values.getFirst()));
                            } else {
                                result.beginPositionEntry();
                                for (String value : values) {
                                    result.appendBytesRef(new BytesRef(value));
                                }
                                result.endPositionEntry();
                            }
                        }
                        case UNSET -> result.appendBytesRef(write(labels.unset(names), buffer));
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException("Invalid time-series metadata", e);
                } finally {
                    context.breaker().addWithoutBreaking(-reservation);
                }
            }
            return result.build();
        }
    }

    /**
     * Reserve scratch before decoding. The conservative multiplier covers decoded strings, parser storage and the
     * transformed maps, including short labels with large object overhead. Only one row's scratch is live; the output builder
     * and blocks account their own storage.
     */
    private long scratchBytes(Block block, int position, BytesRef scratch) {
        long bytes = 0;
        if (block.isNull(position) == false) {
            if (block.getValueCount(position) != 1) {
                throw new IllegalArgumentException("Time-series metadata must be single-valued");
            }
            bytes += 32 + ((BytesRefBlock) block).getBytesRef(block.getFirstValueIndex(position), scratch).length;
        }
        for (String name : names) {
            bytes += 2L * name.length() + 32;
        }
        return 4096 + TimeSeriesMetadata.encodingScratchBytes() + 64 * bytes;
    }

    private static BytesRef write(TimeSeriesMetadata metadata, BreakingBytesRefBuilder buffer) throws IOException {
        buffer.clear();
        metadata.writeTo(new TimeSeriesMetadataOutputStream(buffer));
        return buffer.bytesRefView();
    }

    @Override
    public long baseRamBytesUsed() {
        long bytes = BASE_RAM_BYTES_USED + RamUsageEstimator.shallowSizeOf(names) + metadata.baseRamBytesUsed();
        for (String name : names) {
            bytes += RamUsageEstimator.sizeOf(name);
        }
        return bytes;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(metadata);
    }

    @Override
    public String toString() {
        return "TimeSeriesEvaluator[operation=" + operation + ", names=" + names + ", metadata=" + metadata + "]";
    }

    record Factory(Operation operation, ExpressionEvaluator.Factory metadata, List<String> names) implements ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new TimeSeriesEvaluator(operation, metadata.get(context), names, context);
        }

        @Override
        public String toString() {
            return "TimeSeriesEvaluator[operation=" + operation + ", names=" + names + ", metadata=" + metadata + "]";
        }
    }
}
