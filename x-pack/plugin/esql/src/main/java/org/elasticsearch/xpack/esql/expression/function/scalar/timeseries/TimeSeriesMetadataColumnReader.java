/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataBlockLoader.Dimension;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataBlockLoader.Values;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.util.List;

/**
 * Reads the label containers of a segment's documents, column at a time: the values of each dimension, then a label container
 * per document of them (see {@link TimeSeriesMetadataBlockLoader}).
 */
final class TimeSeriesMetadataColumnReader implements BlockLoader.ColumnAtATimeReader {
    /** The dimensions, in label name order. */
    private final List<Dimension> dimensions;
    private final List<BytesRef> labels;
    private final BlockLoader.ColumnAtATimeReader[] readers;
    private final TimeSeriesMetadataEncoding.Writer writer;

    TimeSeriesMetadataColumnReader(
        CircuitBreaker breaker,
        List<Dimension> dimensions,
        List<BytesRef> labels,
        List<IOFunction<CircuitBreaker, BlockLoader.ColumnAtATimeReader>> factories
    ) throws IOException {
        this.dimensions = dimensions;
        this.labels = labels;
        BlockLoader.ColumnAtATimeReader[] readers = new BlockLoader.ColumnAtATimeReader[factories.size()];
        TimeSeriesMetadataEncoding.Writer writer = null;
        boolean made = false;
        try {
            for (int i = 0; i < readers.length; i++) {
                readers[i] = factories.get(i).apply(breaker);
            }
            writer = new TimeSeriesMetadataEncoding.Writer(breaker);
            made = true;
        } finally {
            if (made == false) {
                Releasables.closeExpectNoException(Releasables.wrap(readers), writer);
            }
        }
        this.readers = readers;
        this.writer = writer;
    }

    @Override
    public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
        throws IOException {
        Block[] values = new Block[readers.length];
        try {
            for (int i = 0; i < readers.length; i++) {
                // A dimension is not loaded with the guarantee the label container is: any of them may be missing. Blocks are
                // the compute engine's, as its block factory is the one that loads them.
                values[i] = (Block) readers[i].read(factory, docs, offset, false);
            }
            int count = docs.count() - offset;
            try (BlockLoader.BytesRefBuilder containers = factory.bytesRefs(count)) {
                BytesRef scratch = new BytesRef();
                for (int p = 0; p < count; p++) {
                    writer.clear();
                    boolean labelled = false;
                    for (int i = 0; i < values.length; i++) {
                        boolean same = i > 0 && labels.get(i).equals(labels.get(i - 1));
                        boolean written = write(i, values[i], p, scratch);
                        if (same && labelled && written) {
                            throw new IllegalArgumentException(
                                "multiple dimension fields resolve to time-series label [" + dimensions.get(i).label() + "]"
                            );
                        }
                        labelled = same ? labelled || written : written;
                    }
                    containers.appendBytesRef(writer.build());
                }
                return containers.build();
            }
        } finally {
            Releasables.closeExpectNoException(values);
        }
    }

    /** Writes the label of dimension {@code i}, holding its values at {@code position}: whether it has any. */
    private boolean write(int i, Block block, int position, BytesRef scratch) {
        if (block.isNull(position)) {
            return false;
        }
        writer.beginLabel(labels.get(i));
        Values kind = dimensions.get(i).values();
        int first = block.getFirstValueIndex(position);
        for (int v = first; v < first + block.getValueCount(position); v++) {
            writer.value(switch (block) {
                case BytesRefBlock bytes when kind == Values.IP -> new BytesRef(
                    EsqlDataTypeConverter.ipToString(bytes.getBytesRef(v, scratch))
                );
                case BytesRefBlock bytes -> bytes.getBytesRef(v, scratch);
                case LongBlock longs when kind == Values.UNSIGNED_LONG -> EsqlDataTypeConverter.unsignedLongToString(longs.getLong(v));
                case LongBlock longs -> new BytesRef(Long.toString(longs.getLong(v)));
                case IntBlock ints -> new BytesRef(Integer.toString(ints.getInt(v)));
                case BooleanBlock booleans -> new BytesRef(Boolean.toString(booleans.getBoolean(v)));
                case DoubleBlock doubles -> new BytesRef(Double.toString(doubles.getDouble(v)));
                default -> throw new IllegalStateException(
                    "dimension [" + dimensions.get(i).label() + "] loads as [" + block.elementType() + "]"
                );
            });
        }
        return writer.endLabel();
    }

    @Override
    public boolean canReuse(int startingDocID) {
        for (BlockLoader.ColumnAtATimeReader reader : readers) {
            if (reader.canReuse(startingDocID) == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(Releasables.wrap(readers), writer);
    }

    @Override
    public String toString() {
        return "TimeSeriesMetadata" + labels.stream().map(BytesRef::utf8ToString).toList();
    }
}
