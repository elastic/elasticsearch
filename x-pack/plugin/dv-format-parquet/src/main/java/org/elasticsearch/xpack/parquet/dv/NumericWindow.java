/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetPositionalReader;

/** A {@link ColumnWindow} of 32 and 64 bit integers, which Lucene serves as {@code long}. */
final class NumericWindow extends ColumnWindow {

    /** The values of a page, after the conversion of the field. */
    static final class NumericPage extends DecodedPageCache.ColumnPage {
        private final long[] values;

        NumericPage(int start, int count, @Nullable boolean[] present, long[] values) {
            super(start, count, present);
            this.values = values;
        }

        long value(int doc) {
            return values[doc - start()];
        }

        @Override
        long valuesWeight() {
            return 8L * values.length;
        }
    }

    private final Conversion conversion;

    NumericWindow(
        ParquetPositionalReader reader,
        ColumnWindow.FileLayout layout,
        String field,
        String column,
        long firstRow,
        int maxDoc,
        int windowSize,
        ParquetDocValuesProducer owner,
        DecodedPageCache cache,
        BlockFactory blockFactory,
        Conversion conversion
    ) {
        super(reader, layout, field, column, firstRow, maxDoc, windowSize, owner, cache, blockFactory);
        this.conversion = conversion;
    }

    @Override
    protected int scanAfterLoads() {
        return 3;
    }

    @Override
    protected DecodedPageCache.ColumnPage decode(Block block, int blockOffset, int count, int startDoc, @Nullable boolean[] present) {
        long[] values = new long[count];
        // a block without multivalued positions needs no check of each position
        boolean single = block.mayHaveMultivaluedFields() == false;
        if (block instanceof LongBlock longs) {
            for (int i = 0; i < count; i++) {
                if (present == null || present[i]) {
                    int index = single ? longs.getFirstValueIndex(blockOffset + i) : singleValueIndex(longs, blockOffset + i);
                    values[i] = conversion.apply(longs.getLong(index));
                }
            }
        } else if (block instanceof IntBlock ints) {
            for (int i = 0; i < count; i++) {
                if (present == null || present[i]) {
                    int index = single ? ints.getFirstValueIndex(blockOffset + i) : singleValueIndex(ints, blockOffset + i);
                    values[i] = conversion.apply(ints.getInt(index));
                }
            }
        } else if (block.areAllValuesNull() == false) {
            throw new IllegalStateException("cannot serve numeric doc values from a [" + block.getClass().getSimpleName() + "]");
        }
        return new NumericPage(startDoc, count, present, values);
    }

    /** The value of a document in the loaded page. */
    long value(int doc) {
        return ((NumericPage) page()).value(doc);
    }

    static int singleValueIndex(Block block, int position) {
        if (block.getValueCount(position) != 1) {
            throw new IllegalStateException("multi-valued Parquet columns are not supported yet");
        }
        return block.getFirstValueIndex(position);
    }
}
