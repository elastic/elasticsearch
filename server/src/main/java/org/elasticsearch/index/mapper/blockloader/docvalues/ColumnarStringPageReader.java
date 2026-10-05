/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.string.StringBlockSink;
import org.elasticsearch.columnar.string.StringColumnSource;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.BreakerPageBudget;

import java.io.IOException;

/**
 * Reads a page of a string column into a block. Shared by the readers of a columnar keyword's two framings, a payload
 * and the bare bytes of a single value, which sit on the same column.
 */
final class ColumnarStringPageReader implements Releasable {

    /** The ranks a page is asked for, held per reader rather than allocated per page. */
    private int[] wanted = new int[0];
    private final PageSink sink = new PageSink();
    /**
     * Charged before the column grows its page storage, so there is no room for a page the breaker would
     * refuse, and released with this reader because the storage lives as long as the reader does. The
     * storage is reused, so it settles after the first page and charges nothing again.
     */
    private final BreakerPageBudget budget;

    ColumnarStringPageReader(CircuitBreaker breaker) {
        this.budget = new BreakerPageBudget(breaker);
    }

    /**
     * The block for {@code docs} from {@code offset} on, or null when the column declines the page.
     *
     * <p>Where the column keeps a dictionary this is the shape it already stores: the page comes back as ordinals
     * into its own distinct values, each resolved once however many documents name it, and is handed over without
     * a lookup per value or a remapping. Where it does not, the page still comes back a page at a time, and a run
     * of equal values is copied once.
     *
     * <p>A document the column has no value for arrives holding none, which the block reads as a null.
     *
     * <p>A document may repeat within a page, which a lookup or a top-n asks for. The iterator a page resolves its
     * ranks through is only required not to be moved backwards, so asking it twice for the same document is a
     * position it already holds.
     */
    @Nullable
    BlockLoader.Block read(StringColumnSource columnar, BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset)
        throws IOException {
        final int count = docs.count() - offset;
        if (count <= 0) {
            return null;
        }
        if (wanted.length < count) {
            wanted = new int[ArrayUtil.oversize(count, Integer.BYTES)];
        }
        for (int i = 0; i < count; i++) {
            wanted[i] = docs.get(offset + i);
        }
        return columnar.reader().readBlock(wanted, 0, count, sink.forPage(factory), budget) ? sink.block : null;
    }

    @Override
    public void close() {
        budget.close();
    }

    /** Turns a page of a string column into a block, in whichever of the shapes the page arrived. */
    private static final class PageSink implements StringBlockSink {

        private BlockLoader.BlockFactory factory;
        private BlockLoader.Block block;

        /** Points the sink at the factory the next page builds with; held per reader, not per page. */
        private PageSink forPage(BlockLoader.BlockFactory factory) {
            this.factory = factory;
            this.block = null;
            return this;
        }

        @Override
        public void appendOrdinals(int[] ordinals, int valueCount, int[] valueCounts, int docCount, BytesRef[] dictionary, int size) {
            if (valueCount == 0) {
                // Every document in the page holds nothing, which a filter selecting documents whose only slot is
                // null gives systematically. A block of nulls says it without an ordinal or a dictionary entry.
                block = factory.constantNulls(docCount);
                return;
            }
            if (size == 1 && valueCounts == null) {
                // Every document in the page holds the same value, which is what a column in term order is made of
                // and what an index sort on the field produces. Saying so is a block that costs nothing to build
                // and nothing to read: no ordinal is written and no consumer walks one.
                block = factory.constantBytes(BytesRef.deepCopyOf(dictionary[0]), docCount);
                return;
            }
            block = factory.buildOrdinalBytesRefDirect(ordinals, valueCount, valueCounts, docCount, dictionary, size);
        }

        @Override
        public void appendValues(BytesRef[] values, int valueCount, int[] valueCounts, int docCount) {
            if (valueCount == 0) {
                block = factory.constantNulls(docCount);
                return;
            }
            // Sized in positions, which is what the block holds - a multi-valued page has more values than those.
            try (BlockLoader.BytesRefBuilder builder = factory.bytesRefs(docCount)) {
                int at = 0;
                for (int doc = 0; doc < docCount; doc++) {
                    final int held = valueCounts == null ? 1 : valueCounts[doc];
                    if (held == 0) {
                        builder.appendNull();
                    } else if (held == 1) {
                        builder.appendBytesRef(values[at++]);
                    } else {
                        builder.beginPositionEntry();
                        for (int i = 0; i < held; i++) {
                            builder.appendBytesRef(values[at++]);
                        }
                        builder.endPositionEntry();
                    }
                }
                assert at == valueCount : "read " + at + " values, was given " + valueCount;
                block = builder.build();
            }
        }
    }
}
