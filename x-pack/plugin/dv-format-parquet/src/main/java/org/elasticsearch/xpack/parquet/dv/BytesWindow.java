/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetPositionalReader;

import java.util.Arrays;

/**
 * A {@link ColumnWindow} of byte strings. A page keeps the strings in one array, because a block's {@code BytesRef} is only valid until
 * the block is released. When the ES|QL reader returns a page of dictionary-encoded strings as ordinals, the page keeps them as ordinals
 * into a dictionary of only the values it uses: it copies each distinct value once and no row, it is smaller, and a consumer can work on
 * a distinct value instead of on every row.
 */
final class BytesWindow extends ColumnWindow {

    /**
     * The strings of a page. A plain page has one value for each document in {@code offsets}; a dictionary page has
     * {@code ordinals} for the documents and {@code offsets} for the entries of its own dictionary.
     */
    static final class BytesPage extends DecodedPageCache.ColumnPage {
        private final byte[] bytes;
        private final int[] offsets;
        /** For a dictionary page, the entry of each document, or {@code -1} for a document that has no value; else {@code null}. */
        @Nullable
        private final int[] ordinals;

        BytesPage(int start, int count, @Nullable boolean[] present, byte[] bytes, int[] offsets, @Nullable int[] ordinals) {
            super(start, count, present);
            this.bytes = bytes;
            this.offsets = offsets;
            this.ordinals = ordinals;
        }

        boolean isDictionary() {
            return ordinals != null;
        }

        /** The number of entries of the dictionary of a dictionary page. */
        int dictionarySize() {
            return offsets.length - 1;
        }

        /** The dictionary entry of a document of a dictionary page that has a value. */
        int ordinal(int doc) {
            return ordinals[doc - start()];
        }

        void entry(int entry, BytesRef result) {
            result.bytes = bytes;
            result.offset = offsets[entry];
            result.length = offsets[entry + 1] - offsets[entry];
        }

        void value(int doc, BytesRef result) {
            entry(ordinals == null ? doc - start() : ordinals[doc - start()], result);
        }

        @Override
        long valuesWeight() {
            return bytes.length + 4L * offsets.length + (ordinals == null ? 0 : 4L * ordinals.length);
        }
    }

    private final BytesRef result = new BytesRef();
    /** Maps an entry of the dictionary of a block to its entry in the dictionary of the page, {@code -1} when it is not used yet. */
    private int[] remap = new int[0];

    BytesWindow(
        ParquetPositionalReader reader,
        ColumnWindow.FileLayout layout,
        String field,
        String column,
        long firstRow,
        int maxDoc,
        int windowSize,
        ParquetDocValuesProducer owner,
        DecodedPageCache cache,
        BlockFactory blockFactory
    ) {
        super(reader, layout, field, column, firstRow, maxDoc, windowSize, owner, cache, blockFactory);
    }

    @Override
    protected int scanAfterLoads() {
        return 1;
    }

    @Override
    protected DecodedPageCache.ColumnPage decode(Block block, int blockOffset, int count, int startDoc, @Nullable boolean[] present) {
        if (block.areAllValuesNull()) {
            return new BytesPage(startDoc, count, present, new byte[0], new int[count + 1], null);
        }
        if (block instanceof BytesRefBlock == false) {
            throw new IllegalStateException("cannot serve binary doc values from a [" + block.getClass().getSimpleName() + "]");
        }
        BytesRefBlock strings = (BytesRefBlock) block;
        BytesRefVector vector = strings.asVector();
        if (vector != null && vector.isConstant() && count > 0) {
            // a page of one value, as the reader returns a column that has only one: a dictionary with one entry
            BytesRef value = vector.getBytesRef(0, new BytesRef());
            byte[] bytes = Arrays.copyOfRange(value.bytes, value.offset, value.offset + value.length);
            return new BytesPage(startDoc, count, present, bytes, new int[] { 0, bytes.length }, new int[count]);
        }
        OrdinalBytesRefBlock ordinalBlock = strings.asOrdinals();
        return ordinalBlock != null
            ? decodeOrdinals(ordinalBlock, blockOffset, count, startDoc, present)
            : decodePlain(strings, blockOffset, count, startDoc, present);
    }

    private DecodedPageCache.ColumnPage decodePlain(
        BytesRefBlock strings,
        int blockOffset,
        int count,
        int startDoc,
        @Nullable boolean[] present
    ) {
        int[] offsets = new int[count + 1];
        boolean single = strings.mayHaveMultivaluedFields() == false;
        BytesRef scratch = new BytesRef();
        // one pass for the size, so that the bytes are allocated once and not grown while the long values of a column come in
        int total = 0;
        for (int i = 0; i < count; i++) {
            if (present == null || present[i]) {
                int index = single ? strings.getFirstValueIndex(blockOffset + i) : NumericWindow.singleValueIndex(strings, blockOffset + i);
                int length = strings.getBytesRef(index, scratch).length;
                total += length;
                offsets[i + 1] = length;
            }
        }
        byte[] bytes = new byte[total];
        int used = 0;
        for (int i = 0; i < count; i++) {
            int length = offsets[i + 1];
            if (length > 0) {
                int index = single ? strings.getFirstValueIndex(blockOffset + i) : NumericWindow.singleValueIndex(strings, blockOffset + i);
                BytesRef value = strings.getBytesRef(index, scratch);
                System.arraycopy(value.bytes, value.offset, bytes, used, length);
                used += length;
            }
            offsets[i + 1] = used;
        }
        return new BytesPage(startDoc, count, present, bytes, offsets, null);
    }

    private DecodedPageCache.ColumnPage decodeOrdinals(
        OrdinalBytesRefBlock block,
        int blockOffset,
        int count,
        int startDoc,
        @Nullable boolean[] present
    ) {
        IntBlock ordinalValues = block.getOrdinalsBlock();
        BytesRefVector dictionary = block.getDictionaryVector();
        int dictionarySize = dictionary.getPositionCount();
        if (remap.length < dictionarySize) {
            remap = new int[Math.max(dictionarySize, remap.length * 2)];
            Arrays.fill(remap, -1);
        }
        boolean single = ordinalValues.mayHaveMultivaluedFields() == false;
        int[] ordinals = new int[count];
        int[] usedEntries = new int[Math.min(count, dictionarySize)];
        int used = 0;
        for (int i = 0; i < count; i++) {
            if (present == null || present[i]) {
                int index = single
                    ? ordinalValues.getFirstValueIndex(blockOffset + i)
                    : NumericWindow.singleValueIndex(ordinalValues, blockOffset + i);
                int entry = ordinalValues.getInt(index);
                int id = remap[entry];
                if (id < 0) {
                    id = used++;
                    remap[entry] = id;
                    usedEntries[id] = entry;
                }
                ordinals[i] = id;
            } else {
                ordinals[i] = -1;
            }
        }
        int[] offsets = new int[used + 1];
        BytesRef scratch = new BytesRef();
        int total = 0;
        for (int k = 0; k < used; k++) {
            total += dictionary.getBytesRef(usedEntries[k], scratch).length;
            offsets[k + 1] = total;
        }
        byte[] bytes = new byte[total];
        for (int k = 0; k < used; k++) {
            BytesRef value = dictionary.getBytesRef(usedEntries[k], scratch);
            System.arraycopy(value.bytes, value.offset, bytes, offsets[k], value.length);
            // restore the invariant that every slot of the map is free
            remap[usedEntries[k]] = -1;
        }
        return new BytesPage(startDoc, count, present, bytes, offsets, ordinals);
    }

    /** The page that holds the document that the iterator is on. */
    BytesPage bytesPage() {
        return (BytesPage) page();
    }

    /** The value of a document in the loaded page. The returned reference is reused by the next call. */
    BytesRef value(int doc) {
        ((BytesPage) page()).value(doc, result);
        return result;
    }
}
