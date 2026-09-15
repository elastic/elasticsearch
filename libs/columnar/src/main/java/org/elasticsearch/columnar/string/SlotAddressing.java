/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.columnar.numeric.LongBlocks;
import org.elasticsearch.columnar.substrate.MonotonicWriter;

import java.io.IOException;

/**
 * Where each document's slots begin, held as a count a document and a base a block.
 *
 * <p>A document's first slot is every count before it added up. Counts are what is kept because they stay
 * small, bounded by what one document holds, and on a column whose documents mostly hold the same number
 * they are the same value over and over, which the stages the counts run take out.
 *
 * <p>A base is the address its block of counts starts at, so reaching a document costs the counts in that
 * block and nothing for the column before it.
 *
 * @param counts how many slots each document holds, one value a document, in rank order
 * @param bases  the first slot address of every block of {@code counts}
 */
public record SlotAddressing(LongBlocks.Metadata counts, MonotonicWriter.Table bases) {

    /** What a column whose slots are in step with its documents holds: a rank is its own value address. */
    public static final SlotAddressing NONE = new SlotAddressing(null, MonotonicWriter.Table.NONE);

    /** How many blocks of counts a column of {@code numDocsWithField} documents has. */
    static long numBlocks(int numDocsWithField, int blockSize) {
        return (numDocsWithField + blockSize - 1L) / blockSize;
    }

    void writeTo(DataOutput out) throws IOException {
        counts.writeTo(out);
        out.writeVLong(bases.dataOffset());
        out.writeVLong(bases.dataLength());
        out.writeVInt(bases.meta().length);
        out.writeBytes(bases.meta(), 0, bases.meta().length);
    }

    static SlotAddressing readFrom(DataInput in) throws IOException {
        final LongBlocks.Metadata counts = LongBlocks.Metadata.readFrom(in);
        final long dataOffset = in.readVLong();
        final long dataLength = in.readVLong();
        final byte[] meta = new byte[in.readVInt()];
        in.readBytes(meta, 0, meta.length);
        return new SlotAddressing(counts, new MonotonicWriter.Table(dataOffset, dataLength, meta));
    }
}
