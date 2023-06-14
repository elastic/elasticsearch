/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;

import java.io.IOException;

public final class BlockOrdinalsReader {
    private final SortedSetDocValues sortedSetDocValues;
    private final Thread creationThread;

    public BlockOrdinalsReader(SortedSetDocValues sortedSetDocValues) {
        this.sortedSetDocValues = sortedSetDocValues;
        this.creationThread = Thread.currentThread();
    }

    public LongBlock readOrdinals(IntVector docs) throws IOException {
        final int positionCount = docs.getPositionCount();
        LongBlock.Builder builder = LongBlock.newBlockBuilder(positionCount);
        for (int p = 0; p < positionCount; p++) {
            int doc = docs.getInt(p);
            if (false == sortedSetDocValues.advanceExact(doc)) {
                builder.appendNull();
                continue;
            }
            int count = sortedSetDocValues.docValueCount();
            if (count == 1) {
                builder.appendLong(sortedSetDocValues.nextOrd());
                continue;
            }
            builder.beginPositionEntry();
            for (int i = 0; i < count; i++) {
                builder.appendLong(sortedSetDocValues.nextOrd());
            }
            builder.endPositionEntry();
        }
        return builder.build();
    }

    public int docID() {
        return sortedSetDocValues.docID();
    }

    /**
     * Checks if the reader can be used to read a range documents starting with the given docID by the current thread.
     */
    public static boolean canReuse(BlockOrdinalsReader reader, int startingDocID) {
        return reader != null && reader.creationThread == Thread.currentThread() && reader.docID() <= startingDocID;
    }
}
