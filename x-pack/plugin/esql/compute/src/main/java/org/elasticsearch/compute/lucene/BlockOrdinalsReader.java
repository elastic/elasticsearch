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
        for (int i = 0; i < positionCount; i++) {
            int doc = docs.getInt(i);
            if (sortedSetDocValues.advanceExact(doc)) {
                builder.appendLong(sortedSetDocValues.nextOrd());
            } else {
                builder.appendNull();
            }
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
