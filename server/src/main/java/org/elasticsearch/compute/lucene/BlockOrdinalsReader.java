/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongArrayBlock;

import java.io.IOException;

public final class BlockOrdinalsReader {
    private final SortedSetDocValues sortedSetDocValues;
    private final Thread creationThread;

    public BlockOrdinalsReader(SortedSetDocValues sortedSetDocValues) {
        this.sortedSetDocValues = sortedSetDocValues;
        this.creationThread = Thread.currentThread();
    }

    public Block readOrdinals(Block docs) throws IOException {
        final int positionCount = docs.getPositionCount();
        final long[] ordinals = new long[positionCount];
        int lastDoc = -1;
        for (int i = 0; i < docs.getPositionCount(); i++) {
            int doc = docs.getInt(i);
            // docs within same block must be in order
            if (lastDoc >= doc) {
                throw new IllegalStateException("docs within same block must be in order");
            }
            if (sortedSetDocValues.advanceExact(doc) == false) {
                throw new IllegalStateException("sparse fields not supported for now, could not read doc [" + doc + "]");
            }
            if (sortedSetDocValues.docValueCount() != 1) {
                throw new IllegalStateException("multi-values not supported for now, could not read doc [" + doc + "]");
            }
            ordinals[i] = sortedSetDocValues.nextOrd();
            lastDoc = doc;
        }
        return new LongArrayBlock(ordinals, positionCount);
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
