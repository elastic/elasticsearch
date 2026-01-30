/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.comparators;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.search.AbstractDocIdSetIterator;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.index.IndexVersion;

import java.io.IOException;

public class XSkipBlockRangeIterator extends AbstractDocIdSetIterator {

    static {
        if (IndexVersion.current().luceneVersion().onOrAfter(org.apache.lucene.util.Version.fromBits(10, 4, 0))) {
            throw new IllegalStateException("Remove this class after upgrading to lucene 10.4");
        }
    }

    private final DocValuesSkipper skipper;
    private final long minValue;
    private final long maxValue;

    public XSkipBlockRangeIterator(DocValuesSkipper skipper, long minValue, long maxValue) {
        this.skipper = skipper;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    @Override
    public int nextDoc() throws IOException {
        return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
        if (target <= skipper.maxDocID(0)) {
            // within current block
            if (doc > -1) {
                // already positioned, so we've checked bounds and know that we're in a matching block
                return doc = target;
            } else {
                // first call, the skipper might already be positioned so ask it to find the next
                // matching block (which could be the current one)
                skipper.advance(minValue, maxValue);
                return doc = Math.max(target, skipper.minDocID(0));
            }
        }
        // Advance to target
        skipper.advance(target);

        // Find the next matching block (could be the current block)
        skipper.advance(minValue, maxValue);

        return doc = Math.max(target, skipper.minDocID(0));
    }

    @Override
    public long cost() {
        return DocIdSetIterator.NO_MORE_DOCS;
    }

    @Override
    public int docIDRunEnd() throws IOException {
        int maxDoc = skipper.maxDocID(0);
        int nextLevel = 1;
        while (nextLevel < skipper.numLevels() && skipper.minValue(nextLevel) < maxValue && skipper.maxValue(nextLevel) > minValue) {
            maxDoc = skipper.maxDocID(nextLevel);
            nextLevel++;
        }
        return maxDoc + 1;
    }

    @Override
    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
        while (doc < upTo) {
            int end = Math.min(upTo, docIDRunEnd());
            bitSet.set(doc - offset, end - offset);
            advance(end);
        }
    }
}
