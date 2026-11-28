/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.filter;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;

/**
 * One pass over a shard. Each segment is cut into windows of doc ids. A window starts with every live doc in play, the
 * root of the filter tree rejects some, and the consumer receives what is left. A segment with deleted docs pays one
 * liveness check per doc in the window, because {@code Bits} has no bulk form.
 */
public final class DocFilterPass {
    private DocFilterPass() {}

    /** Receives the docs of a window that passed. Bit {@code i} is doc {@code window.from() + i}. The bit set is reused for the next window. */
    @FunctionalInterface
    public interface WindowConsumer {
        void accept(DocWindow window, FixedBitSet passing) throws IOException;
    }

    public static void run(IndexReader reader, DocFilter root, int windowSize, WindowConsumer consumer) throws IOException {
        FixedBitSet inPlay = new FixedBitSet(windowSize);
        for (LeafReaderContext leaf : reader.leaves()) {
            Bits live = leaf.reader().getLiveDocs();
            int maxDoc = leaf.reader().maxDoc();
            for (int from = 0; from < maxDoc; from += windowSize) {
                DocWindow window = new DocWindow(leaf, from, Math.min(maxDoc, from + windowSize));
                putLiveDocsInPlay(window, live, inPlay);
                root.filter(window, inPlay);
                consumer.accept(window, inPlay);
            }
        }
    }

    /** Puts every doc of {@code window} that {@code live} accepts in play, and nothing past the window. */
    static void putLiveDocsInPlay(DocWindow window, Bits live, FixedBitSet inPlay) {
        inPlay.clear();
        inPlay.set(0, window.length());
        if (live != null) {
            for (int bit = 0; bit < window.length(); bit++) {
                if (live.get(window.from() + bit) == false) {
                    inPlay.clear(bit);
                }
            }
        }
    }

    /** The number of docs that pass the filter, the way {@code COUNT(*)} sees them. */
    public static long count(IndexReader reader, DocFilter root, int windowSize) throws IOException {
        long[] count = new long[1];
        run(reader, root, windowSize, (window, passing) -> count[0] += passing.cardinality());
        return count[0];
    }
}
