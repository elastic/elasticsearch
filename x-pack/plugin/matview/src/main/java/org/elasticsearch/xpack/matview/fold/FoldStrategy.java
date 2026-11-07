/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.matview.fold;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.util.Bits;
import org.elasticsearch.core.Nullable;

import java.io.Closeable;
import java.io.IOException;

/**
 * How the documents of a merge are folded. A strategy creates one {@link MergeFold} per merge, which wraps the
 * merge's input readers one at a time and tells each wrapped reader which documents it hides and what value a
 * surviving document carries for a reduced field.
 */
public interface FoldStrategy {

    MergeFold newMergeFold(FoldSpec spec, @Nullable FoldGuard guard);

    /** Per-merge state; readers are wrapped in merge order and closed when the merge finishes. */
    interface MergeFold extends Closeable {
        CodecReader wrap(CodecReader reader) throws IOException;
    }

    /** What one wrapped reader asks its merge fold. */
    interface ReaderFold {
        /** Live docs with the folded documents cleared, or {@code null} when this reader hides nothing. */
        @Nullable
        Bits foldedLive();

        int numDocs();

        /**
         * Value a surviving document carries for a reduced field: the group's reduced value for a representative,
         * {@code raw} for a document that does not fold.
         */
        long reducedValue(int doc, int field, long raw) throws IOException;
    }
}
