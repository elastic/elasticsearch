/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * Observes the sorted field write process to produce additional metadata.
 *
 * <p>The lifecycle follows the sorted field write phases:
 * <ol>
 *   <li>{@link #onTerm}: called once per term during the terms dictionary write pass</li>
 *   <li>{@link #prepareForDocs}: called after all terms have been written, before the
 *       document pass begins. Implementations use this to compact term-phase data
 *       and prepare for document tracking.</li>
 *   <li>{@link #onDoc}: called once per document during the numeric ordinals write pass</li>
 *   <li>{@link #flush}: called after both passes are complete. Implementations write
 *       their accumulated metadata to the data and meta outputs.</li>
 * </ol>
 *
 * <p>Implementations are mutable and single-use. A new instance must be created
 * for each field via {@link SortedFieldObserverFactory}.
 *
 * @see SortedFieldObserverFactory
 */
public interface SortedFieldObserver {

    /** No-op observer that ignores all events. */
    SortedFieldObserver NOOP = new SortedFieldObserver() {
        @Override
        public void onTerm(BytesRef term, long ord) {}

        @Override
        public void prepareForDocs() {}

        @Override
        public void onDoc(int docId, long ord) {}

        @Override
        public void flush(IndexOutput data, IndexOutput meta) {}
    };

    /**
     * Called once per term during the terms dictionary write.
     *
     * @param term the term bytes
     * @param ord  the term ordinal
     */
    void onTerm(BytesRef term, long ord);

    /**
     * Called after all terms have been written and before the document pass begins.
     */
    void prepareForDocs();

    /**
     * Called once per document during the numeric ordinals write.
     *
     * @param docId the document ID
     * @param ord   the ordinal value for this document
     */
    void onDoc(int docId, long ord);

    /**
     * Writes accumulated metadata to the segment outputs.
     *
     * @param data the data output stream
     * @param meta the metadata output stream
     */
    void flush(IndexOutput data, IndexOutput meta) throws IOException;
}
