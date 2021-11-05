/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.fetch;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;

/**
 * Sub phase within the fetch phase used to fetch things *about* the documents like highlighting or matched queries.
 */
public interface FetchSubPhase {

    class HitContext {
        private final SearchHit hit;
        private final LeafReaderContext readerContext;
        private final int docId;
        private SourceLookup sourceLookup;

        public HitContext(SearchHit hit, LeafReaderContext context, int docId) {
            this.hit = hit;
            this.readerContext = context;
            this.docId = docId;
            this.sourceLookup = new SourceLookup();
            sourceLookup.setSegmentAndDocument(context, docId);
        }

        public SearchHit hit() {
            return hit;
        }

        public LeafReader reader() {
            return readerContext.reader();
        }

        public LeafReaderContext readerContext() {
            return readerContext;
        }

        /**
         * @return the docId of this hit relative to the leaf reader context
         */
        public int docId() {
            return docId;
        }

        /**
         * This lookup provides access to the source for the given hit document. Note
         * that it should always be set to the correct doc ID and {@link LeafReaderContext}.
         *
         * In most cases, the hit document's source is loaded eagerly at the start of the
         * {@link FetchPhase}. This lookup will contain the preloaded source.
         */
        public SourceLookup sourceLookup() {
            return sourceLookup;
        }

        public void setSourceLookup(SourceLookup sourceLookup) {
            this.sourceLookup = sourceLookup;
        }

        public IndexReader topLevelReader() {
            return ReaderUtil.getTopLevelContext(readerContext).reader();
        }
    }

    /**
     * Returns a {@link FetchSubPhaseProcessor} for this sub phase.
     *
     * If nothing should be executed for the provided {@code FetchContext}, then the
     * implementation should return {@code null}
     */
    FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) throws IOException;
}
