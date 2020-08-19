/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.fetch;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Sub phase within the fetch phase used to fetch things *about* the documents like highlighting or matched queries.
 */
public interface FetchSubPhase {

    class HitContext {
        private SearchHit hit;
        private IndexSearcher searcher;
        private LeafReaderContext readerContext;
        private int docId;
        private final SourceLookup sourceLookup = new SourceLookup();
        private Map<String, Object> cache;

        public void reset(SearchHit hit, LeafReaderContext context, int docId, IndexSearcher searcher) {
            this.hit = hit;
            this.readerContext = context;
            this.docId = docId;
            this.searcher = searcher;
            this.sourceLookup.setSegmentAndDocument(context, docId);
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

        public IndexReader topLevelReader() {
            return searcher.getIndexReader();
        }

        public Map<String, Object> cache() {
            if (cache == null) {
                cache = new HashMap<>();
            }
            return cache;
        }
    }

    /**
     * Executes the hit level phase, with a reader and doc id (note, its a low level reader, and the matching doc).
     */
    default void hitExecute(SearchContext context, HitContext hitContext) throws IOException {}

    /**
     * Executes the hits level phase (note, hits are sorted by doc ids).
     */
    default void hitsExecute(SearchContext context, SearchHit[] hits) throws IOException {}
}
