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
        private Map<String, Object> cache;

        public void reset(SearchHit hit, LeafReaderContext context, int docId, IndexSearcher searcher) {
            this.hit = hit;
            this.readerContext = context;
            this.docId = docId;
            this.searcher = searcher;
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


    default void hitsExecute(SearchContext context, SearchHit[] hits) throws IOException {}
}
