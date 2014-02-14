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

import com.google.common.collect.Maps;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;

import java.util.Map;

/**
 *
 */
public interface FetchSubPhase {

    public static class HitContext {
        private InternalSearchHit hit;
        private IndexReader topLevelReader;
        private int topLevelDocId;
        private AtomicReaderContext readerContext;
        private int docId;
        private FieldsVisitor fieldVisitor;
        private Map<String, Object> cache;
        private IndexSearcher atomicIndexSearcher;

        public void reset(InternalSearchHit hit, AtomicReaderContext context, int docId, IndexReader topLevelReader, int topLevelDocId, FieldsVisitor fieldVisitor) {
            this.hit = hit;
            this.readerContext = context;
            this.docId = docId;
            this.topLevelReader = topLevelReader;
            this.topLevelDocId = topLevelDocId;
            this.fieldVisitor = fieldVisitor;
            this.atomicIndexSearcher = null;
        }

        public InternalSearchHit hit() {
            return hit;
        }

        public AtomicReader reader() {
            return readerContext.reader();
        }

        public AtomicReaderContext readerContext() {
            return readerContext;
        }

        public IndexSearcher searcher() {
            if (atomicIndexSearcher == null) {
                // Use the reader directly otherwise the IndexSearcher assertion will trip because it expects a top level
                // reader context.
                atomicIndexSearcher = new IndexSearcher(readerContext.reader());
            }
            return atomicIndexSearcher;
        }

        public int docId() {
            return docId;
        }

        public IndexReader topLevelReader() {
            return topLevelReader;
        }

        public int topLevelDocId() {
            return topLevelDocId;
        }

        public FieldsVisitor fieldVisitor() {
            return fieldVisitor;
        }

        public Map<String, Object> cache() {
            if (cache == null) {
                cache = Maps.newHashMap();
            }
            return cache;
        }
    }

    Map<String, ? extends SearchParseElement> parseElements();

    boolean hitExecutionNeeded(SearchContext context);

    /**
     * Executes the hit level phase, with a reader and doc id (note, its a low level reader, and the matching doc).
     */
    void hitExecute(SearchContext context, HitContext hitContext) throws ElasticsearchException;

    boolean hitsExecutionNeeded(SearchContext context);

    void hitsExecute(SearchContext context, InternalSearchHit[] hits) throws ElasticsearchException;
}
