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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.search.SearchHit;
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
        private LeafReaderContext readerContext;
        private int docId;
        private Map<String, Object> cache;
        private IndexSearcher atomicIndexSearcher;

        public void reset(InternalSearchHit hit, LeafReaderContext context, int docId, IndexReader topLevelReader) {
            this.hit = hit;
            this.readerContext = context;
            this.docId = docId;
            this.topLevelReader = topLevelReader;
            this.atomicIndexSearcher = null;
        }

        public InternalSearchHit hit() {
            return hit;
        }

        public LeafReader reader() {
            return readerContext.reader();
        }

        public LeafReaderContext readerContext() {
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

        public Map<String, Object> cache() {
            if (cache == null) {
                cache = Maps.newHashMap();
            }
            return cache;
        }

        public String getSourcePath(String sourcePath) {
            SearchHit.NestedIdentity nested = hit().getNestedIdentity();
            if (nested != null) {
                // in case of nested we need to figure out what is the _source field from the perspective
                // of the nested hit it self. The nested _source is isolated and the root and potentially parent objects
                // are gone
                StringBuilder nestedPath = new StringBuilder();
                for (; nested != null; nested = nested.getChild()) {
                    nestedPath.append(nested.getField());
                }

                assert sourcePath.startsWith(nestedPath.toString());
                int startIndex = nestedPath.length() + 1; // the path until the deepest nested object + '.'
                return sourcePath.substring(startIndex);
            } else {
                return sourcePath;
            }
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
