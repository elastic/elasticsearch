/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;

import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public interface SearchHitPhase {

    public static class HitContext {
        private InternalSearchHit hit;
        private IndexReader reader;
        private int docId;
        private Document doc;

        public void reset(InternalSearchHit hit, IndexReader reader, int docId, Document doc) {
            this.hit = hit;
            this.reader = reader;
            this.docId = docId;
            this.doc = doc;
        }

        public InternalSearchHit hit() {
            return hit;
        }

        public IndexReader reader() {
            return reader;
        }

        public int docId() {
            return docId;
        }

        public Document doc() {
            return doc;
        }
    }

    Map<String, ? extends SearchParseElement> parseElements();

    boolean executionNeeded(SearchContext context);

    /**
     * Executes the hit level phase, with a reader and doc id (note, its a low level reader, and the matching doc).
     */
    void execute(SearchContext context, HitContext hitContext) throws ElasticSearchException;
}
