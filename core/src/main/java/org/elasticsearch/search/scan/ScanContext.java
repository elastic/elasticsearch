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

package org.elasticsearch.search.scan;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.MinDocQuery;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The scan context allows to optimize readers we already processed during scanning. We do that by keeping track
 * of the last collected doc ID and only collecting doc IDs that are greater.
 */
public class ScanContext {

    private volatile int docUpTo;

    public ScanCollector collector(SearchContext context) {
        return collector(context.size(), context.trackScores());
    }

    /** Create a {@link ScanCollector} for the given page size. */
    ScanCollector collector(int size, boolean trackScores) {
        return new ScanCollector(size, trackScores);
    }

    /**
     * Wrap the query so that it can skip directly to the right document.
     */
    public Query wrapQuery(Query query) {
        return Queries.filtered(query, new MinDocQuery(docUpTo));
    }

    public class ScanCollector extends SimpleCollector {

        private final List<ScoreDoc> docs;

        private final int size;

        private final boolean trackScores;

        private Scorer scorer;

        private int docBase;

        private ScanCollector(int size, boolean trackScores) {
            this.trackScores = trackScores;
            this.docs = new ArrayList<>(size);
            this.size = size;
        }

        public TopDocs topDocs() {
            return new TopDocs(docs.size(), docs.toArray(new ScoreDoc[docs.size()]), 0f);
        }

        @Override
        public boolean needsScores() {
            return trackScores;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
        }

        @Override
        public void collect(int doc) throws IOException {
            int topLevelDoc = docBase + doc;
            docs.add(new ScoreDoc(topLevelDoc, trackScores ? scorer.score() : 0f));
            // record that we collected up to this document
            assert topLevelDoc >= docUpTo;
            docUpTo = topLevelDoc + 1;
            if (docs.size() >= size) {
                throw new CollectionTerminatedException();
            }
        }

        @Override
        public void doSetNextReader(LeafReaderContext context) throws IOException {
            if (docs.size() >= size || context.docBase + context.reader().maxDoc() <= docUpTo) {
                // no need to collect a new segment, we either already collected enough
                // or the segment is not competitive
                throw new CollectionTerminatedException();
            }
            docBase = context.docBase;
        }
    }

}
