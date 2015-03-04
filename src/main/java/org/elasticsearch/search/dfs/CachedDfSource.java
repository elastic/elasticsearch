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

package org.elasticsearch.search.dfs;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.Similarity;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class CachedDfSource extends IndexSearcher {

    private final AggregatedDfs aggregatedDfs;

    private final int maxDoc;

    public CachedDfSource(IndexReader reader, AggregatedDfs aggregatedDfs, Similarity similarity) throws IOException {
        super(reader);
        this.aggregatedDfs = aggregatedDfs;
        setSimilarity(similarity);
        if (aggregatedDfs.maxDoc() > Integer.MAX_VALUE) {
            maxDoc = Integer.MAX_VALUE;
        } else {
            maxDoc = (int) aggregatedDfs.maxDoc();
        }
    }


    @Override
    public TermStatistics termStatistics(Term term, TermContext context) throws IOException {
        TermStatistics termStatistics = aggregatedDfs.termStatistics().get(term);
        if (termStatistics == null) {
            // we don't have stats for this - this might be a must_not clauses etc. that doesn't allow extract terms on the query
           return super.termStatistics(term, context);
        }
        return termStatistics;
    }

    @Override
    public CollectionStatistics collectionStatistics(String field) throws IOException {
        CollectionStatistics collectionStatistics = aggregatedDfs.fieldStatistics().get(field);
        if (collectionStatistics == null) {
            // we don't have stats for this - this might be a must_not clauses etc. that doesn't allow extract terms on the query
           return super.collectionStatistics(field);
        }
        return collectionStatistics;
    }
    
    public int maxDoc() {
        return this.maxDoc;
    }

    @Override
    public Query rewrite(Query query) {
        // this is a bit of a hack. We know that a query which
        // creates a Weight based on this Dummy-Searcher is
        // always already rewritten (see preparedWeight()).
        // Therefore we just return the unmodified query here
        return query;
    }

    @Override
    public Document doc(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void doc(int docID, StoredFieldVisitor fieldVisitor) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Explanation explain(Weight weight, int doc) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
        throw new UnsupportedOperationException();
    }
}
