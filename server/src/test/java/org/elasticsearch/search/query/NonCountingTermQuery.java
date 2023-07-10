/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;

import java.io.IOException;

/**
 * A {@link TermQuery} which does not retrieve hit count from Weight#count for reproducible tests.
 * Using this query we will never early-terminate the collection phase because we can already
 * get the document count from the term statistics of each segment.
 */
class NonCountingTermQuery extends TermQuery {

    NonCountingTermQuery(Term term) {
        super(term);
    }

    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        Weight w = super.createWeight(searcher, scoreMode, boost);
        return new FilterWeight(w) {
            public int count(LeafReaderContext context) throws IOException {
                return -1;
            }
        };
    }
}
