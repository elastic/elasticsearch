/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.DiversifyingChildrenByteKnnVectorQuery;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;

public class ESDiversifyingChildrenByteKnnVectorQuery extends DiversifyingChildrenByteKnnVectorQuery implements QueryProfilerProvider {
    private final Integer kParam;
    private long vectorOpsCount;
    private final int k;

    public ESDiversifyingChildrenByteKnnVectorQuery(
        String field,
        byte[] query,
        Query childFilter,
        Integer k,
        int numCands,
        BitSetProducer parentsFilter
    ) {
        super(field, query, childFilter, numCands, parentsFilter);
        this.kParam = k;
        this.k = numCands;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        Query rewrittenQuery = super.rewrite(searcher);
        if (rewrittenQuery instanceof MatchNoDocsQuery) {
            // If the rewritten query is a MatchNoDocsQuery, we can return it directly.
            return rewrittenQuery;
        }
        // We don't rewrite this query, so we return it as is.
        return KnnScoreDocQuery.fromQuery(rewrittenQuery, kParam == null ? k : kParam, searcher);
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
        TopDocs topK = kParam == null ? super.mergeLeafResults(perLeafResults) : TopDocs.merge(kParam, perLeafResults);
        vectorOpsCount = topK.totalHits.value;
        return topK;
    }

    @Override
    public void profile(QueryProfiler queryProfiler) {
        queryProfiler.addVectorOpsCount(vectorOpsCount);
    }
}
