/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.DiversifyingChildrenFloatKnnVectorQuery;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.elasticsearch.search.profile.query.QueryProfiler;

public class ESDiversifyingChildrenFloatKnnVectorQuery extends DiversifyingChildrenFloatKnnVectorQuery implements QueryProfilerProvider {
    private final Integer kParam;
    private long vectorOpsCount;

    public ESDiversifyingChildrenFloatKnnVectorQuery(
        String field,
        float[] query,
        Query childFilter,
        Integer k,
        int numCands,
        BitSetProducer parentsFilter,
        KnnSearchStrategy strategy
    ) {
        super(field, query, childFilter, numCands, parentsFilter, strategy);
        this.kParam = k;
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
        TopDocs topK = kParam == null ? super.mergeLeafResults(perLeafResults) : TopDocs.merge(kParam, perLeafResults);
        vectorOpsCount = topK.totalHits.value();
        return topK;
    }

    @Override
    public void profile(QueryProfiler queryProfiler) {
        queryProfiler.addVectorOpsCount(vectorOpsCount);
    }

    public KnnSearchStrategy getStrategy() {
        return searchStrategy;
    }
}
