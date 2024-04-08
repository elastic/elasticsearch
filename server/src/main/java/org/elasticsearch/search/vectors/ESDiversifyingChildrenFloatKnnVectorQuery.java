/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.DiversifyingChildrenFloatKnnVectorQuery;
import org.elasticsearch.search.profile.query.QueryProfiler;

public class ESDiversifyingChildrenFloatKnnVectorQuery extends DiversifyingChildrenFloatKnnVectorQuery implements ProfilingQuery {
    private long vectorOpsCount;

    public ESDiversifyingChildrenFloatKnnVectorQuery(String field, float[] query, Query childFilter, int k, BitSetProducer parentsFilter) {
        super(field, query, childFilter, k, parentsFilter);
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
        TopDocs topK = super.mergeLeafResults(perLeafResults);
        vectorOpsCount = topK.totalHits.value;
        return topK;
    }

    @Override
    public void profile(QueryProfiler queryProfiler) {
        queryProfiler.setVectorOpsCount(vectorOpsCount);
    }
}
