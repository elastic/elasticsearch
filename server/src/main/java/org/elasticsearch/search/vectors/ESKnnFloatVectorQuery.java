/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.search.profile.query.QueryProfiler;

public class ESKnnFloatVectorQuery extends KnnFloatVectorQuery implements ProfilingQuery {
    private final Integer kParam;
    private long vectorOpsCount;

    public ESKnnFloatVectorQuery(String field, float[] target, Integer k, int numCands, Query filter) {
        super(field, target, numCands, filter);
        this.kParam = k;
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
        // if k param is set, we get only top k results from each shard
        TopDocs topK = kParam == null ? super.mergeLeafResults(perLeafResults) : TopDocs.merge(kParam, perLeafResults);
        vectorOpsCount = topK.totalHits.value();
        return topK;
    }

    @Override
    public void profile(QueryProfiler queryProfiler) {
        queryProfiler.setVectorOpsCount(vectorOpsCount);
    }
}
