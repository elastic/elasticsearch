/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.util.Bits;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;

public class ESKnnFloatVectorQuery extends KnnFloatVectorQuery implements ProfilingQuery {
    private static final TopDocs NO_RESULTS = TopDocsCollector.EMPTY_TOPDOCS;
    private long vectorOpsCount;
    private final float[] target;

    public ESKnnFloatVectorQuery(String field, float[] target, int k, Query filter) {
        super(field, target, k, filter);
        this.target = target;
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
        TopDocs topK = super.mergeLeafResults(perLeafResults);
        vectorOpsCount = topK.totalHits.value;
        return topK;
    }

    @Override
    protected TopDocs approximateSearch(LeafReaderContext context, Bits acceptDocs, int visitedLimit) throws IOException {
        // We increment visit limit by one to bypass a fencepost error in the collector
        if (visitedLimit < Integer.MAX_VALUE) {
            visitedLimit += 1;
        }
        TopDocs results = context.reader().searchNearestVectors(field, target, k, acceptDocs, visitedLimit);
        return results != null ? results : NO_RESULTS;
    }

    @Override
    public void profile(QueryProfiler queryProfiler) {
        queryProfiler.setVectorOpsCount(vectorOpsCount);
    }
}
