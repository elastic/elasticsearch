/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;

import java.util.List;

/**
 * {@code RankShardContext} is a base class used to generate ranking
 * results on each shard where it's responsible for executing any
 * queries during the query phase required for its global ranking method.
 */
public abstract class RankShardContext {

    protected final List<Query> queries;
    protected final int size;
    protected final int from;
    protected final int windowSize;

    public RankShardContext(List<Query> queries, int size, int from, int windowSize) {
        this.queries = queries;
        this.size = size;
        this.from = from;
        this.windowSize = windowSize;
    }

    public List<Query> queries() {
        return queries;
    }

    public int size() {
        return size;
    }

    public int from() {
        return from;
    }

    public int windowSize() {
        return windowSize;
    }

    public abstract RankShardResult sort(List<TopDocs> rrfRankResults);
}
