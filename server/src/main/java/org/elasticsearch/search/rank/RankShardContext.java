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
import org.elasticsearch.search.query.QuerySearchResult;

import java.util.List;

/**
 * {@code RankShardContext} is a base class used to generate ranking
 * results on each shard where it's responsible for executing any
 * queries during the query phase required for its global ranking method.
 */
public abstract class RankShardContext {

    protected final List<Query> queries;
    protected final int windowSize;
    protected final int from;

    public RankShardContext(List<Query> queries, int windowSize, int from) {
        this.queries = queries;
        this.windowSize = windowSize;
        this.from = from;
    }

    public List<Query> queries() {
        return queries;
    }

    public int windowSize() {
        return windowSize;
    }

    public int from() {
        return from;
    }

    public abstract void sort(List<TopDocs> rrfRankResults, QuerySearchResult querySearchResult);
}
