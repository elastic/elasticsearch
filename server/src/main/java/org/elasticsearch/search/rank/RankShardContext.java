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
    protected final int from;
    protected final int windowSize;

    public RankShardContext(List<Query> queries, int from, int windowSize) {
        this.queries = queries;
        this.from = from;
        this.windowSize = windowSize;
    }

    public List<Query> queries() {
        return queries;
    }

    public int windowSize() {
        return windowSize;
    }

    /**
     * This is used to reduce the number of required results that are serialized
     * to the coordinating node. Normally we would have to serialize {@code (queries + knns)*window_size}
     * results, but we can infer that there will likely be overlap of document results. Given that we
     * know any searches that match the same document must be on the same shard, we can sort on the shard
     * instead for a top window_size set of results and reduce the amount of data we serialize.
     */
    public abstract RankShardResult combine(List<TopDocs> rankResults);
}
