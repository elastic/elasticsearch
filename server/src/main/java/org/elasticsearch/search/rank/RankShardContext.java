/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.search.Query;
import org.elasticsearch.search.internal.SearchContext;

import java.util.List;

public abstract class RankShardContext {

    protected final List<Query> queries;
    protected final int size;
    protected final int from;

    public RankShardContext(List<Query> queries, int size, int from) {
        this.queries = queries;
        this.size = size;
        this.from = from;
    }

    public abstract void executeQueries(SearchContext searchContext);
}
