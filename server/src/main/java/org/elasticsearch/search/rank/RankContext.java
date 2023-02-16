/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.search.SearchPhaseController.SortedTopDocs;
import org.elasticsearch.action.search.SearchPhaseController.TopDocsStats;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.query.QuerySearchResult;

import java.util.List;

public abstract class RankContext {

    protected final List<QueryBuilder> queryBuilders;
    protected final int size;
    protected final int from;

    public RankContext(List<QueryBuilder> queryBuilders, int size, int from) {
        this.queryBuilders = queryBuilders;
        this.size = size;
        this.from = from;
    }

    public abstract SortedTopDocs rank(List<QuerySearchResult> querySearchResults, TopDocsStats topDocStats);

    public abstract void decorateSearchHit(ScoreDoc scoreDoc, SearchHit searchHit);
}
