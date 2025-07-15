/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.DefaultShardContext;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.ShardContext;

// FIXME(gal, NOCOMMIT) Come up with a better name for this class.
class MySearchContext implements Releasable {
    private final int index;
    private final SearchContext searchContext;
    private final SetOnce<ShardContext> shardContext = new SetOnce<>();

    MySearchContext(int index, SearchContext searchContext) {
        this.index = index;
        this.searchContext = searchContext;
    }

    ShardContext shardContext() {
        shardContext.trySet(createShardContext());
        return shardContext.get();
    }

    public SearchContext searchContext() {
        return searchContext;
    }

    private ShardContext createShardContext() {
        SearchExecutionContext searchExecutionContext = new SearchExecutionContext(searchContext.getSearchExecutionContext()) {
            @Override
            public SourceProvider createSourceProvider() {
                return new ReinitializingSourceProvider(super::createSourceProvider);
            }
        };
        return new DefaultShardContext(index, this, searchExecutionContext, searchContext.request().getAliasFilter());
    }

    @Override
    public void close() {
        searchContext.close();
    }
}
