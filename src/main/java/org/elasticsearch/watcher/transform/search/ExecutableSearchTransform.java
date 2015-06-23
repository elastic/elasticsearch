/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.DynamicIndexName;
import org.elasticsearch.watcher.support.WatcherUtils;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.elasticsearch.watcher.transform.ExecutableTransform;
import org.elasticsearch.watcher.watch.Payload;

/**
 *
 */
public class ExecutableSearchTransform extends ExecutableTransform<SearchTransform, SearchTransform.Result> {

    public static final SearchType DEFAULT_SEARCH_TYPE = SearchType.QUERY_THEN_FETCH;

    protected final ClientProxy client;
    private final @Nullable DynamicIndexName[] indexNames;

    public ExecutableSearchTransform(SearchTransform transform, ESLogger logger, ClientProxy client, DynamicIndexName.Parser indexNameParser) {
        super(transform, logger);
        this.client = client;
        String[] indices = transform.getRequest().indices();
        this.indexNames = indices != null ? indexNameParser.parse(indices) : null;
    }

    DynamicIndexName[] indexNames() {
        return indexNames;
    }

    @Override
    public SearchTransform.Result execute(WatchExecutionContext ctx, Payload payload) {
        SearchRequest request = null;
        try {
            request = WatcherUtils.createSearchRequestFromPrototype(transform.getRequest(), indexNames, ctx, payload);
            SearchResponse resp = client.search(request, transform.getTimeout());
            return new SearchTransform.Result(request, new Payload.XContent(resp));
        } catch (Exception e) {
            logger.error("failed to execute [{}] transform for [{}]", e, SearchTransform.TYPE, ctx.id());
            return new SearchTransform.Result(request, e);
        }
    }
}
