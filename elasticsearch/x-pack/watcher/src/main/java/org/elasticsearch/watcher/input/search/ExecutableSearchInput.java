/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.input.ExecutableInput;
import org.elasticsearch.watcher.support.WatcherUtils;
import org.elasticsearch.watcher.support.XContentFilterKeysUtils;
import org.elasticsearch.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.watcher.watch.Payload;

import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * An input that executes search and returns the search response as the initial payload
 */
public class ExecutableSearchInput extends ExecutableInput<SearchInput, SearchInput.Result> {

    public static final SearchType DEFAULT_SEARCH_TYPE = SearchType.QUERY_THEN_FETCH;

    private final WatcherClientProxy client;
    private final @Nullable TimeValue timeout;

    public ExecutableSearchInput(SearchInput input, ESLogger logger, WatcherClientProxy client, @Nullable TimeValue defaultTimeout) {
        super(input, logger);
        this.client = client;
        this.timeout = input.getTimeout() != null ? input.getTimeout() : defaultTimeout;
    }

    @Override
    public SearchInput.Result execute(WatchExecutionContext ctx, Payload payload) {
        SearchRequest request = null;
        try {
            request = WatcherUtils.createSearchRequestFromPrototype(input.getSearchRequest(), ctx, payload);
            return doExecute(ctx, request);
        } catch (Exception e) {
            logger.error("failed to execute [{}] input for [{}]", e, SearchInput.TYPE, ctx.watch());
            return new SearchInput.Result(request, e);
        }
    }

    SearchInput.Result doExecute(WatchExecutionContext ctx, SearchRequest request) throws Exception {
        if (logger.isTraceEnabled()) {
            ToXContent source = request.source() != null ? request.source() : request.template();
            logger.trace("[{}] running query for [{}] [{}]", ctx.id(), ctx.watch().id(), XContentHelper.toString(source));
        }

        SearchResponse response = client.search(request, timeout);

        if (logger.isDebugEnabled()) {
            logger.debug("[{}] found [{}] hits", ctx.id(), response.getHits().getTotalHits());
            for (SearchHit hit : response.getHits()) {
                logger.debug("[{}] hit [{}]", ctx.id(), XContentHelper.toString(hit));
            }
        }

        final Payload payload;
        if (input.getExtractKeys() != null) {
            XContentBuilder builder = jsonBuilder().startObject().value(response).endObject();
            XContentParser parser = XContentHelper.createParser(builder.bytes());
            Map<String, Object> filteredKeys = XContentFilterKeysUtils.filterMapOrdered(input.getExtractKeys(), parser);
            payload = new Payload.Simple(filteredKeys);
        } else {
            payload = new Payload.XContent(response);
        }

        return new SearchInput.Result(request, payload);
    }
}
