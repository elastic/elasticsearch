/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.input.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.input.ExecutableInput;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.support.XContentFilterKeysUtils;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateService;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.watcher.input.search.SearchInput.TYPE;

/**
 * An input that executes search and returns the search response as the initial payload
 */
public class ExecutableSearchInput extends ExecutableInput<SearchInput, SearchInput.Result> {
    public static final SearchType DEFAULT_SEARCH_TYPE = SearchType.QUERY_THEN_FETCH;

    private static final Logger logger = LogManager.getLogger(ExecutableSearchInput.class);

    private static final Params EMPTY_PARAMS = new MapParams(Collections.emptyMap());

    private final Client client;
    private final WatcherSearchTemplateService searchTemplateService;
    private final TimeValue timeout;

    public ExecutableSearchInput(
        SearchInput input,
        Client client,
        WatcherSearchTemplateService searchTemplateService,
        TimeValue defaultTimeout
    ) {
        super(input);
        this.client = client;
        this.searchTemplateService = searchTemplateService;
        this.timeout = input.getTimeout() != null ? input.getTimeout() : defaultTimeout;
    }

    @Override
    public SearchInput.Result execute(WatchExecutionContext ctx, Payload payload) {
        WatcherSearchTemplateRequest request = null;
        try {
            Script template = input.getRequest().getOrCreateTemplate();
            String renderedTemplate = searchTemplateService.renderTemplate(template, ctx, payload);
            request = new WatcherSearchTemplateRequest(input.getRequest(), new BytesArray(renderedTemplate));
            return doExecute(ctx, request);
        } catch (Exception e) {
            logger.error(() -> format("failed to execute [%s] input for watch [%s]", TYPE, ctx.watch().id()), e);
            return new SearchInput.Result(request, e);
        }
    }

    SearchInput.Result doExecute(WatchExecutionContext ctx, WatcherSearchTemplateRequest request) throws Exception {
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] running query for [{}] [{}]", ctx.id(), ctx.watch().id(), request.getSearchSource().utf8ToString());
        }

        SearchRequest searchRequest = searchTemplateService.toSearchRequest(request);
        ClientHelper.assertNoAuthorizationHeader(ctx.watch().status().getHeaders());
        final SearchResponse response = ClientHelper.executeWithHeaders(
            ctx.watch().status().getHeaders(),
            ClientHelper.WATCHER_ORIGIN,
            client,
            () -> client.search(searchRequest).actionGet(timeout)
        );

        if (logger.isDebugEnabled()) {
            logger.debug("[{}] found [{}] hits", ctx.id(), response.getHits().getTotalHits().value);
            for (SearchHit hit : response.getHits()) {
                logger.debug("[{}] hit [{}]", ctx.id(), hit.getSourceAsMap());
            }
        }

        final Payload payload;
        final Params params;
        if (request.isRestTotalHitsAsint()) {
            params = new MapParams(Collections.singletonMap("rest_total_hits_as_int", "true"));
        } else {
            params = EMPTY_PARAMS;
        }
        if (input.getExtractKeys() != null) {
            BytesReference bytes = XContentHelper.toXContent(response, XContentType.SMILE, params, false);
            // EMPTY is safe here because we never use namedObject
            try (
                XContentParser parser = XContentHelper.createParser(
                    NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    bytes,
                    XContentType.SMILE
                )
            ) {
                Map<String, Object> filteredKeys = XContentFilterKeysUtils.filterMapOrdered(input.getExtractKeys(), parser);
                payload = new Payload.Simple(filteredKeys);
            }
        } else {
            payload = new Payload.XContent(response, params);
        }

        return new SearchInput.Result(request, payload);
    }
}
