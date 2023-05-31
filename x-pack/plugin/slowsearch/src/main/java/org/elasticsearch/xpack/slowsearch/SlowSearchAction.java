/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.xpack.slowsearch;

import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestChunkedToXContentListener;
import org.elasticsearch.usage.SearchUsageHolder;

import java.io.IOException;
import java.util.List;
import java.util.function.IntConsumer;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.action.search.RestSearchAction.parseSearchRequest;

/**
 * Example of adding a cat action with a plugin.
 */
public class SlowSearchAction extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(SlowSearchAction.class);

    private final SearchUsageHolder searchUsageHolder;

    public SlowSearchAction(SearchUsageHolder searchUsageHolder) {
        this.searchUsageHolder = searchUsageHolder;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_slowsearch"), new Route(POST, "/_slowsearch"));
    }

    @Override
    public String getName() {
        return "slowsearch_rest_handler";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        long sleepSec = request.paramAsLong("sleep", 1);
        try {
            logger.warn(format("sleeping [%d] seconds", sleepSec));
            Thread.sleep(sleepSec * 1_000);
        } catch (InterruptedException ignored) {}
        SearchRequest searchRequest = new SearchRequest();
        IntConsumer setSize = size -> searchRequest.source().size(size);
        request.withContentOrSourceParamParserOrNull(
            parser -> parseSearchRequest(searchRequest, request, parser, client.getNamedWriteableRegistry(), setSize, searchUsageHolder)
        );

        return channel -> {
            RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancelClient.execute(SearchAction.INSTANCE, searchRequest, new RestChunkedToXContentListener<>(channel));
        };
    }
}
