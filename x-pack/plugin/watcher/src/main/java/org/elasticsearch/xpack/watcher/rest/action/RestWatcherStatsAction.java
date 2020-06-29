/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsRequest;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestWatcherStatsAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestWatcherStatsAction.class);

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_watcher/stats"),
            new Route(GET, "/_watcher/stats/{metric}"));
    }

    @Override
    public String getName() {
        return "watcher_stats";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, NodeClient client) {
        Set<String> metrics = Strings.tokenizeByCommaToSet(restRequest.param("metric", ""));

        WatcherStatsRequest request = new WatcherStatsRequest();
        if (metrics.contains("_all")) {
            request.includeCurrentWatches(true);
            request.includeQueuedWatches(true);
        } else {
            request.includeCurrentWatches(metrics.contains("current_watches"));
            request.includeQueuedWatches(metrics.contains("queued_watches") || metrics.contains("pending_watches"));
        }

        if (metrics.contains("pending_watches")) {
            deprecationLogger.deprecate("pending_watches", "The pending_watches parameter is deprecated, use queued_watches instead");
        }


        return channel -> client.execute(WatcherStatsAction.INSTANCE, request, new RestActions.NodesResponseRestListener<>(channel));
    }

    private static final Set<String> RESPONSE_PARAMS = Collections.singleton("emit_stacktraces");

    @Override
    protected Set<String> responseParams() {
        // this parameter is only needed when current watches are supposed to be returned
        // it's used in the WatchExecutionContext.toXContent() method
        return RESPONSE_PARAMS;
    }
}
