/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.ShardSearchRequest;

public class RetryQueryPhase extends AbstractSearchAsyncAction<SearchPhaseResult> {

    private final SearchPhaseController searchPhaseController;

    // TODO: delay before starting retry?
    // TODO: ability to retry multiple times?
    RetryQueryPhase(final SearchPhaseContext context, final Logger logger,
                    final SearchPhaseController searchPhaseController,
                    final QueryPhaseResultConsumer resultConsumer,
                    final GroupShardsIterator<SearchShardIterator> shardsIts) {
        super("retry-query", context, logger, shardsIts, resultConsumer, context.getRequest().getMaxConcurrentShardRequests());
        this.searchPhaseController = searchPhaseController;
    }

    protected void executePhaseOnShard(final SearchShardIterator shardIt,
                                       final SearchShardTarget shard,
                                       final SearchActionListener<SearchPhaseResult> listener) {
        ShardSearchRequest request = context.buildShardSearchRequest(shardIt, listener.requestIndex);
        context.getSearchTransport().sendExecuteQuery(context.getConnection(shard.getClusterAlias(), shard.getNodeId()),
            request, context.getTask(), listener);
    }

    @Override
    protected SearchPhase getNextPhase(final SearchPhaseResults<SearchPhaseResult> results, SearchPhaseContext context) {
        return new FetchSearchPhase(results, searchPhaseController, null, context);
    }

}
