/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.action.search.SearchResponseMetrics;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.action.search.ParsedScrollId.QUERY_AND_FETCH_TYPE;
import static org.elasticsearch.action.search.ParsedScrollId.QUERY_THEN_FETCH_TYPE;
import static org.elasticsearch.action.search.TransportSearchHelper.parseScrollId;

public class TransportSearchScrollAction extends HandledTransportAction<SearchScrollRequest, SearchResponse> {
    public static final ActionType<SearchResponse> TYPE = new ActionType<>("indices:data/read/scroll");
    public static final RemoteClusterActionType<SearchResponse> REMOTE_TYPE = new RemoteClusterActionType<>(
        TYPE.name(),
        SearchResponse::new
    );
    private static final Logger logger = LogManager.getLogger(TransportSearchScrollAction.class);
    private final ClusterService clusterService;
    private final SearchTransportService searchTransportService;
    private final SearchResponseMetrics searchResponseMetrics;

    @Inject
    public TransportSearchScrollAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        SearchTransportService searchTransportService,
        SearchResponseMetrics searchResponseMetrics
    ) {
        super(TYPE.name(), transportService, actionFilters, SearchScrollRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.searchTransportService = searchTransportService;
        this.searchResponseMetrics = searchResponseMetrics;
    }

    @Override
    protected void doExecute(Task task, SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        ActionListener<SearchResponse> loggingAndMetrics = new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                try {
                    searchResponseMetrics.recordTookTime(searchResponse.getTookInMillis());
                    SearchResponseMetrics.ResponseCountTotalStatus responseCountTotalStatus =
                        SearchResponseMetrics.ResponseCountTotalStatus.SUCCESS;
                    if (searchResponse.getShardFailures() != null && searchResponse.getShardFailures().length > 0) {
                        ShardOperationFailedException[] groupedFailures = ExceptionsHelper.groupBy(searchResponse.getShardFailures());
                        for (ShardOperationFailedException f : groupedFailures) {
                            Throwable cause = f.getCause() == null ? f : f.getCause();
                            if (ExceptionsHelper.status(cause).getStatus() >= 500
                                && ExceptionsHelper.isNodeOrShardUnavailableTypeException(cause) == false) {
                                logger.warn("TransportSearchScrollAction shard failure (partial results response)", f);
                                responseCountTotalStatus = SearchResponseMetrics.ResponseCountTotalStatus.PARTIAL_FAILURE;
                            }
                        }
                    }
                    listener.onResponse(searchResponse);
                    // increment after the delegated onResponse to ensure we don't
                    // record both a success and a failure if there is an exception
                    searchResponseMetrics.incrementResponseCount(responseCountTotalStatus);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                searchResponseMetrics.incrementResponseCount(SearchResponseMetrics.ResponseCountTotalStatus.FAILURE);
                listener.onFailure(e);
            }
        };
        try {
            ParsedScrollId scrollId = parseScrollId(request.scrollId());
            Runnable action = switch (scrollId.getType()) {
                case QUERY_THEN_FETCH_TYPE -> new SearchScrollQueryThenFetchAsyncAction(
                    logger,
                    clusterService,
                    searchTransportService,
                    request,
                    (SearchTask) task,
                    scrollId,
                    loggingAndMetrics
                );
                case QUERY_AND_FETCH_TYPE -> // TODO can we get rid of this?
                    new SearchScrollQueryAndFetchAsyncAction(
                        logger,
                        clusterService,
                        searchTransportService,
                        request,
                        (SearchTask) task,
                        scrollId,
                        loggingAndMetrics
                    );
                default -> throw new IllegalArgumentException("Scroll id type [" + scrollId.getType() + "] unrecognized");
            };
            action.run();
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
