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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.action.search.ParsedScrollId.QUERY_AND_FETCH_TYPE;
import static org.elasticsearch.action.search.ParsedScrollId.QUERY_THEN_FETCH_TYPE;
import static org.elasticsearch.action.search.TransportSearchHelper.parseScrollId;

public class TransportSearchScrollAction extends HandledTransportAction<SearchScrollRequest, SearchResponse> {

    private static final Logger logger = LogManager.getLogger(TransportSearchScrollAction.class);
    private final ClusterService clusterService;
    private final SearchTransportService searchTransportService;

    @Inject
    public TransportSearchScrollAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        SearchTransportService searchTransportService
    ) {
        super(SearchScrollAction.NAME, transportService, actionFilters, SearchScrollRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.searchTransportService = searchTransportService;
    }

    @Override
    protected void doExecute(Task task, SearchScrollRequest request, ActionListener<SearchResponse> listener) {
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
                    listener
                );
                case QUERY_AND_FETCH_TYPE -> // TODO can we get rid of this?
                    new SearchScrollQueryAndFetchAsyncAction(
                        logger,
                        clusterService,
                        searchTransportService,
                        request,
                        (SearchTask) task,
                        scrollId,
                        listener
                    );
                default -> throw new IllegalArgumentException("Scroll id type [" + scrollId.getType() + "] unrecognized");
            };
            action.run();
        } catch (Exception e) {
            if (ExceptionsHelper.status(e).getStatus() >= 400) { // TODO: change to 500
                logger.warn(
                    "{} Exception in TransportSearchScrollAction for task: [{}] and request [{}]"
                        + ". Exception message: {}. Stack trace: {}",
                    ExceptionsHelper.status(e).getStatus(),
                    task,
                    request,
                    e.getMessage(),
                    ExceptionsHelper.stackTrace(e)
                );
            }
            listener.onFailure(e);
        }
    }
}
