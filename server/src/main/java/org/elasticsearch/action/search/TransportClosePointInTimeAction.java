/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;

public class TransportClosePointInTimeAction extends HandledTransportAction<ClosePointInTimeRequest, ClosePointInTimeResponse> {

    private final ClusterService clusterService;
    private final SearchTransportService searchTransportService;
    private final NamedWriteableRegistry namedWriteableRegistry;

    @Inject
    public TransportClosePointInTimeAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        SearchTransportService searchTransportService,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        super(ClosePointInTimeAction.NAME, transportService, actionFilters, ClosePointInTimeRequest::new);
        this.clusterService = clusterService;
        this.searchTransportService = searchTransportService;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    @Override
    protected void doExecute(Task task, ClosePointInTimeRequest request, ActionListener<ClosePointInTimeResponse> listener) {
        final SearchContextId searchContextId = SearchContextId.decode(namedWriteableRegistry, request.getId());
        final Collection<SearchContextIdForNode> contextIds = searchContextId.shards().values();
        ClearScrollController.closeContexts(
            clusterService.state().nodes(),
            searchTransportService,
            contextIds,
            listener.map(freed -> new ClosePointInTimeResponse(freed == contextIds.size(), freed))
        );
    }
}
