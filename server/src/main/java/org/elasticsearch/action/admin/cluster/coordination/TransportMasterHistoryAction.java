/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.coordination.MasterHistoryService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

/**
 * This transport action fetches the MasterHistory from a remote node.
 */
public class TransportMasterHistoryAction extends HandledTransportAction<MasterHistoryAction.Request, MasterHistoryAction.Response> {
    private final MasterHistoryService masterHistoryService;

    @Inject
    public TransportMasterHistoryAction(
        TransportService transportService,
        ActionFilters actionFilters,
        MasterHistoryService masterHistoryService
    ) {
        super(MasterHistoryAction.NAME, transportService, actionFilters, MasterHistoryAction.Request::new);
        this.masterHistoryService = masterHistoryService;
    }

    @Override
    protected void doExecute(Task task, MasterHistoryAction.Request request, ActionListener<MasterHistoryAction.Response> listener) {
        listener.onResponse(new MasterHistoryAction.Response(masterHistoryService.getLocalMasterHistory().getImmutableView()));
    }
}
