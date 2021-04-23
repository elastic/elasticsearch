/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportGetStoredScriptAction extends TransportMasterNodeReadAction<GetStoredScriptRequest,
        GetStoredScriptResponse> {

    private final ScriptService scriptService;

    @Inject
    public TransportGetStoredScriptAction(TransportService transportService, ClusterService clusterService,
                                          ThreadPool threadPool, ActionFilters actionFilters,
                                          IndexNameExpressionResolver indexNameExpressionResolver, ScriptService scriptService) {
        super(GetStoredScriptAction.NAME, transportService, clusterService, threadPool, actionFilters,
            GetStoredScriptRequest::new, indexNameExpressionResolver, GetStoredScriptResponse::new, ThreadPool.Names.SAME);
        this.scriptService = scriptService;
    }

    @Override
    protected void masterOperation(Task task, GetStoredScriptRequest request, ClusterState state,
                                   ActionListener<GetStoredScriptResponse> listener) throws Exception {
        listener.onResponse(new GetStoredScriptResponse(request.id(), scriptService.getStoredScript(state, request)));
    }

    @Override
    protected ClusterBlockException checkBlock(GetStoredScriptRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

}
