/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.textstructure.action.FindMessageStructureAction;
import org.elasticsearch.xpack.core.textstructure.action.FindStructureResponse;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureFinder;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureFinderManager;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureOverrides;

public class TransportFindMessageStructureAction extends HandledTransportAction<FindMessageStructureAction.Request, FindStructureResponse> {

    private final ThreadPool threadPool;

    @Inject
    public TransportFindMessageStructureAction(TransportService transportService, ActionFilters actionFilters, ThreadPool threadPool) {
        super(
            FindMessageStructureAction.NAME,
            transportService,
            actionFilters,
            FindMessageStructureAction.Request::new,
            threadPool.generic()
        );
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, FindMessageStructureAction.Request request, ActionListener<FindStructureResponse> listener) {
        try {
            listener.onResponse(buildTextStructureResponse(request));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private FindStructureResponse buildTextStructureResponse(FindMessageStructureAction.Request request) throws Exception {
        TextStructureFinderManager structureFinderManager = new TextStructureFinderManager(threadPool.scheduler());
        TextStructureFinder textStructureFinder = structureFinderManager.findTextStructure(
            request.getMessages(),
            new TextStructureOverrides(request),
            request.getTimeout()
        );
        return new FindStructureResponse(textStructureFinder.getStructure());
    }
}
