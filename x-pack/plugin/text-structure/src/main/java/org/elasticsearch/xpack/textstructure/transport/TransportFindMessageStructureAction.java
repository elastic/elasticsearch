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
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.textstructure.action.FindMessageStructureAction;
import org.elasticsearch.xpack.core.textstructure.action.FindStructureResponse;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureFinder;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureFinderManager;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureOverrides;

import static org.elasticsearch.threadpool.ThreadPool.Names.GENERIC;

public class TransportFindMessageStructureAction extends HandledTransportAction<FindMessageStructureAction.Request, FindStructureResponse> {

    private final ThreadPool threadPool;
    private final TextStructExecutor executor;

    @Inject
    public TransportFindMessageStructureAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        TextStructExecutor executor
    ) {
        super(
            FindMessageStructureAction.NAME,
            transportService,
            actionFilters,
            FindMessageStructureAction.Request::new,
            executor.handledTransportActionExecutorService()
        );
        this.threadPool = threadPool;
        this.executor = executor;
    }

    @Override
    protected void doExecute(Task task, FindMessageStructureAction.Request request, ActionListener<FindStructureResponse> listener) {
        executor.execute(listener, () -> buildTextStructureResponse(request));
    }

    private FindStructureResponse buildTextStructureResponse(FindMessageStructureAction.Request request) throws Exception {
        assert ThreadPool.assertCurrentThreadPool(GENERIC);
        TextStructureFinderManager structureFinderManager = new TextStructureFinderManager(threadPool.scheduler());
        TextStructureFinder textStructureFinder = structureFinderManager.findTextStructure(
            request.getMessages(),
            new TextStructureOverrides(request),
            request.getTimeout()
        );
        return new FindStructureResponse(textStructureFinder.getStructure());
    }
}
