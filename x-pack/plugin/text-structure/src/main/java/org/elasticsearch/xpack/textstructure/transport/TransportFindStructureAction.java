/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xpack.core.textstructure.action.FindStructureAction;
import org.elasticsearch.xpack.core.textstructure.action.FindStructureResponse;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureFinder;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureFinderManager;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureOverrides;

import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import static org.elasticsearch.common.util.concurrent.EsExecutors.DIRECT_EXECUTOR_SERVICE;

public class TransportFindStructureAction extends HandledTransportAction<FindStructureAction.Request, FindStructureResponse> {

    private final ThreadPool threadPool;
    private final ExecutorService executorService;

    @Inject
    public TransportFindStructureAction(TransportService transportService, ActionFilters actionFilters, ThreadPool threadPool) {
        super(FindStructureAction.NAME, transportService, actionFilters, FindStructureAction.Request::new, DIRECT_EXECUTOR_SERVICE);
        this.threadPool = threadPool;
        this.executorService = threadPool.generic();
    }

    @Override
    protected void doExecute(Task task, FindStructureAction.Request request, ActionListener<FindStructureResponse> listener) {
        // workaround for https://github.com/elastic/elasticsearch/issues/97916 - TODO remove this when we can
        // when the workaround is removed, change the value that this class's constructor passes to the super constructor from
        // DIRECT_EXECUTOR_SERVICE back to threadpool.generic() so that we continue to fork off of the transport thread.
        var buildTextStructureResponse = ActionRunnable.supply(listener, () -> buildTextStructureResponse(request));
        try {
            executorService.execute(buildTextStructureResponse);
        } catch (RejectedExecutionException e) {
            buildTextStructureResponse.onRejection(e);
        }
    }

    private FindStructureResponse buildTextStructureResponse(FindStructureAction.Request request) throws Exception {
        assert ThreadPool.assertCurrentThreadPool(Names.GENERIC);
        TextStructureFinderManager structureFinderManager = new TextStructureFinderManager(threadPool.scheduler());
        try (InputStream sampleStream = request.getSample().streamInput()) {
            TextStructureFinder textStructureFinder = structureFinderManager.findTextStructure(
                request.getLinesToSample(),
                request.getLineMergeSizeLimit(),
                sampleStream,
                new TextStructureOverrides(request),
                request.getTimeout()
            );
            return new FindStructureResponse(textStructureFinder.getStructure());
        }
    }
}
