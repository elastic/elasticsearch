/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.FileStructureAction;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.filestructurefinder.FileStructureFinder;
import org.elasticsearch.xpack.ml.filestructurefinder.FileStructureFinderManager;

public class TransportFileStructureAction extends HandledTransportAction<FileStructureAction.Request, FileStructureAction.Response> {

    private final ThreadPool threadPool;

    @Inject
    public TransportFileStructureAction(Settings settings, TransportService transportService, ActionFilters actionFilters,
                                        ThreadPool threadPool) {
        super(settings, FileStructureAction.NAME, transportService, actionFilters, FileStructureAction.Request::new);
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, FileStructureAction.Request request, ActionListener<FileStructureAction.Response> listener) {

        // As determining the file structure might take a while, we run
        // in a different thread to avoid blocking the network thread.
        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
            try {
                listener.onResponse(buildFileStructureResponse(request));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private FileStructureAction.Response buildFileStructureResponse(FileStructureAction.Request request) throws Exception {

        FileStructureFinderManager structureFinderManager = new FileStructureFinderManager();

        FileStructureFinder fileStructureFinder =
            structureFinderManager.findFileStructure(request.getLinesToSample(), request.getSample().streamInput());

        return new FileStructureAction.Response(fileStructureFinder.getStructure());
    }
}
