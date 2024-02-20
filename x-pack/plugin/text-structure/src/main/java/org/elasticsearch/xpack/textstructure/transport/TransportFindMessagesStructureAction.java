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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.textstructure.action.FindMessagesStructureAction;
import org.elasticsearch.xpack.core.textstructure.action.FindStructureResponse;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureFinder;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureFinderManager;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureOverrides;
import org.elasticsearch.xpack.textstructure.structurefinder.TimeoutChecker;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.StringJoiner;

import static org.elasticsearch.threadpool.ThreadPool.Names.GENERIC;

public class TransportFindMessagesStructureAction extends HandledTransportAction<
    FindMessagesStructureAction.Request,
    FindStructureResponse> {

    private final ThreadPool threadPool;

    @Inject
    public TransportFindMessagesStructureAction(TransportService transportService, ActionFilters actionFilters, ThreadPool threadPool) {
        super(
            FindMessagesStructureAction.NAME,
            transportService,
            actionFilters,
            FindMessagesStructureAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, FindMessagesStructureAction.Request request, ActionListener<FindStructureResponse> listener) {
        StringJoiner sample = new StringJoiner("\n");
        request.getMessages().forEach(sample::add);
        threadPool.executor(GENERIC).execute(() -> {
            try {
                listener.onResponse(buildTextStructureResponse(sample.toString(), request));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private FindStructureResponse buildTextStructureResponse(String sample, FindMessagesStructureAction.Request request) throws Exception {
        TextStructureFinderManager structureFinderManager = new TextStructureFinderManager(threadPool.scheduler());
        try (TimeoutChecker timeoutChecker = new TimeoutChecker("structure analysis", request.getTimeout(), threadPool.scheduler())) {
            TextStructureFinder textStructureFinder = structureFinderManager.makeBestStructureFinder(
                new ArrayList<>(),
                sample,
                StandardCharsets.UTF_8.name(),
                false,
                request.getLineMergeSizeLimit(),
                new TextStructureOverrides(request),
                timeoutChecker
            );

            return new FindStructureResponse(textStructureFinder.getStructure());
        }
    }
}
