/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportValidateViewAction extends HandledTransportAction<ValidateViewAction.Request, ActionResponse.Empty> {
    private final ViewService viewService;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportValidateViewAction(
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ViewService viewService,
        ProjectResolver projectResolver
    ) {
        super(
            ValidateViewAction.NAME,
            transportService,
            actionFilters,
            ValidateViewAction.Request::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.viewService = viewService;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void doExecute(Task task, ValidateViewAction.Request request, ActionListener<ActionResponse.Empty> listener) {
        viewService.validateView(projectResolver.getProjectId(), request.view(), listener.map(ignored -> ActionResponse.Empty.INSTANCE));
    }
}
