/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.settings;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.settings.ReloadRemoteClusterCredentialsAction;

public class TransportReloadRemoteClusterCredentialsAction extends TransportAction<
    ReloadRemoteClusterCredentialsAction.Request,
    ActionResponse.Empty> {

    private final RemoteClusterService remoteClusterService;

    @Inject
    public TransportReloadRemoteClusterCredentialsAction(TransportService transportService, ActionFilters actionFilters) {
        super(ReloadRemoteClusterCredentialsAction.NAME, actionFilters, transportService.getTaskManager());
        this.remoteClusterService = transportService.getRemoteClusterService();
    }

    @Override
    protected void doExecute(
        Task task,
        ReloadRemoteClusterCredentialsAction.Request request,
        ActionListener<ActionResponse.Empty> listener
    ) {
        remoteClusterService.updateRemoteClusterCredentials(request.getSettings());
        listener.onResponse(ActionResponse.Empty.INSTANCE);
    }
}
