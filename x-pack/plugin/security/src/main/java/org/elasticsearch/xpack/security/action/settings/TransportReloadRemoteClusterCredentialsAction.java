/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.settings;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.ActionTypes;
import org.elasticsearch.xpack.security.Security;

import java.io.IOException;

/**
 * This is a local-only action which updates remote cluster credentials for remote cluster connections, from keystore settings reloaded via
 * a call to {@link org.elasticsearch.rest.action.admin.cluster.RestReloadSecureSettingsAction}.
 *
 * It's invoked as part of the {@link Security#reload(Settings)} call.
 *
 * This action is largely an implementation detail to work around the fact that Security is a plugin without direct access to many core
 * classes, including the {@link RemoteClusterService} which is required for a credentials reload. A transport action gives us access to
 * the {@link RemoteClusterService} which is injectable but not part of the plugin contract.
 */
public class TransportReloadRemoteClusterCredentialsAction extends TransportAction<
    TransportReloadRemoteClusterCredentialsAction.Request,
    ActionResponse.Empty> {

    private final RemoteClusterService remoteClusterService;

    @Inject
    public TransportReloadRemoteClusterCredentialsAction(TransportService transportService, ActionFilters actionFilters) {
        super(ActionTypes.RELOAD_REMOTE_CLUSTER_CREDENTIALS_ACTION.name(), actionFilters, transportService.getTaskManager());
        this.remoteClusterService = transportService.getRemoteClusterService();
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<ActionResponse.Empty> listener) {
        // We avoid stashing and marking context as system to keep the action as minimal as possible (i.e., avoid copying context)
        remoteClusterService.updateRemoteClusterCredentials(request.getSettings());
        listener.onResponse(ActionResponse.Empty.INSTANCE);
    }

    public static class Request extends ActionRequest {
        private final Settings settings;

        public Request(Settings settings) {
            this.settings = settings;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public Settings getSettings() {
            return settings;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            localOnly();
        }
    }
}
