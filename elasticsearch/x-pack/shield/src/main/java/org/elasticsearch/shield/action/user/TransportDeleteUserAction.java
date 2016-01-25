/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.user;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.esnative.ESNativeUsersStore;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportDeleteUserAction extends HandledTransportAction<DeleteUserRequest, DeleteUserResponse> {

    private final ESNativeUsersStore usersStore;

    @Inject
    public TransportDeleteUserAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
                                  IndexNameExpressionResolver indexNameExpressionResolver,
                                  ESNativeUsersStore usersStore, TransportService transportService) {
        super(settings, DeleteUserAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, DeleteUserRequest::new);
        this.usersStore = usersStore;
    }

    @Override
    protected void doExecute(DeleteUserRequest request, final ActionListener<DeleteUserResponse> listener) {
        try {
            usersStore.removeUser(request, new ActionListener<Boolean>() {
                @Override
                public void onResponse(Boolean found) {
                    listener.onResponse(new DeleteUserResponse(found));
                }

                @Override
                public void onFailure(Throwable e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            logger.error("failed to delete user [{}]", e);
            listener.onFailure(e);
        }
    }
}
