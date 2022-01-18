/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserAction;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserRequest;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserResponse;
import org.elasticsearch.xpack.core.security.authc.esnative.ClientReservedRealm;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;

public class TransportDeleteUserAction extends HandledTransportAction<DeleteUserRequest, DeleteUserResponse> {

    private final Settings settings;
    private final NativeUsersStore usersStore;

    @Inject
    public TransportDeleteUserAction(
        Settings settings,
        ActionFilters actionFilters,
        NativeUsersStore usersStore,
        TransportService transportService
    ) {
        super(DeleteUserAction.NAME, transportService, actionFilters, DeleteUserRequest::new);
        this.settings = settings;
        this.usersStore = usersStore;
    }

    @Override
    protected void doExecute(Task task, DeleteUserRequest request, final ActionListener<DeleteUserResponse> listener) {
        final String username = request.username();
        if (ClientReservedRealm.isReserved(username, settings)) {
            if (AnonymousUser.isAnonymousUsername(username, settings)) {
                listener.onFailure(new IllegalArgumentException("user [" + username + "] is anonymous and cannot be deleted"));
                return;
            } else {
                listener.onFailure(new IllegalArgumentException("user [" + username + "] is reserved and cannot be deleted"));
                return;
            }
        } else if (User.isInternalUsername(username)) {
            listener.onFailure(new IllegalArgumentException("user [" + username + "] is internal"));
            return;
        }

        usersStore.deleteUser(request, listener.delegateFailure((l, found) -> l.onResponse(new DeleteUserResponse(found))));
    }
}
