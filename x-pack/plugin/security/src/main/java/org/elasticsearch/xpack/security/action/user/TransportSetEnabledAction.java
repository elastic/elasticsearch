/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.SetEnabledAction;
import org.elasticsearch.xpack.core.security.action.user.SetEnabledRequest;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;

/**
 * Transport action that handles setting a native or reserved user to enabled
 */
public class TransportSetEnabledAction extends HandledTransportAction<SetEnabledRequest, ActionResponse.Empty> {

    private final Settings settings;
    private final SecurityContext securityContext;
    private final NativeUsersStore usersStore;

    @Inject
    public TransportSetEnabledAction(
        Settings settings,
        TransportService transportService,
        ActionFilters actionFilters,
        SecurityContext securityContext,
        NativeUsersStore usersStore
    ) {
        super(SetEnabledAction.NAME, transportService, actionFilters, SetEnabledRequest::new);
        this.settings = settings;
        this.securityContext = securityContext;
        this.usersStore = usersStore;
    }

    @Override
    protected void doExecute(Task task, SetEnabledRequest request, ActionListener<ActionResponse.Empty> listener) {
        final String username = request.username();
        // make sure the user is not disabling themselves
        if (securityContext.getUser().principal().equals(request.username())) {
            listener.onFailure(new IllegalArgumentException("users may not update the enabled status of their own account"));
            return;
        } else if (User.isInternalUsername(username)) {
            listener.onFailure(new IllegalArgumentException("user [" + username + "] is internal"));
            return;
        } else if (AnonymousUser.isAnonymousUsername(username, settings)) {
            listener.onFailure(new IllegalArgumentException("user [" + username + "] is anonymous and cannot be modified using the api"));
            return;
        }

        usersStore.setEnabled(
            username,
            request.enabled(),
            request.getRefreshPolicy(),
            listener.delegateFailure((l, v) -> l.onResponse(ActionResponse.Empty.INSTANCE))
        );
    }
}
