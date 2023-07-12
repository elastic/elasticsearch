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
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;

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
        if (isSameUserRequest(request)) {
            listener.onFailure(new IllegalArgumentException("users may not update the enabled status of their own account"));
            return;
        } else if (AnonymousUser.isAnonymousUsername(username, settings)) {
            listener.onFailure(new IllegalArgumentException("user [" + username + "] is anonymous and cannot be modified using the api"));
            return;
        }

        usersStore.setEnabled(
            username,
            request.enabled(),
            request.getRefreshPolicy(),
            listener.safeMap(v -> ActionResponse.Empty.INSTANCE)
        );
    }

    private boolean isSameUserRequest(SetEnabledRequest request) {
        final var effectiveSubject = securityContext.getAuthentication().getEffectiveSubject();
        final var realmType = effectiveSubject.getRealm().getType();
        // Only native or reserved realm users can be disabled via the API. If the realm of the effective subject is neither,
        // the target must be a different user
        return (ReservedRealm.TYPE.equals(realmType) || NativeRealmSettings.TYPE.equals(realmType))
            && effectiveSubject.getUser().principal().equals(request.username());
    }
}
