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
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;

import java.util.Locale;

public class TransportChangePasswordAction extends HandledTransportAction<ChangePasswordRequest, ActionResponse.Empty> {

    private final Settings settings;
    private final NativeUsersStore nativeUsersStore;

    @Inject
    public TransportChangePasswordAction(
        Settings settings,
        TransportService transportService,
        ActionFilters actionFilters,
        NativeUsersStore nativeUsersStore
    ) {
        super(ChangePasswordAction.NAME, transportService, actionFilters, ChangePasswordRequest::new);
        this.settings = settings;
        this.nativeUsersStore = nativeUsersStore;
    }

    @Override
    protected void doExecute(Task task, ChangePasswordRequest request, ActionListener<ActionResponse.Empty> listener) {
        final String username = request.username();
        if (AnonymousUser.isAnonymousUsername(username, settings)) {
            listener.onFailure(new IllegalArgumentException("user [" + username + "] is anonymous and cannot be modified via the API"));
            return;
        }
        final Hasher requestPwdHashAlgo = Hasher.resolveFromHash(request.passwordHash());
        final Hasher configPwdHashAlgo = Hasher.resolve(XPackSettings.PASSWORD_HASHING_ALGORITHM.get(settings));
        if (requestPwdHashAlgo.equals(configPwdHashAlgo) == false
            && Hasher.getAvailableAlgoStoredHash().contains(requestPwdHashAlgo.name().toLowerCase(Locale.ROOT)) == false) {
            listener.onFailure(
                new IllegalArgumentException(
                    "The provided password hash is not a hash or it could not be resolved to a supported hash algorithm. "
                        + "The supported password hash algorithms are "
                        + Hasher.getAvailableAlgoStoredHash().toString()
                )
            );
            return;
        }
        nativeUsersStore.changePassword(request, listener.safeMap(v -> ActionResponse.Empty.INSTANCE));
    }
}
