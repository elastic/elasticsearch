/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.file.FileUserPasswdStore;

import java.util.Locale;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.security.user.UsernamesField.ELASTIC_NAME;

public class TransportChangePasswordAction extends HandledTransportAction<ChangePasswordRequest, ActionResponse.Empty> {

    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>("cluster:admin/xpack/security/user/change_password");
    private final Settings settings;
    private final NativeUsersStore nativeUsersStore;
    private final FileUserPasswdStore fileUserPasswdStore;

    @Inject
    public TransportChangePasswordAction(
        Settings settings,
        TransportService transportService,
        ActionFilters actionFilters,
        NativeUsersStore nativeUsersStore,
        FileUserPasswdStore fileUserPasswdStore
    ) {
        super(TYPE.name(), transportService, actionFilters, ChangePasswordRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.settings = settings;
        this.nativeUsersStore = nativeUsersStore;
        this.fileUserPasswdStore = fileUserPasswdStore;
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
            && Hasher.getAvailableAlgoStoredPasswordHash().contains(requestPwdHashAlgo.name().toLowerCase(Locale.ROOT)) == false) {
            listener.onFailure(
                new IllegalArgumentException(
                    "The provided password hash is not a hash or it could not be resolved to a supported hash algorithm. "
                        + "The supported password hash algorithms are "
                        + Hasher.getAvailableAlgoStoredPasswordHash().toString()
                )
            );
            return;
        }

        if (fileUserPasswdStore.userExists(request.username())) {
            // File realm users cannot be managed through this API.
            // unresolved q: is it possible a username is repeated across file and native realms, such that stopping here is incorrect?
            logger.debug(() -> format("failed to change password for user [%s]", request.username()));
            ValidationException validationException = new ValidationException();
            validationException.addValidationError(
                "user ["
                    + username
                    + "] is file-based and cannot be managed via this API."
                    + (ELASTIC_NAME.equalsIgnoreCase(username)
                        ? " To update the '" + ELASTIC_NAME + "' user in a cloud deployment, use the console."
                        : "")
            );
            listener.onFailure(validationException);
            return;
        }

        nativeUsersStore.changePassword(request, listener.safeMap(v -> ActionResponse.Empty.INSTANCE));

    }
}
