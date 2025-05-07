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
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.file.FileRealm;

import java.util.Locale;
import java.util.Optional;

import static org.elasticsearch.xpack.core.security.user.UsernamesField.ELASTIC_NAME;
import static org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore.USER_NOT_FOUND_MESSAGE;

public class TransportChangePasswordAction extends HandledTransportAction<ChangePasswordRequest, ActionResponse.Empty> {

    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>("cluster:admin/xpack/security/user/change_password");
    private final Settings settings;
    private final NativeUsersStore nativeUsersStore;
    private final Realms realms;

    @Inject
    public TransportChangePasswordAction(
        Settings settings,
        TransportService transportService,
        ActionFilters actionFilters,
        NativeUsersStore nativeUsersStore,
        Realms realms
    ) {
        super(TYPE.name(), transportService, actionFilters, ChangePasswordRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.settings = settings;
        this.nativeUsersStore = nativeUsersStore;
        this.realms = realms;
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

        // check if user exists in the native realm
        nativeUsersStore.getUser(username, new ActionListener<>() {
            @Override
            public void onResponse(User user) {
                Optional<Realm> realm = realms.stream().filter(t -> FileRealmSettings.TYPE.equalsIgnoreCase(t.type())).findAny();
                if (user == null && realm.isPresent()) {
                    // we should check if this request is mistakenly trying to reset a file realm user
                    FileRealm fileRealm = (FileRealm) realm.get();
                    fileRealm.lookupUser(username, new ActionListener<>() {
                        @Override
                        public void onResponse(User user) {
                            if (user != null) {
                                ValidationException validationException = new ValidationException();
                                validationException.addValidationError(
                                    "user ["
                                        + username
                                        + "] is file-based and cannot be managed via this API."
                                        + (ELASTIC_NAME.equalsIgnoreCase(username)
                                            ? " To update the '" + ELASTIC_NAME + "' user in a cloud deployment, use the console."
                                            : "")
                                );
                                onFailure(validationException);
                            } else {
                                onFailure(createUserNotFoundException());
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }
                    });
                } else if (user == null) {
                    listener.onFailure(createUserNotFoundException());
                } else {
                    // safe to proceed
                    nativeUsersStore.changePassword(request, listener.safeMap(v -> ActionResponse.Empty.INSTANCE));
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });

    }

    private static ValidationException createUserNotFoundException() {
        ValidationException validationException = new ValidationException();
        validationException.addValidationError(USER_NOT_FOUND_MESSAGE);
        return validationException;
    }
}
