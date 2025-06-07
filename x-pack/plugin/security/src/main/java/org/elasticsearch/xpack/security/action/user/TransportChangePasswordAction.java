/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
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
import org.elasticsearch.xpack.core.security.authc.esnative.ClientReservedRealm;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;

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

        if (ClientReservedRealm.isReservedCandidate(username) && XPackSettings.RESERVED_REALM_ENABLED_SETTING.get(settings) == false) {
            // when on cloud and resetting the elastic operator user by mistake
            ValidationException validationException = new ValidationException();
            validationException.addValidationError(
                ELASTIC_NAME.equalsIgnoreCase(username)
                    ? " To update the user [" + ELASTIC_NAME + "] in a cloud deployment, use the console."
                    : "user [" + username + "] belongs to the " + ReservedRealm.NAME + " realm which is disabled." // shouldn't be
                                                                                                                   // happening
            );
            listener.onFailure(validationException);
            return;
        }

        // check if user exists in the native realm
        nativeUsersStore.getUser(username, new ActionListener<>() {
            @Override
            public void onResponse(User user) {
                // nativeUsersStore.changePassword can create a missing reserved user, so enter only if not reserved
                if (ClientReservedRealm.isReserved(username, settings) == false && user == null) {
                    List<Realm> nonNativeRealms = realms.getActiveRealms()
                        .stream()
                        .filter(t -> Set.of(NativeRealmSettings.TYPE, ReservedRealm.TYPE).contains(t.type()) == false) // Reserved realm is
                                                                                                                       // implemented in the
                                                                                                                       // native store
                        .toList();
                    if (nonNativeRealms.isEmpty()) {
                        listener.onFailure(createUserNotFoundException());
                        return;
                    }

                    GroupedActionListener<User> gal = new GroupedActionListener<>(nonNativeRealms.size(), new ActionListener<>() {
                        @Override
                        public void onResponse(Collection<User> users) {
                            List<String> realmErrors = new ArrayList<>();
                            for (User user : users) {
                                if (user != null) {
                                    realmErrors.add(
                                        "user ["
                                            + username
                                            + "] does not belong to the native realm and cannot be"
                                            + " managed via this API."
                                    );
                                    break;
                                }
                            }
                            if (realmErrors.isEmpty()) {
                                // user wasn't found in any other realm, display standard not-found message
                                listener.onFailure(createUserNotFoundException());
                            } else {
                                ValidationException validationException = new ValidationException();
                                realmErrors.forEach(validationException::addValidationError);
                                listener.onFailure(validationException);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }
                    });
                    for (Realm realm : nonNativeRealms) {
                        EsExecutors.DIRECT_EXECUTOR_SERVICE.execute(
                            ActionRunnable.wrap(gal, userActionListener -> realm.lookupUser(username, userActionListener))
                        );
                    }
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
