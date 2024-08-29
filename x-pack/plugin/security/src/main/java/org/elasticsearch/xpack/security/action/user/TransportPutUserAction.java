/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.user;

import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.core.security.action.user.PutUserResponse;
import org.elasticsearch.xpack.core.security.authc.esnative.ClientReservedRealm;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.support.NativeRealmValidationUtil;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class TransportPutUserAction extends HandledTransportAction<PutUserRequest, PutUserResponse> {

    private final Settings settings;
    private final NativeUsersStore usersStore;
    private final SecurityContext securityContext;

    @Inject
    public TransportPutUserAction(
        Settings settings,
        ActionFilters actionFilters,
        NativeUsersStore usersStore,
        SecurityContext securityContext,
        TransportService transportService
    ) {
        super(PutUserAction.NAME, transportService, actionFilters, PutUserRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.settings = settings;
        this.usersStore = usersStore;
        this.securityContext = securityContext;
    }

    @Override
    protected void doExecute(Task task, final PutUserRequest request, final ActionListener<PutUserResponse> listener) {
        final ActionRequestValidationException validationException = validateRequest(request);
        if (validationException != null) {
            listener.onFailure(validationException);
        } else {
            usersStore.putUser(request, new ActionListener<Boolean>() {
                @Override
                public void onResponse(Boolean created) {
                    if (created) {
                        logger.info("added user [{}]", request.username());
                    } else {
                        logger.info("updated user [{}]", request.username());
                    }
                    listener.onResponse(new PutUserResponse(created));
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error((Supplier<?>) () -> "failed to put user [" + request.username() + "]", e);
                    listener.onFailure(e);
                }
            });
        }
    }

    private ActionRequestValidationException validateRequest(PutUserRequest request) {
        ActionRequestValidationException validationException = null;
        final String username = request.username();
        if (isDisablingOwnUser(request)) {
            validationException = addValidationError(
                "native and reserved realm users may not update the enabled status of their own account",
                validationException
            );
        }
        if (ClientReservedRealm.isReserved(username, settings)) {
            if (AnonymousUser.isAnonymousUsername(username, settings)) {
                validationException = addValidationError(
                    "user [" + username + "] is anonymous and cannot be modified via the API",
                    validationException
                );
            } else {
                validationException = addValidationError(
                    "user [" + username + "] is reserved and only the " + "password can be changed",
                    validationException
                );
            }
        } else {
            Validation.Error usernameError = NativeRealmValidationUtil.validateUsername(username, true, settings);
            if (usernameError != null) {
                validationException = addValidationError(usernameError.toString(), validationException);
            }
        }

        if (request.roles() != null) {
            for (String role : request.roles()) {
                Validation.Error roleNameError = NativeRealmValidationUtil.validateRoleName(role, true);
                if (roleNameError != null) {
                    validationException = addValidationError(roleNameError.toString(), validationException);
                }
            }
        }
        return validationException;
    }

    private boolean isDisablingOwnUser(PutUserRequest request) {
        if (request.enabled() == false) {
            final var effectiveSubject = securityContext.getAuthentication().getEffectiveSubject();
            final var realmType = effectiveSubject.getRealm().getType();
            // Only native or reserved realm users can be disabled via the API. If the realm of the effective subject is neither,
            // the target must be a different user
            return (NativeRealmSettings.TYPE.equals(realmType)) && effectiveSubject.getUser().principal().equals(request.username());
        }
        return false;
    }

}
