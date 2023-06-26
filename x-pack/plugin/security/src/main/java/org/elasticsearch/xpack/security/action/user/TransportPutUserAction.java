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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.core.security.action.user.PutUserResponse;
import org.elasticsearch.xpack.core.security.authc.esnative.ClientReservedRealm;
import org.elasticsearch.xpack.core.security.support.NativeRealmValidationUtil;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class TransportPutUserAction extends HandledTransportAction<PutUserRequest, PutUserResponse> {

    private final Settings settings;
    private final NativeUsersStore usersStore;

    @Inject
    public TransportPutUserAction(
        Settings settings,
        ActionFilters actionFilters,
        NativeUsersStore usersStore,
        TransportService transportService
    ) {
        super(PutUserAction.NAME, transportService, actionFilters, PutUserRequest::new);
        this.settings = settings;
        this.usersStore = usersStore;
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
}
