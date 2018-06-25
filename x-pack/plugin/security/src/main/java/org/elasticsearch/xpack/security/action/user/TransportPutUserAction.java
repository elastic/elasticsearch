/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.user;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;

public class TransportPutUserAction extends HandledTransportAction<PutUserRequest, PutUserResponse> {

    private final NativeUsersStore usersStore;

    @Inject
    public TransportPutUserAction(Settings settings, ActionFilters actionFilters,
                                  NativeUsersStore usersStore, TransportService transportService) {
        super(settings, PutUserAction.NAME, transportService, actionFilters, PutUserRequest::new);
        this.usersStore = usersStore;
    }

    @Override
    protected void doExecute(Task task, final PutUserRequest request, final ActionListener<PutUserResponse> listener) {
        final String username = request.username();
        if (ClientReservedRealm.isReserved(username, settings)) {
            if (AnonymousUser.isAnonymousUsername(username, settings)) {
                listener.onFailure(new IllegalArgumentException("user [" + username + "] is anonymous and cannot be modified via the API"));
                return;
            } else {
                listener.onFailure(new IllegalArgumentException("user [" + username + "] is reserved and only the " +
                        "password can be changed"));
                return;
            }
        } else if (SystemUser.NAME.equals(username) || XPackUser.NAME.equals(username)) {
            listener.onFailure(new IllegalArgumentException("user [" + username + "] is internal"));
            return;
        }

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
                logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to put user [{}]", request.username()), e);
                listener.onFailure(e);
            }
        });
    }
}
