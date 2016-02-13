/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.user;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.esnative.ESNativeUsersStore;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

public class TransportGetUsersAction extends HandledTransportAction<GetUsersRequest, GetUsersResponse> {

    private final ESNativeUsersStore usersStore;

    @Inject
    public TransportGetUsersAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
                                   IndexNameExpressionResolver indexNameExpressionResolver, ESNativeUsersStore usersStore,
                                   TransportService transportService) {
        super(settings, GetUsersAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                GetUsersRequest::new);
        this.usersStore = usersStore;
    }

    @Override
    protected void doExecute(final GetUsersRequest request, final ActionListener<GetUsersResponse> listener) {
        if (request.usernames().length == 1) {
            final String username = request.usernames()[0];
            // We can fetch a single user with a get, much cheaper:
            usersStore.getUser(username, new ActionListener<User>() {
                @Override
                public void onResponse(User user) {
                    if (user == null) {
                        listener.onResponse(new GetUsersResponse());
                    } else {
                        listener.onResponse(new GetUsersResponse(user));
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.error("failed to retrieve user [{}]", e, username);
                    listener.onFailure(e);
                }
            });
        } else {
            usersStore.getUsers(request.usernames(), new ActionListener<List<User>>() {
                @Override
                public void onResponse(List<User> users) {
                    listener.onResponse(new GetUsersResponse(users));
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.error("failed to retrieve user [{}]", e,
                            Strings.arrayToDelimitedString(request.usernames(), ","));
                    listener.onFailure(e);
                }
            });
        }
    }
}
