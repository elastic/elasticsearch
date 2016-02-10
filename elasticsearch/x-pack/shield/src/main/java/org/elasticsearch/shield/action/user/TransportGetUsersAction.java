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
import org.elasticsearch.shield.user.AnonymousUser;
import org.elasticsearch.shield.user.SystemUser;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.authc.esnative.NativeUsersStore;
import org.elasticsearch.shield.authc.esnative.ReservedRealm;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

public class TransportGetUsersAction extends HandledTransportAction<GetUsersRequest, GetUsersResponse> {

    private final NativeUsersStore usersStore;

    @Inject
    public TransportGetUsersAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
                                   IndexNameExpressionResolver indexNameExpressionResolver, NativeUsersStore usersStore,
                                   TransportService transportService) {
        super(settings, GetUsersAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                GetUsersRequest::new);
        this.usersStore = usersStore;
    }

    @Override
    protected void doExecute(final GetUsersRequest request, final ActionListener<GetUsersResponse> listener) {
        final String[] requestedUsers = request.usernames();
        final boolean specificUsersRequested = requestedUsers != null && requestedUsers.length > 0;
        final List<String> usersToSearchFor = new ArrayList<>();
        final List<User> users = new ArrayList<>();

        if (specificUsersRequested) {
            for (String username : requestedUsers) {
                if (ReservedRealm.isReserved(username)) {
                    User user = ReservedRealm.getUser(username);
                    if (user != null) {
                        users.add(user);
                    } else {
                        // the only time a user should be null is if username matches for the anonymous user and the anonymous user is not
                        // enabled!
                        assert AnonymousUser.enabled() == false && AnonymousUser.isAnonymousUsername(username);
                    }
                } else if (SystemUser.NAME.equals(username)) {
                    listener.onFailure(new IllegalArgumentException("user [" + username + "] is internal"));
                    return;
                } else {
                    usersToSearchFor.add(username);
                }
            }
        } else {
            users.addAll(ReservedRealm.users());
        }

        if (usersToSearchFor.size() == 1) {
            final String username = usersToSearchFor.get(0);
            // We can fetch a single user with a get, much cheaper:
            usersStore.getUser(username, new ActionListener<User>() {
                @Override
                public void onResponse(User user) {
                    if (user != null) {
                        users.add(user);
                    }
                    listener.onResponse(new GetUsersResponse(users));
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.error("failed to retrieve user [{}]", e, username);
                    listener.onFailure(e);
                }
            });
        } else if (specificUsersRequested && usersToSearchFor.isEmpty()) {
            listener.onResponse(new GetUsersResponse(users));
        } else {
            usersStore.getUsers(usersToSearchFor.toArray(new String[usersToSearchFor.size()]), new ActionListener<List<User>>() {
                @Override
                public void onResponse(List<User> usersFound) {
                    users.addAll(usersFound);
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
