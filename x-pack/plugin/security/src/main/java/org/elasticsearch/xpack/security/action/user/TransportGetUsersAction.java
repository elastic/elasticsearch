/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.user.GetUsersAction;
import org.elasticsearch.xpack.core.security.action.user.GetUsersRequest;
import org.elasticsearch.xpack.core.security.action.user.GetUsersResponse;
import org.elasticsearch.xpack.core.security.authc.esnative.ClientReservedRealm;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TransportGetUsersAction extends HandledTransportAction<GetUsersRequest, GetUsersResponse> {

    private final Settings settings;
    private final NativeUsersStore usersStore;
    private final ReservedRealm reservedRealm;

    @Inject
    public TransportGetUsersAction(Settings settings, ActionFilters actionFilters,
                                   NativeUsersStore usersStore, TransportService transportService, ReservedRealm reservedRealm) {
        super(GetUsersAction.NAME, transportService, actionFilters, GetUsersRequest::new);
        this.settings = settings;
        this.usersStore = usersStore;
        this.reservedRealm = reservedRealm;
    }

    @Override
    protected void doExecute(Task task, final GetUsersRequest request, final ActionListener<GetUsersResponse> listener) {
        final String[] requestedUsers = request.usernames();
        final boolean specificUsersRequested = requestedUsers != null && requestedUsers.length > 0;
        final List<String> usersToSearchFor = new ArrayList<>();
        final List<User> users = new ArrayList<>();
        final List<String> realmLookup = new ArrayList<>();
        if (specificUsersRequested) {
            for (String username : requestedUsers) {
                if (ClientReservedRealm.isReserved(username, settings)) {
                    realmLookup.add(username);
                } else if (User.isInternalUsername(username)) {
                    listener.onFailure(new IllegalArgumentException("user [" + username + "] is internal"));
                    return;
                } else {
                    usersToSearchFor.add(username);
                }
            }
        }

        final ActionListener<Collection<Collection<User>>> sendingListener = ActionListener.wrap((userLists) -> {
                users.addAll(userLists.stream().flatMap(Collection::stream).filter(Objects::nonNull).collect(Collectors.toList()));
                listener.onResponse(new GetUsersResponse(users));
            }, listener::onFailure);
        final GroupedActionListener<Collection<User>> groupListener =
                new GroupedActionListener<>(sendingListener, 2);
        // We have two sources for the users object, the reservedRealm and the usersStore, we query both at the same time with a
        // GroupedActionListener
        if (realmLookup.isEmpty()) {
            if (specificUsersRequested == false) {
                // we get all users from the realm
                reservedRealm.users(groupListener);
            } else {
                groupListener.onResponse(Collections.emptyList());// pass an empty list to inform the group listener
                // - no real lookups necessary
            }
        } else {
            // nested group listener action here - for each of the users we got and fetch it concurrently - once we are done we notify
            // the "global" group listener.
            GroupedActionListener<User> realmGroupListener = new GroupedActionListener<>(groupListener, realmLookup.size());
            for (String user : realmLookup) {
                reservedRealm.lookupUser(user, realmGroupListener);
            }
        }

        // user store lookups
        if (specificUsersRequested && usersToSearchFor.isEmpty()) {
            groupListener.onResponse(Collections.emptyList()); // no users requested notify
        } else {
            // go and get all users from the users store and pass it directly on to the group listener
            usersStore.getUsers(usersToSearchFor.toArray(new String[usersToSearchFor.size()]), groupListener);
        }
    }
}
