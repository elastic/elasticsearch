/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.user.GetUsersAction;
import org.elasticsearch.xpack.core.security.action.user.GetUsersRequest;
import org.elasticsearch.xpack.core.security.action.user.GetUsersResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authc.esnative.ClientReservedRealm;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.profile.ProfileService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TransportGetUsersAction extends HandledTransportAction<GetUsersRequest, GetUsersResponse> {

    private final Settings settings;
    private final NativeUsersStore usersStore;
    private final ReservedRealm reservedRealm;
    private final Authentication.RealmRef nativeRealmRef;
    private final ProfileService profileService;

    @Inject
    public TransportGetUsersAction(
        Settings settings,
        ActionFilters actionFilters,
        NativeUsersStore usersStore,
        TransportService transportService,
        ReservedRealm reservedRealm,
        Realms realms,
        ProfileService profileService
    ) {
        super(GetUsersAction.NAME, transportService, actionFilters, GetUsersRequest::new);
        this.settings = settings;
        this.usersStore = usersStore;
        this.reservedRealm = reservedRealm;
        this.nativeRealmRef = realms.getRealmRefs()
            .values()
            .stream()
            .filter(realmRef -> NativeRealmSettings.TYPE.equals(realmRef.getType()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("native realm realm ref not found"));
        this.profileService = profileService;
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
                } else {
                    usersToSearchFor.add(username);
                }
            }
        }

        final ActionListener<Collection<Collection<User>>> sendingListener = ActionListener.wrap((userLists) -> {
            users.addAll(userLists.stream().flatMap(Collection::stream).filter(Objects::nonNull).toList());
            if (request.isWithProfileUid()) {
                resolveProfileUids(
                    users,
                    ActionListener.wrap(
                        profileUidLookup -> listener.onResponse(new GetUsersResponse(users, profileUidLookup)),
                        listener::onFailure
                    )
                );
            } else {
                listener.onResponse(new GetUsersResponse(users));
            }

        }, listener::onFailure);
        final GroupedActionListener<Collection<User>> groupListener = new GroupedActionListener<>(sendingListener, 2);
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

    private void resolveProfileUids(List<User> users, ActionListener<Map<String, String>> listener) {
        final List<Subject> subjects = users.stream().map(user -> {
            if (user instanceof AnonymousUser) {
                return new Subject(user, Authentication.RealmRef.newAnonymousRealmRef(Node.NODE_NAME_SETTING.get(settings)));
            } else if (ClientReservedRealm.isReserved(user.principal(), settings)) {
                return new Subject(user, reservedRealm.realmRef());
            } else {
                return new Subject(user, nativeRealmRef);
            }
        }).toList();

        profileService.searchProfilesForSubjects(subjects, ActionListener.wrap(resultsAndErrors -> {
            if (resultsAndErrors.errors().isEmpty()) {
                assert users.size() == resultsAndErrors.results().size();
                final Map<String, String> profileUidLookup = resultsAndErrors.results()
                    .stream()
                    .filter(t -> Objects.nonNull(t.v2()))
                    .map(t -> new Tuple<>(t.v1().getUser().principal(), t.v2().uid()))
                    .collect(Collectors.toUnmodifiableMap(Tuple::v1, Tuple::v2));
                listener.onResponse(profileUidLookup);
            } else {
                final ElasticsearchStatusException exception = new ElasticsearchStatusException(
                    "failed to retrieve profile for users. please retry without fetching profile uid (with_profile_uid=false)",
                    RestStatus.INTERNAL_SERVER_ERROR
                );
                resultsAndErrors.errors().values().forEach(exception::addSuppressed);
                listener.onFailure(exception);
            }
        }, listener::onFailure));
    }
}
