/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.apikey;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authc.esnative.ClientReservedRealm;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.profile.ProfileService;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class TransportGetApiKeyAction extends TransportAction<GetApiKeyRequest, GetApiKeyResponse> {

    private final ApiKeyService apiKeyService;
    private final SecurityContext securityContext;
    private final Map<RealmConfig.RealmIdentifier, Authentication.RealmRef> realmsRefMap;
    private final ProfileService profileService;

    @Inject
    public TransportGetApiKeyAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ApiKeyService apiKeyService,
        SecurityContext context,
        Realms realms,
        ProfileService profileService
    ) {
        super(GetApiKeyAction.NAME, actionFilters, transportService.getTaskManager());
        this.apiKeyService = apiKeyService;
        this.securityContext = context;
        this.realmsRefMap = realms.getRealmRefs();
        this.profileService = profileService;
    }

    @Override
    protected void doExecute(Task task, GetApiKeyRequest request, ActionListener<GetApiKeyResponse> listener) {
        String[] apiKeyIds = Strings.hasText(request.getApiKeyId()) ? new String[] { request.getApiKeyId() } : null;
        String apiKeyName = request.getApiKeyName();
        String username = request.getUserName();
        String[] realms = Strings.hasText(request.getRealmName()) ? new String[] { request.getRealmName() } : null;

        final Authentication authentication = securityContext.getAuthentication();
        if (authentication == null) {
            listener.onFailure(new IllegalStateException("authentication is required"));
        }
        if (request.ownedByAuthenticatedUser()) {
            assert username == null;
            assert realms == null;
            // restrict username and realm to current authenticated user.
            username = authentication.getEffectiveSubject().getUser().principal();
            realms = ApiKeyService.getOwnersRealmNames(authentication);
        }
        apiKeyService.getApiKeys(
            realms,
            username,
            apiKeyName,
            apiKeyIds,
            request.withLimitedBy(),
            request.activeOnly(),
            ActionListener.wrap(apiKeyInfos -> {
                apiKeyInfos.stream().map(ApiKey::getRealmIdentifier)
            }, listener::onFailure)
        );
    }

    private List<Subject> getApiKeyCreatorSubjects(Collection<ApiKey> apiKeyInfos) {
        apiKeyInfos.stream().map(apiKeyInfo -> {
            User user = new User(apiKeyInfo.getUsername(), Strings.EMPTY_ARRAY);
            Subject subject = new Subject(user, realmsRefMap.get(apiKeyInfo.getRealmIdentifier()));

        })
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
            if (resultsAndErrors == null) {
                // profile index does not exist
                listener.onResponse(null);
            } else if (resultsAndErrors.errors().isEmpty()) {
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
