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
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.profile.ProfileService;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class TransportGetApiKeyAction extends TransportAction<GetApiKeyRequest, GetApiKeyResponse> {

    private final ApiKeyService apiKeyService;
    private final SecurityContext securityContext;
    private final ProfileService profileService;
    private final Function<RealmConfig.RealmIdentifier, Authentication.RealmRef> getRealmRef;

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
        this.profileService = profileService;
        this.getRealmRef = realms::getRealmRef;
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
                if (request.isWithProfileUid()) {
                    resolveProfileUids(
                        apiKeyInfos,
                        ActionListener.wrap(
                            apiKeyInfosWithProfileUid -> listener.onResponse(new GetApiKeyResponse(apiKeyInfosWithProfileUid)),
                            listener::onFailure
                        )
                    );
                } else {
                    listener.onResponse(
                        new GetApiKeyResponse(apiKeyInfos.stream().map(apiKeyInfo -> new ApiKey.WithProfileUid(apiKeyInfo, null)).toList())
                    );
                }
            }, listener::onFailure)
        );
    }

    private void resolveProfileUids(Collection<ApiKey> apiKeyInfos, ActionListener<Collection<ApiKey.WithProfileUid>> listener) {
        List<Subject> subjects = apiKeyInfos.stream()
            .map(this::getApiKeyCreatorSubject)
            .filter(Objects::nonNull)
            .distinct()
            .collect(Collectors.toList());
        profileService.searchProfilesForSubjects(subjects, ActionListener.wrap(resultsAndErrors -> {
            if (resultsAndErrors == null) {
                // profile index does not exist
                listener.onResponse(apiKeyInfos.stream().map(apiKeyInfo -> new ApiKey.WithProfileUid(apiKeyInfo, null)).toList());
            } else if (resultsAndErrors.errors().isEmpty()) {
                assert subjects.size() == resultsAndErrors.results().size();
                Map<Subject, String> profileUidLookup = resultsAndErrors.results()
                    .stream()
                    .filter(t -> Objects.nonNull(t.v2()))
                    .map(t -> new Tuple<>(t.v1(), t.v2().uid()))
                    .collect(Collectors.toUnmodifiableMap(Tuple::v1, Tuple::v2));
                listener.onResponse(
                    apiKeyInfos.stream()
                        .map(
                            apiKeyInfo -> new ApiKey.WithProfileUid(
                                apiKeyInfo,
                                getApiKeyCreatorSubject(apiKeyInfo) == null
                                    ? null
                                    : profileUidLookup.get(getApiKeyCreatorSubject(apiKeyInfo))
                            )
                        )
                        .toList()
                );
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

    private Subject getApiKeyCreatorSubject(ApiKey apiKeyInfo) {
        Authentication.RealmRef realmRef = getRealmRef.apply(apiKeyInfo.getRealmIdentifier());
        if (realmRef == null) {
            return null;
        }
        return new Subject(new User(apiKeyInfo.getUsername(), Strings.EMPTY_ARRAY), realmRef);
    }
}
