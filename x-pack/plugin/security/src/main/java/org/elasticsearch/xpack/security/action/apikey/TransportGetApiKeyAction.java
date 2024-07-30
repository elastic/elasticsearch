/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.apikey;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.profile.ProfileService;

public final class TransportGetApiKeyAction extends TransportAction<GetApiKeyRequest, GetApiKeyResponse> {

    private final ApiKeyService apiKeyService;
    private final SecurityContext securityContext;
    private final ProfileService profileService;

    @Inject
    public TransportGetApiKeyAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ApiKeyService apiKeyService,
        SecurityContext context,
        ProfileService profileService
    ) {
        super(GetApiKeyAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.apiKeyService = apiKeyService;
        this.securityContext = context;
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
                if (request.withProfileUid()) {
                    profileService.resolveProfileUidsForApiKeys(
                        apiKeyInfos,
                        ActionListener.wrap(
                            ownerProfileUids -> listener.onResponse(new GetApiKeyResponse(apiKeyInfos, ownerProfileUids)),
                            listener::onFailure
                        )
                    );
                } else {
                    listener.onResponse(new GetApiKeyResponse(apiKeyInfos, null));
                }
            }, listener::onFailure)
        );
    }

}
