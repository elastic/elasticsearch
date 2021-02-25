/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

public final class TransportInvalidateApiKeyAction extends HandledTransportAction<InvalidateApiKeyRequest, InvalidateApiKeyResponse> {

    private final ApiKeyService apiKeyService;
    private final SecurityContext securityContext;

    @Inject
    public TransportInvalidateApiKeyAction(TransportService transportService, ActionFilters actionFilters, ApiKeyService apiKeyService,
                                           SecurityContext context) {
        super(InvalidateApiKeyAction.NAME, transportService, actionFilters,
            (Writeable.Reader<InvalidateApiKeyRequest>) InvalidateApiKeyRequest::new);
        this.apiKeyService = apiKeyService;
        this.securityContext = context;
    }

    @Override
    protected void doExecute(Task task, InvalidateApiKeyRequest request, ActionListener<InvalidateApiKeyResponse> listener) {
        String[] apiKeyIds = request.getIds();
        String apiKeyName = request.getName();
        String username = request.getUserName();
        String realm = request.getRealmName();

        final Authentication authentication = securityContext.getAuthentication();
        if (authentication == null) {
            listener.onFailure(new IllegalStateException("authentication is required"));
        }
        if (request.ownedByAuthenticatedUser()) {
            assert username == null;
            assert realm == null;
            // restrict username and realm to current authenticated user.
            username = authentication.getUser().principal();
            realm = ApiKeyService.getCreatorRealmName(authentication);
        }

        apiKeyService.invalidateApiKeys(realm, username, apiKeyName, apiKeyIds, listener);
    }

}
