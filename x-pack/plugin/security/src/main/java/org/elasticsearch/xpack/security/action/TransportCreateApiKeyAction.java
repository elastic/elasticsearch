/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.util.ArrayList;

/**
 * Implementation of the action needed to create an API key
 */
public final class TransportCreateApiKeyAction extends HandledTransportAction<CreateApiKeyRequest, CreateApiKeyResponse> {

    private final ApiKeyService apiKeyService;
    private final CompositeRolesStore compositeRolesStore;
    private final SecurityContext securityContext;

    @Inject
    public TransportCreateApiKeyAction(Settings settings, TransportService transportService, ActionFilters actionFilters,
                                       ApiKeyService apiKeyService, CompositeRolesStore compositeRolesStore, SecurityContext context) {
        super(settings, CreateApiKeyAction.NAME, transportService, actionFilters,
            (Writeable.Reader<CreateApiKeyRequest>) CreateApiKeyRequest::new);
        this.apiKeyService = apiKeyService;
        this.compositeRolesStore = compositeRolesStore;
        this.securityContext = context;
    }

    @Override
    protected void doExecute(Task task, CreateApiKeyRequest request, ActionListener<CreateApiKeyResponse> listener) {
        final Authentication authentication = securityContext.getAuthentication();
        if (authentication == null) {
            listener.onFailure(new IllegalStateException("authentication is required"));
        } else if (request.getRoleDescriptors() == null || request.getRoleDescriptors().isEmpty()) {
            compositeRolesStore.getRoleDescriptors(Sets.newHashSet(authentication.getUser().roles()),
                ActionListener.wrap(rdSet -> {
                    request.setRoleDescriptors(new ArrayList<>(rdSet));
                    apiKeyService.createApiKey(authentication, request, listener);
                }, listener::onFailure));
        } else {
            apiKeyService.createApiKey(authentication, request, listener);
        }
    }
}
