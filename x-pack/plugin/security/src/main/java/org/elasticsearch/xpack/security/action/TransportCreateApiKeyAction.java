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
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.support.ApiKeyGenerator;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

/**
 * Implementation of the action needed to create an API key
 */
public final class TransportCreateApiKeyAction extends HandledTransportAction<CreateApiKeyRequest, CreateApiKeyResponse> {

    private final ApiKeyGenerator generator;
    private final SecurityContext securityContext;

    @Inject
    public TransportCreateApiKeyAction(TransportService transportService, ActionFilters actionFilters, ApiKeyService apiKeyService,
                                       SecurityContext context, CompositeRolesStore rolesStore, NamedXContentRegistry xContentRegistry) {
        super(CreateApiKeyAction.NAME, transportService, actionFilters, CreateApiKeyRequest::new);
        this.generator = new ApiKeyGenerator(apiKeyService, rolesStore, xContentRegistry);
        this.securityContext = context;
    }

    @Override
    protected void doExecute(Task task, CreateApiKeyRequest request, ActionListener<CreateApiKeyResponse> listener) {
        final Authentication authentication = securityContext.getAuthentication();
        if (authentication == null) {
            listener.onFailure(new IllegalStateException("authentication is required"));
        } else {
            if (Authentication.AuthenticationType.API_KEY == authentication.getAuthenticationType() && grantsAnyPrivileges(request)) {
                listener.onFailure(new IllegalArgumentException(
                    "creating derived api keys requires an explicit role descriptor that is empty (has no privileges)"));
                return;
            }
            generator.generateApiKey(authentication, request, listener);
        }
    }

    private boolean grantsAnyPrivileges(CreateApiKeyRequest request) {
        return request.getRoleDescriptors() == null
            || request.getRoleDescriptors().isEmpty()
            || false == request.getRoleDescriptors().stream().allMatch(RoleDescriptor::isEmpty);
    }
}
