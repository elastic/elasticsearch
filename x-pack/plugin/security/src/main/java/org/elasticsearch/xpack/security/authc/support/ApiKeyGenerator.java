/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.support.DLSRoleQueryValidator;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ApiKeyGenerator {

    private final ApiKeyService apiKeyService;
    private final CompositeRolesStore rolesStore;
    private final NamedXContentRegistry xContentRegistry;

    public ApiKeyGenerator(ApiKeyService apiKeyService, CompositeRolesStore rolesStore, NamedXContentRegistry xContentRegistry) {
        this.apiKeyService = apiKeyService;
        this.rolesStore = rolesStore;
        this.xContentRegistry = xContentRegistry;
    }

    public void generateApiKey(Authentication authentication, CreateApiKeyRequest request, ActionListener<CreateApiKeyResponse> listener) {
        if (authentication == null) {
            listener.onFailure(new ElasticsearchSecurityException("no authentication available to generate API key"));
            return;
        }
        apiKeyService.ensureEnabled();

        final ActionListener<Set<RoleDescriptor>> roleDescriptorsListener = ActionListener.wrap(roleDescriptors -> {
            for (RoleDescriptor rd : roleDescriptors) {
                try {
                    DLSRoleQueryValidator.validateQueryField(rd.getIndicesPrivileges(), xContentRegistry);
                } catch (ElasticsearchException | IllegalArgumentException e) {
                    listener.onFailure(e);
                    return;
                }
            }
            apiKeyService.createApiKey(authentication, request, roleDescriptors, listener);
        }, listener::onFailure);

        if (ServiceAccountSettings.REALM_NAME.equals(authentication.getSourceRealm().getName())) {
            final ServiceAccount serviceAccount = ServiceAccountService.getServiceAccounts().get(authentication.getUser().principal());
            if (serviceAccount == null) {
                roleDescriptorsListener.onFailure(new ElasticsearchSecurityException(
                    "the authentication is created by a service account that does not exist: ["
                        + authentication.getUser().principal() + "]"));
            } else {
                roleDescriptorsListener.onResponse(Set.of(serviceAccount.roleDescriptor()));
            }
        } else {
            rolesStore.getRoleDescriptors(new HashSet<>(Arrays.asList(authentication.getUser().roles())), roleDescriptorsListener);
        }
    }

}
