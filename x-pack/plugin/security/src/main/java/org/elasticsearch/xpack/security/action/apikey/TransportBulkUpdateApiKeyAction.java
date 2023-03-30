/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.apikey;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.util.Set;

public final class TransportBulkUpdateApiKeyAction extends TransportBaseUpdateApiKeyAction<
    BulkUpdateApiKeyRequest,
    BulkUpdateApiKeyResponse> {

    private final ApiKeyService apiKeyService;

    @Inject
    public TransportBulkUpdateApiKeyAction(
        final TransportService transportService,
        final ActionFilters actionFilters,
        final ApiKeyService apiKeyService,
        final SecurityContext context,
        final CompositeRolesStore rolesStore,
        final NamedXContentRegistry xContentRegistry
    ) {
        super(
            BulkUpdateApiKeyAction.NAME,
            transportService,
            actionFilters,
            BulkUpdateApiKeyRequest::new,
            context,
            rolesStore,
            xContentRegistry
        );
        this.apiKeyService = apiKeyService;
    }

    @Override
    void doExecuteUpdate(
        final Task task,
        final BulkUpdateApiKeyRequest request,
        final Authentication authentication,
        final Set<RoleDescriptor> roleDescriptors,
        final ActionListener<BulkUpdateApiKeyResponse> listener
    ) {
        apiKeyService.updateApiKeys(authentication, request, roleDescriptors, listener);
    }
}
