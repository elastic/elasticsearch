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
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.util.Map;
import java.util.Set;

public final class TransportUpdateApiKeyAction extends TransportBaseUpdateApiKeyAction<UpdateApiKeyRequest, UpdateApiKeyResponse> {

    private final ApiKeyService apiKeyService;

    @Inject
    public TransportUpdateApiKeyAction(
        final TransportService transportService,
        final ActionFilters actionFilters,
        final ApiKeyService apiKeyService,
        final SecurityContext context,
        final CompositeRolesStore rolesStore,
        final NamedXContentRegistry xContentRegistry
    ) {
        super(UpdateApiKeyAction.NAME, transportService, actionFilters, UpdateApiKeyRequest::new, context, rolesStore, xContentRegistry);
        this.apiKeyService = apiKeyService;
    }

    @Override
    void doExecuteUpdate(
        final Task task,
        final UpdateApiKeyRequest request,
        final Authentication authentication,
        final Set<RoleDescriptor> roleDescriptors,
        final ActionListener<UpdateApiKeyResponse> listener
    ) {
        apiKeyService.updateApiKeys(
            authentication,
            BulkUpdateApiKeyRequest.wrap(request),
            roleDescriptors,
            ActionListener.wrap(bulkResponse -> listener.onResponse(toSingleResponse(request.getId(), bulkResponse)), listener::onFailure)
        );
    }

    private UpdateApiKeyResponse toSingleResponse(final String apiKeyId, final BulkUpdateApiKeyResponse response) throws Exception {
        if (response.getTotalResultCount() != 1) {
            throw new IllegalStateException(
                "single result required for single API key update but result count was [" + response.getTotalResultCount() + "]"
            );
        }
        if (response.getErrorDetails().isEmpty() == false) {
            final Map.Entry<String, Exception> errorEntry = response.getErrorDetails().entrySet().iterator().next();
            if (errorEntry.getKey().equals(apiKeyId) == false) {
                throwIllegalStateExceptionOnIdMismatch(apiKeyId, errorEntry.getKey());
            }
            throw errorEntry.getValue();
        } else if (response.getUpdated().isEmpty() == false) {
            final String updatedId = response.getUpdated().get(0);
            if (updatedId.equals(apiKeyId) == false) {
                throwIllegalStateExceptionOnIdMismatch(apiKeyId, updatedId);
            }
            return new UpdateApiKeyResponse(true);
        } else {
            final String noopId = response.getNoops().get(0);
            if (noopId.equals(apiKeyId) == false) {
                throwIllegalStateExceptionOnIdMismatch(apiKeyId, noopId);
            }
            return new UpdateApiKeyResponse(false);
        }
    }

    private void throwIllegalStateExceptionOnIdMismatch(final String requestId, final String responseId) {
        final String message = "response ID [" + responseId + "] does not match request ID [" + requestId + "] for single API key update";
        assert false : message;
        throw new IllegalStateException(message);
    }
}
