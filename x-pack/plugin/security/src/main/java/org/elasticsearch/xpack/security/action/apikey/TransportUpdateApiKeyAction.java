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
    void doUpdate(
        final UpdateApiKeyRequest request,
        final Authentication authentication,
        final Set<RoleDescriptor> roleDescriptors,
        final ActionListener<UpdateApiKeyResponse> listener
    ) {
        apiKeyService.updateApiKeys(
            authentication,
            BulkUpdateApiKeyRequest.wrap(request),
            roleDescriptors,
            ActionListener.wrap(bulkResponse -> listener.onResponse(fromBulkResponse(request.getId(), bulkResponse)), listener::onFailure)
        );
    }

    private UpdateApiKeyResponse fromBulkResponse(final String apiKeyId, final BulkUpdateApiKeyResponse response) throws Exception {
        if (response.getErrorDetails().isEmpty() == false) {
            if (false == (response.getErrorDetails().size() == 1
                && response.getErrorDetails().containsKey(apiKeyId)
                && response.getUpdated().isEmpty()
                && response.getNoops().isEmpty())) {
                singleMatchingResponseRequiredException(apiKeyId);
            }
            throw response.getErrorDetails().values().iterator().next();
        } else if (response.getUpdated().isEmpty() == false) {
            if (false == (response.getUpdated().size() == 1
                && response.getUpdated().get(0).equals(apiKeyId)
                && response.getNoops().isEmpty())) {
                singleMatchingResponseRequiredException(apiKeyId);
            }
            return new UpdateApiKeyResponse(true);
        } else {
            if (false == (response.getNoops().size() == 1 && response.getNoops().get(0).equals(apiKeyId))) {
                singleMatchingResponseRequiredException(apiKeyId);
            }
            return new UpdateApiKeyResponse(false);
        }
    }

    private void singleMatchingResponseRequiredException(final String apiKeyId) {
        final String message = "single API key update must provide single response matching requested ID [" + apiKeyId + "]";
        assert false : message;
        throw new IllegalStateException(message);
    }
}
