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
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.BaseBulkUpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateCrossClusterApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateCrossClusterApiKeyRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

import java.util.List;
import java.util.Set;

public final class TransportUpdateCrossClusterApiKeyAction extends TransportBaseUpdateApiKeyAction<
    UpdateCrossClusterApiKeyRequest,
    UpdateApiKeyResponse> {

    private final ApiKeyService apiKeyService;

    @Inject
    public TransportUpdateCrossClusterApiKeyAction(
        final TransportService transportService,
        final ActionFilters actionFilters,
        final ApiKeyService apiKeyService,
        final SecurityContext context
    ) {
        super(UpdateCrossClusterApiKeyAction.NAME, transportService, actionFilters, UpdateCrossClusterApiKeyRequest::new, context);
        this.apiKeyService = apiKeyService;
    }

    @Override
    void doExecuteUpdate(
        final Task task,
        final UpdateCrossClusterApiKeyRequest request,
        final Authentication authentication,
        final ActionListener<UpdateApiKeyResponse> listener
    ) {
        apiKeyService.updateApiKeys(
            authentication,
            new BaseBulkUpdateApiKeyRequest(List.of(request.getId()), request.getRoleDescriptors(), request.getMetadata()) {
                @Override
                public ApiKey.Type getType() {
                    return ApiKey.Type.CROSS_CLUSTER;
                }
            },
            Set.of(),
            ActionListener.wrap(bulkResponse -> listener.onResponse(toSingleResponse(request.getId(), bulkResponse)), listener::onFailure)
        );
    }
}
