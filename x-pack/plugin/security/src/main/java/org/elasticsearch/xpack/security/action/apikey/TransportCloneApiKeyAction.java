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
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKeyCredentials;
import org.elasticsearch.xpack.core.security.action.apikey.CloneApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CloneApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

/**
 * Transport action for cloning an API key. Validates the source credential and creates a new key
 * with the same role descriptors and creator, with a new name, id, and optional expiration/metadata.
 */
public final class TransportCloneApiKeyAction extends HandledTransportAction<CloneApiKeyRequest, CreateApiKeyResponse> {

    private final ApiKeyService apiKeyService;

    @Inject
    public TransportCloneApiKeyAction(TransportService transportService, ActionFilters actionFilters, ApiKeyService apiKeyService) {
        super(CloneApiKeyAction.NAME, transportService, actionFilters, CloneApiKeyRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.apiKeyService = apiKeyService;
    }

    @Override
    protected void doExecute(Task task, CloneApiKeyRequest request, ActionListener<CreateApiKeyResponse> listener) {
        final ApiKeyCredentials credentials;
        try {
            credentials = apiKeyService.parseCredentialsFromApiKeyString(request.getApiKey());
        } catch (IllegalArgumentException e) {
            listener.onFailure(
                new ElasticsearchStatusException("invalid API key credential: " + e.getMessage(), RestStatus.BAD_REQUEST, e)
            );
            return;
        }
        if (credentials == null) {
            listener.onFailure(new ElasticsearchStatusException("invalid API key credential", RestStatus.BAD_REQUEST));
            return;
        }

        apiKeyService.cloneApiKey(request, credentials, listener);
    }
}
