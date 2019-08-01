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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.GetApiKeyAction;
import org.elasticsearch.xpack.core.security.action.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.GetApiKeyResponse;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

public final class TransportGetApiKeyAction extends HandledTransportAction<GetApiKeyRequest,GetApiKeyResponse> {

    private final ApiKeyService apiKeyService;

    @Inject
    public TransportGetApiKeyAction(TransportService transportService, ActionFilters actionFilters, ApiKeyService apiKeyService) {
        super(GetApiKeyAction.NAME, transportService, actionFilters,
                (Writeable.Reader<GetApiKeyRequest>) GetApiKeyRequest::new);
        this.apiKeyService = apiKeyService;
    }

    @Override
    protected void doExecute(Task task, GetApiKeyRequest request, ActionListener<GetApiKeyResponse> listener) {
        apiKeyService.getApiKeys(request.getRealmName(), request.getUserName(), request.getApiKeyName(), request.getApiKeyId(), listener);
    }

}
