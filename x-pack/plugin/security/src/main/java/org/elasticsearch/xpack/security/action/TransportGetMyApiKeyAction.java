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
import org.elasticsearch.xpack.core.security.action.GetApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.GetMyApiKeyAction;
import org.elasticsearch.xpack.core.security.action.GetMyApiKeyRequest;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

public final class TransportGetMyApiKeyAction extends HandledTransportAction<GetMyApiKeyRequest, GetApiKeyResponse> {

    private final ApiKeyService apiKeyService;

    @Inject
    public TransportGetMyApiKeyAction(TransportService transportService, ActionFilters actionFilters, ApiKeyService apiKeyService) {
        super(GetMyApiKeyAction.NAME, transportService, actionFilters, (Writeable.Reader<GetMyApiKeyRequest>) GetMyApiKeyRequest::new);
        this.apiKeyService = apiKeyService;
    }

    @Override
    protected void doExecute(Task task, GetMyApiKeyRequest request, ActionListener<GetApiKeyResponse> listener) {
        apiKeyService.getApiKeysForCurrentUser(request, listener);
    }

}
