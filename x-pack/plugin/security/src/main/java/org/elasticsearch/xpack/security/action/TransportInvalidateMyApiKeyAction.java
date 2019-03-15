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
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.InvalidateMyApiKeyAction;
import org.elasticsearch.xpack.core.security.action.InvalidateMyApiKeyRequest;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

public final class TransportInvalidateMyApiKeyAction extends HandledTransportAction<InvalidateMyApiKeyRequest, InvalidateApiKeyResponse> {

    private final ApiKeyService apiKeyService;

    @Inject
    public TransportInvalidateMyApiKeyAction(TransportService transportService, ActionFilters actionFilters, ApiKeyService apiKeyService) {
        super(InvalidateMyApiKeyAction.NAME, transportService, actionFilters,
                (Writeable.Reader<InvalidateMyApiKeyRequest>) InvalidateMyApiKeyRequest::new);
        this.apiKeyService = apiKeyService;
    }

    @Override
    protected void doExecute(Task task, InvalidateMyApiKeyRequest request, ActionListener<InvalidateApiKeyResponse> listener) {
        apiKeyService.invalidateApiKeysForCurrentUser(request, listener);
    }

}
