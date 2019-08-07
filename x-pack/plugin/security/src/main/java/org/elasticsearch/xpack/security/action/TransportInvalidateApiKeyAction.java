/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyResponse;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

public final class TransportInvalidateApiKeyAction extends HandledTransportAction<InvalidateApiKeyRequest, InvalidateApiKeyResponse> {

    private final ApiKeyService apiKeyService;

    @Inject
    public TransportInvalidateApiKeyAction(TransportService transportService, ActionFilters actionFilters, ApiKeyService apiKeyService) {
        super(InvalidateApiKeyAction.NAME, transportService, actionFilters,
                (Writeable.Reader<InvalidateApiKeyRequest>) InvalidateApiKeyRequest::new);
        this.apiKeyService = apiKeyService;
    }

    @Override
    protected void doExecute(Task task, InvalidateApiKeyRequest request, ActionListener<InvalidateApiKeyResponse> listener) {
        if (Strings.hasText(request.getRealmName()) || Strings.hasText(request.getUserName())) {
            apiKeyService.invalidateApiKeysForRealmAndUser(request.getRealmName(), request.getUserName(), listener);
        } else if (Strings.hasText(request.getId())) {
            apiKeyService.invalidateApiKeyForApiKeyId(request.getId(), listener);
        } else {
            apiKeyService.invalidateApiKeyForApiKeyName(request.getName(), listener);
        }
    }

}
