/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.apikey;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.BaseUpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;

import java.util.Map;

public abstract class TransportBaseUpdateApiKeyAction<Request extends BaseUpdateApiKeyRequest, Response extends ActionResponse> extends
    HandledTransportAction<Request, Response> {

    private final SecurityContext securityContext;

    protected TransportBaseUpdateApiKeyAction(
        final String actionName,
        final TransportService transportService,
        final ActionFilters actionFilters,
        final Writeable.Reader<Request> requestReader,
        final SecurityContext context
    ) {
        super(actionName, transportService, actionFilters, requestReader);
        this.securityContext = context;
    }

    @Override
    public final void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final var authentication = securityContext.getAuthentication();
        if (authentication == null) {
            listener.onFailure(new IllegalStateException("authentication is required"));
            return;
        } else if (authentication.isApiKey()) {
            listener.onFailure(
                new IllegalArgumentException("authentication via API key not supported: only the owner user can update an API key")
            );
            return;
        }

        doExecuteUpdate(task, request, authentication, listener);
    }

    abstract void doExecuteUpdate(Task task, Request request, Authentication authentication, ActionListener<Response> listener);

    protected static UpdateApiKeyResponse toSingleResponse(final String apiKeyId, final BulkUpdateApiKeyResponse response)
        throws Exception {
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

    private static void throwIllegalStateExceptionOnIdMismatch(final String requestId, final String responseId) {
        final String message = "response ID [" + responseId + "] does not match request ID [" + requestId + "] for single API key update";
        assert false : message;
        throw new IllegalStateException(message);
    }
}
