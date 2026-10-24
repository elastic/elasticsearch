/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.namedcredentials;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.namedcredentials.DeleteNamedCredentialAction;
import org.elasticsearch.xpack.security.namedcredentials.NamedCredentialsService;

/**
 * Transport action for deleting a named credential by name.
 */
public final class TransportDeleteNamedCredentialAction extends TransportAction<DeleteNamedCredentialAction.Request, AcknowledgedResponse> {

    private final NamedCredentialsService namedCredentialsService;

    @Inject
    public TransportDeleteNamedCredentialAction(
        TransportService transportService,
        ActionFilters actionFilters,
        NamedCredentialsService namedCredentialsService
    ) {
        super(DeleteNamedCredentialAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.namedCredentialsService = namedCredentialsService;
    }

    @Override
    protected void doExecute(Task task, DeleteNamedCredentialAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        namedCredentialsService.deleteCredential(request.credentialName(), listener.map(found -> {
            if (found == false) {
                throw new ResourceNotFoundException("named credential [" + request.credentialName() + "] not found");
            }
            return AcknowledgedResponse.TRUE;
        }));
    }
}
