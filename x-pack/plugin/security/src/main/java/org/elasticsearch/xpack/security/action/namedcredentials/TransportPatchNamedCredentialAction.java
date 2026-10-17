/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.namedcredentials;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.namedcredentials.PatchNamedCredentialAction;
import org.elasticsearch.xpack.security.namedcredentials.NamedCredentialsService;

/**
 * Transport action for partially updating an existing named credential.
 */
public final class TransportPatchNamedCredentialAction extends TransportAction<
    PatchNamedCredentialAction.Request,
    PatchNamedCredentialAction.Response> {

    private final NamedCredentialsService namedCredentialsService;

    @Inject
    public TransportPatchNamedCredentialAction(
        TransportService transportService,
        ActionFilters actionFilters,
        NamedCredentialsService namedCredentialsService
    ) {
        super(PatchNamedCredentialAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.namedCredentialsService = namedCredentialsService;
    }

    @Override
    protected void doExecute(
        Task task,
        PatchNamedCredentialAction.Request request,
        ActionListener<PatchNamedCredentialAction.Response> listener
    ) {
        namedCredentialsService.patchCredential(request, listener.map(ignored -> new PatchNamedCredentialAction.Response()));
    }
}
