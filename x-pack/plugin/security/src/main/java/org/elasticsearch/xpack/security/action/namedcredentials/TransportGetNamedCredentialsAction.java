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
import org.elasticsearch.xpack.core.security.action.namedcredentials.GetNamedCredentialsAction;
import org.elasticsearch.xpack.security.namedcredentials.NamedCredentialsService;

/**
 * Transport action for fetching one or all named credentials (auth block redacted).
 */
public final class TransportGetNamedCredentialsAction extends TransportAction<
    GetNamedCredentialsAction.Request,
    GetNamedCredentialsAction.Response> {

    private final NamedCredentialsService namedCredentialsService;

    @Inject
    public TransportGetNamedCredentialsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        NamedCredentialsService namedCredentialsService
    ) {
        super(GetNamedCredentialsAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.namedCredentialsService = namedCredentialsService;
    }

    @Override
    protected void doExecute(
        Task task,
        GetNamedCredentialsAction.Request request,
        ActionListener<GetNamedCredentialsAction.Response> listener
    ) {
        final boolean single = request.credentialName() != null;
        namedCredentialsService.getCredentials(
            request.credentialName(),
            listener.map(credentials -> new GetNamedCredentialsAction.Response(credentials, single))
        );
    }
}
