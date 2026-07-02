/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.xpack;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;

/**
 * ML's <code>DatafeedManager</code> invokes this api, so we need to return an empty response.
 */
public class DummyTransportGetRollupIndexCapsAction extends HandledTransportAction<
    GetRollupIndexCapsAction.Request,
    GetRollupIndexCapsAction.Response> {

    @Inject
    public DummyTransportGetRollupIndexCapsAction(TransportService transportService, ActionFilters actionFilters) {
        super(
            GetRollupIndexCapsAction.NAME,
            transportService,
            actionFilters,
            GetRollupIndexCapsAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    @Override
    protected void doExecute(
        Task task,
        GetRollupIndexCapsAction.Request request,
        ActionListener<GetRollupIndexCapsAction.Response> listener
    ) {
        listener.onResponse(new GetRollupIndexCapsAction.Response());
    }
}
