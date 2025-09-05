/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sample;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.ingest.SamplingService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

public class TransportGetSampleAction extends HandledTransportAction<GetSampleAction.Request, GetSampleAction.Response> {
    private final SamplingService samplingService;

    @Inject
    public TransportGetSampleAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        SamplingService samplingService
    ) {
        super(GetSampleAction.NAME, transportService, actionFilters, GetSampleAction.Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.samplingService = samplingService;
    }

    @Override
    protected void doExecute(Task task, GetSampleAction.Request request, ActionListener<GetSampleAction.Response> listener) {
        String index = request.indices()[0];
        List<IndexRequest> samples = samplingService.getSamples(index);
        GetSampleAction.Response response = new GetSampleAction.Response(samples);
        listener.onResponse(response);
    }
}
