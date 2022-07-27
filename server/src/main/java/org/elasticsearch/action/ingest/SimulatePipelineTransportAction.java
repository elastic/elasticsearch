/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

public class SimulatePipelineTransportAction extends HandledTransportAction<SimulatePipelineRequest, SimulatePipelineResponse> {

    private final IngestService ingestService;
    private final SimulateExecutionService executionService;

    @Inject
    public SimulatePipelineTransportAction(
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        IngestService ingestService
    ) {
        super(SimulatePipelineAction.NAME, transportService, actionFilters, SimulatePipelineRequest::new);
        this.ingestService = ingestService;
        this.executionService = new SimulateExecutionService(threadPool);
    }

    @Override
    protected void doExecute(Task task, SimulatePipelineRequest request, ActionListener<SimulatePipelineResponse> listener) {
        final Map<String, Object> source = XContentHelper.convertToMap(request.getSource(), false, request.getXContentType()).v2();

        final SimulatePipelineRequest.Parsed simulateRequest;
        try {
            if (request.getId() != null) {
                simulateRequest = SimulatePipelineRequest.parseWithPipelineId(
                    request.getId(),
                    source,
                    request.isVerbose(),
                    ingestService,
                    request.getRestApiVersion()
                );
            } else {
                simulateRequest = SimulatePipelineRequest.parse(source, request.isVerbose(), ingestService, request.getRestApiVersion());
            }
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        executionService.execute(simulateRequest, listener);
    }
}
