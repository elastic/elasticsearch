/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.metrics;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class MetricsTransportAction extends HandledTransportAction<
    MetricsTransportAction.MetricsRequest,
    MetricsTransportAction.MetricsResponse> {

    public static final String NAME = "indices:data/write/metrics";
    public static final ActionType<MetricsTransportAction.MetricsResponse> TYPE = new ActionType<>(NAME);

    private static final Logger logger = LogManager.getLogger(MetricsTransportAction.class);

    private final ProjectResolver projectResolver;
    private final IndicesService indicesService;

    @Inject
    public MetricsTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndicesService indicesService
    ) {
        super(NAME, transportService, actionFilters, MetricsRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.projectResolver = projectResolver;
        this.indicesService = indicesService;
    }

    @Override
    protected void doExecute(Task task, MetricsRequest request, ActionListener<MetricsResponse> listener) {
        try {
            var metricsServiceRequest = ExportMetricsServiceRequest.parseFrom(request.exportMetricsServiceRequest.streamInput());

            logger.info("Received " + metricsServiceRequest.getResourceMetricsCount() + " metrics");

            // resolve index somehow
            logger.info("Indices service " + indicesService);

            listener.onResponse(new MetricsResponse());
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public static class MetricsRequest extends ActionRequest {
        private String index;
        private BytesReference exportMetricsServiceRequest;

        public MetricsRequest(StreamInput in) throws IOException {
            super(in);
            index = in.readString();
            exportMetricsServiceRequest = in.readBytesReference();
        }

        public MetricsRequest(String index, BytesReference exportMetricsServiceRequest) {
            this.index = index;
            this.exportMetricsServiceRequest = exportMetricsServiceRequest;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class MetricsResponse extends ActionResponse {
        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }
}
