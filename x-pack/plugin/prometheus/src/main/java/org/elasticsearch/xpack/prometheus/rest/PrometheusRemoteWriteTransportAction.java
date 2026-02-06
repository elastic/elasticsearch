/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite;

import java.io.IOException;

/**
 * Transport action for handling Prometheus Remote Write requests.
 * This action processes incoming metrics data in Prometheus remote write format.
 *
 * @see <a href="https://prometheus.io/docs/concepts/remote_write_spec/">Prometheus Remote Write Specification</a>
 */
public class PrometheusRemoteWriteTransportAction extends HandledTransportAction<
    PrometheusRemoteWriteTransportAction.RemoteWriteRequest,
    PrometheusRemoteWriteTransportAction.RemoteWriteResponse> {

    public static final String NAME = "indices:data/write/prometheus/remote_write";
    public static final ActionType<RemoteWriteResponse> TYPE = new ActionType<>(NAME);

    private static final Logger logger = LogManager.getLogger(PrometheusRemoteWriteTransportAction.class);

    @Inject
    public PrometheusRemoteWriteTransportAction(TransportService transportService, ActionFilters actionFilters, ThreadPool threadPool) {
        super(NAME, transportService, actionFilters, RemoteWriteRequest::new, threadPool.executor(ThreadPool.Names.WRITE));
    }

    @Override
    protected void doExecute(Task task, RemoteWriteRequest request, ActionListener<RemoteWriteResponse> listener) {
        try {
            RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.parseFrom(request.remoteWriteRequest.streamInput());

            // Log the received data for debugging (skeleton implementation)
            logger.debug("Received Prometheus remote write request with {} timeseries", writeRequest.getTimeseriesCount());

            // TODO: Process and store the timeseries data
            // For now, just acknowledge receipt with success

            listener.onResponse(new RemoteWriteResponse(RestStatus.NO_CONTENT));
        } catch (Exception e) {
            logger.error("Failed to process Prometheus remote write request", e);
            listener.onFailure(e);
        }
    }

    public static class RemoteWriteRequest extends ActionRequest implements CompositeIndicesRequest {
        private final BytesReference remoteWriteRequest;

        public RemoteWriteRequest(StreamInput in) throws IOException {
            super(in);
            remoteWriteRequest = in.readBytesReference();
        }

        public RemoteWriteRequest(BytesReference remoteWriteRequest) {
            this.remoteWriteRequest = remoteWriteRequest;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBytesReference(remoteWriteRequest);
        }
    }

    public static class RemoteWriteResponse extends ActionResponse {
        private final RestStatus status;

        public RemoteWriteResponse(RestStatus status) {
            this.status = status;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(status);
        }

        public RestStatus getStatus() {
            return status;
        }
    }
}
