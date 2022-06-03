/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.MasterHistoryService;
import org.elasticsearch.cluster.coordination.StableMasterHealthIndicatorService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

public class StableMasterIndicatorAction extends ActionType<StableMasterIndicatorAction.Response> {

    public static final StableMasterIndicatorAction INSTANCE = new StableMasterIndicatorAction();
    public static final String NAME = "cluster:internal/stable_master/get";

    private StableMasterIndicatorAction() {
        super(NAME, StableMasterIndicatorAction.Response::new);
    }

    public static class Request extends ActionRequest {
        private final boolean explain;

        public Request(boolean explain) {
            this.explain = explain;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.explain = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(explain);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return explain == ((Request) o).explain;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(explain);
        }

    }

    public static class Response extends ActionResponse {

        private final HealthIndicatorResult healthIndicatorResult;

        public Response(StreamInput in) throws IOException {
            super(in);
            healthIndicatorResult = new HealthIndicatorResult(in);
        }

        public Response(HealthIndicatorResult healthIndicatorResult) {
            this.healthIndicatorResult = healthIndicatorResult;
        }

        public HealthIndicatorResult getHealthIndicatorResult() {
            return healthIndicatorResult;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            healthIndicatorResult.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StableMasterIndicatorAction.Response response = (StableMasterIndicatorAction.Response) o;
            return healthIndicatorResult.equals(response.healthIndicatorResult);
        }

        @Override
        public int hashCode() {
            return Objects.hash(healthIndicatorResult);
        }
    }

    /**
     * This transport action fetches the MasterHistory from a remote node.
     */
    public static class TransportAction extends HandledTransportAction<
        StableMasterIndicatorAction.Request,
        StableMasterIndicatorAction.Response> {
        private final ClusterService clusterService;
        private final TransportService transportService;
        private final Coordinator coordinator;
        private final MasterHistoryService masterHistoryService;

        @Inject
        public TransportAction(
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            Coordinator coordinator,
            MasterHistoryService masterHistoryService
        ) {
            super(StableMasterIndicatorAction.NAME, transportService, actionFilters, StableMasterIndicatorAction.Request::new);
            this.clusterService = clusterService;
            this.transportService = transportService;
            this.coordinator = coordinator;
            this.masterHistoryService = masterHistoryService;
        }

        @Override
        protected void doExecute(
            Task task,
            StableMasterIndicatorAction.Request request,
            ActionListener<StableMasterIndicatorAction.Response> listener
        ) {
            listener.onResponse(
                new Response(
                    new StableMasterHealthIndicatorService(clusterService, coordinator, masterHistoryService, transportService).calculate(
                        request.explain
                    )
                )
            );
        }
    }

}
