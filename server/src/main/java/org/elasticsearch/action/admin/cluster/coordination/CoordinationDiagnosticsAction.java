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
import org.elasticsearch.cluster.coordination.CoordinationDiagnosticsService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.cluster.coordination.CoordinationDiagnosticsService.CoordinationDiagnosticsResult;

/**
 * This action exposes CoordinationDiagnosticsService#diagnoseMasterStability so that a node can get a remote node's view of
 * coordination diagnostics (including master stability).
 */
public class CoordinationDiagnosticsAction extends ActionType<CoordinationDiagnosticsAction.Response> {

    public static final CoordinationDiagnosticsAction INSTANCE = new CoordinationDiagnosticsAction();
    public static final String NAME = "internal:cluster/coordination_diagnostics/info";

    private CoordinationDiagnosticsAction() {
        super(NAME, CoordinationDiagnosticsAction.Response::new);
    }

    public static class Request extends ActionRequest {
        final boolean explain; // Non-private for testing

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

        private final CoordinationDiagnosticsResult result;

        public Response(StreamInput in) throws IOException {
            super(in);
            result = new CoordinationDiagnosticsResult(in);
        }

        public Response(CoordinationDiagnosticsResult result) {
            this.result = result;
        }

        public CoordinationDiagnosticsResult getCoordinationDiagnosticsResult() {
            return result;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            result.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CoordinationDiagnosticsAction.Response response = (CoordinationDiagnosticsAction.Response) o;
            return result.equals(response.result);
        }

        @Override
        public int hashCode() {
            return Objects.hash(result);
        }
    }

    /**
     * This transport action calls CoordinationDiagnosticsService#diagnoseMasterStability
     */
    public static class TransportAction extends HandledTransportAction<Request, Response> {
        private final CoordinationDiagnosticsService coordinationDiagnosticsService;

        @Inject
        public TransportAction(
            TransportService transportService,
            ActionFilters actionFilters,
            CoordinationDiagnosticsService coordinationDiagnosticsService
        ) {
            super(CoordinationDiagnosticsAction.NAME, transportService, actionFilters, CoordinationDiagnosticsAction.Request::new);
            this.coordinationDiagnosticsService = coordinationDiagnosticsService;
        }

        @Override
        protected void doExecute(Task task, CoordinationDiagnosticsAction.Request request, ActionListener<Response> listener) {
            listener.onResponse(new Response(coordinationDiagnosticsService.diagnoseMasterStability(request.explain)));
        }
    }

}
