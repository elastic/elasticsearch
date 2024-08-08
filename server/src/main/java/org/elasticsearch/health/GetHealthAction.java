/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.health.stats.HealthApiStats;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetHealthAction extends ActionType<GetHealthAction.Response> {

    public static final GetHealthAction INSTANCE = new GetHealthAction();
    public static final String NAME = "cluster:monitor/health_api";

    private GetHealthAction() {
        super(NAME);
    }

    public static class Response extends ActionResponse implements ChunkedToXContent {

        private final ClusterName clusterName;
        @Nullable
        private final HealthStatus status;
        private final List<HealthIndicatorResult> indicators;

        public Response(final ClusterName clusterName, final List<HealthIndicatorResult> indicators, boolean showTopLevelStatus) {
            this.indicators = indicators;
            this.clusterName = clusterName;
            if (showTopLevelStatus) {
                this.status = HealthStatus.merge(indicators.stream().map(HealthIndicatorResult::status));
            } else {
                this.status = null;
            }
        }

        public Response(final ClusterName clusterName, final List<HealthIndicatorResult> indicators, HealthStatus topLevelStatus) {
            this.indicators = indicators;
            this.clusterName = clusterName;
            this.status = topLevelStatus;
        }

        public ClusterName getClusterName() {
            return clusterName;
        }

        public HealthStatus getStatus() {
            return status;
        }

        public HealthIndicatorResult findIndicator(String name) {
            return indicators.stream()
                .filter(c -> Objects.equals(c.name(), name))
                .findFirst()
                .orElseThrow(() -> new NoSuchElementException("Indicator [" + name + "] is not found"));
        }

        public List<HealthIndicatorResult> getIndicatorResults() {
            return indicators;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }

        @Override
        @SuppressWarnings("unchecked")
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
            return Iterators.concat(Iterators.single((ToXContent) (builder, params) -> {
                builder.startObject();
                if (status != null) {
                    builder.field("status", status.xContentValue());
                }
                builder.field("cluster_name", clusterName.value());
                builder.startObject("indicators");
                return builder;
            }),
                Iterators.concat(
                    indicators.stream()
                        .map(
                            indicator -> Iterators.concat(
                                // having the indicator name printed here prevents us from flat mapping all
                                // indicators however the affected resources which are the O(indices) fields are
                                // flat mapped over all diagnoses within the indicator
                                Iterators.single((ToXContent) (builder, params) -> builder.field(indicator.name())),
                                indicator.toXContentChunked(outerParams)
                            )
                        )
                        .toArray(Iterator[]::new)
                ),
                Iterators.single((b, p) -> b.endObject().endObject())
            );
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Response response = (Response) o;
            return clusterName.equals(response.clusterName) && status == response.status && indicators.equals(response.indicators);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clusterName, status, indicators);
        }

        @Override
        public String toString() {
            return "Response{clusterName=" + clusterName + ", status=" + status + ", indicatorResults=" + indicators + '}';
        }
    }

    public static class Request extends ActionRequest {
        private final String indicatorName;
        private final boolean verbose;
        private final int size;

        public Request(boolean verbose, int size) {
            this(null, verbose, size);
        }

        public Request(String indicatorName, boolean verbose, int size) {
            this.indicatorName = indicatorName;
            this.verbose = verbose;
            this.size = size;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (size < 0) {
                validationException = addValidationError("The size parameter must be a positive integer", validationException);
            }
            return validationException;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }
    }

    public static class LocalAction extends TransportAction<Request, Response> {

        private final ClusterService clusterService;
        private final HealthService healthService;
        private final NodeClient client;
        private final HealthApiStats healthApiStats;

        @Inject
        public LocalAction(
            ActionFilters actionFilters,
            TransportService transportService,
            ClusterService clusterService,
            HealthService healthService,
            NodeClient client,
            HealthApiStats healthApiStats
        ) {
            super(NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
            this.clusterService = clusterService;
            this.healthService = healthService;
            this.client = client;
            this.healthApiStats = healthApiStats;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> responseListener) {
            assert task instanceof CancellableTask;
            final CancellableTask cancellableTask = (CancellableTask) task;
            if (cancellableTask.notifyIfCancelled(responseListener)) {
                return;
            }

            healthService.getHealth(
                new ParentTaskAssigningClient(client, clusterService.localNode(), task),
                request.indicatorName,
                request.verbose,
                request.size,
                responseListener.map(healthIndicatorResults -> {
                    Response response = new Response(
                        clusterService.getClusterName(),
                        healthIndicatorResults,
                        request.indicatorName == null
                    );
                    healthApiStats.track(request.verbose, response);
                    return response;
                })
            );
        }
    }
}
