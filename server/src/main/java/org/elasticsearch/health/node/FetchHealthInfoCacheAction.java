/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.health.node.action.HealthNodeRequest;
import org.elasticsearch.health.node.action.TransportHealthNodeAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

/**
 * This action retrieves all the HealthInfo data from the health node. It is meant to be used when a user makes a health API request. The
 * data that this action retrieves is populated by UpdateHealthInfoCacheAction.
 */
public class FetchHealthInfoCacheAction extends ActionType<FetchHealthInfoCacheAction.Response> {

    public static class Request extends HealthNodeRequest {
        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String getDescription() {
            return "Fetching health information from the health node.";
        }
    }

    public static class Response extends ActionResponse {
        private final HealthInfo healthInfo;

        public Response(final HealthInfo healthInfo) {
            this.healthInfo = healthInfo;
        }

        public Response(StreamInput input) throws IOException {
            this.healthInfo = new HealthInfo(input);
        }

        @Override
        public void writeTo(StreamOutput output) throws IOException {
            this.healthInfo.writeTo(output);
        }

        public HealthInfo getHealthInfo() {
            return healthInfo;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FetchHealthInfoCacheAction.Response response = (FetchHealthInfoCacheAction.Response) o;
            return healthInfo.equals(response.healthInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(healthInfo);
        }
    }

    public static final FetchHealthInfoCacheAction INSTANCE = new FetchHealthInfoCacheAction();
    public static final String NAME = "cluster:monitor/fetch/health/info";

    private FetchHealthInfoCacheAction() {
        super(NAME);
    }

    public static class TransportAction extends TransportHealthNodeAction<
        FetchHealthInfoCacheAction.Request,
        FetchHealthInfoCacheAction.Response> {
        private final HealthInfoCache nodeHealthOverview;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            HealthInfoCache nodeHealthOverview
        ) {
            super(
                FetchHealthInfoCacheAction.NAME,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                FetchHealthInfoCacheAction.Request::new,
                FetchHealthInfoCacheAction.Response::new,
                threadPool.executor(ThreadPool.Names.MANAGEMENT)
            );
            this.nodeHealthOverview = nodeHealthOverview;
        }

        @Override
        protected void healthOperation(
            Task task,
            FetchHealthInfoCacheAction.Request request,
            ClusterState clusterState,
            ActionListener<FetchHealthInfoCacheAction.Response> listener
        ) {
            listener.onResponse(new Response(nodeHealthOverview.getHealthInfo()));
        }
    }
}
