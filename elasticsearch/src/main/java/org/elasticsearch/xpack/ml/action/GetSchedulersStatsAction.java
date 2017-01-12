/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.ml.job.persistence.QueryPage;
import org.elasticsearch.xpack.ml.scheduler.Scheduler;
import org.elasticsearch.xpack.ml.scheduler.SchedulerConfig;
import org.elasticsearch.xpack.ml.scheduler.SchedulerStatus;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class GetSchedulersStatsAction extends Action<GetSchedulersStatsAction.Request, GetSchedulersStatsAction.Response,
        GetSchedulersStatsAction.RequestBuilder> {

    public static final GetSchedulersStatsAction INSTANCE = new GetSchedulersStatsAction();
    public static final String NAME = "cluster:admin/ml/schedulers/stats/get";

    private static final String ALL = "_all";
    private static final String STATUS = "status";

    private GetSchedulersStatsAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        private String schedulerId;

        public Request(String schedulerId) {
            this.schedulerId = ExceptionsHelper.requireNonNull(schedulerId, SchedulerConfig.ID.getPreferredName());
        }

        Request() {}

        public String getSchedulerId() {
            return schedulerId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            schedulerId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(schedulerId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schedulerId);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(schedulerId, other.schedulerId);
        }
    }

    public static class RequestBuilder extends MasterNodeReadOperationRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, GetSchedulersStatsAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public static class SchedulerStats implements ToXContent, Writeable {

            private final String schedulerId;
            private final SchedulerStatus schedulerStatus;

            SchedulerStats(String schedulerId, SchedulerStatus schedulerStatus) {
                this.schedulerId = Objects.requireNonNull(schedulerId);
                this.schedulerStatus = Objects.requireNonNull(schedulerStatus);
            }

            SchedulerStats(StreamInput in) throws IOException {
                schedulerId = in.readString();
                schedulerStatus = SchedulerStatus.fromStream(in);
            }

            public String getSchedulerId() {
                return schedulerId;
            }

            public SchedulerStatus getSchedulerStatus() {
                return schedulerStatus;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(SchedulerConfig.ID.getPreferredName(), schedulerId);
                builder.field(STATUS, schedulerStatus);
                builder.endObject();

                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(schedulerId);
                schedulerStatus.writeTo(out);
            }

            @Override
            public int hashCode() {
                return Objects.hash(schedulerId, schedulerStatus);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == null) {
                    return false;
                }
                if (getClass() != obj.getClass()) {
                    return false;
                }
                GetSchedulersStatsAction.Response.SchedulerStats other = (GetSchedulersStatsAction.Response.SchedulerStats) obj;
                return Objects.equals(schedulerId, other.schedulerId) && Objects.equals(this.schedulerStatus, other.schedulerStatus);
            }
        }

        private QueryPage<SchedulerStats> schedulersStats;

        public Response(QueryPage<SchedulerStats> schedulersStats) {
            this.schedulersStats = schedulersStats;
        }

        public Response() {}

        public QueryPage<SchedulerStats> getResponse() {
            return schedulersStats;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            schedulersStats = new QueryPage<>(in, SchedulerStats::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            schedulersStats.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            schedulersStats.doXContentBody(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(schedulersStats);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(schedulersStats, other.schedulersStats);
        }

        @SuppressWarnings("deprecation")
        @Override
        public final String toString() {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.prettyPrint();
                builder.startObject();
                toXContent(builder, EMPTY_PARAMS);
                builder.endObject();
                return builder.string();
            } catch (Exception e) {
                // So we have a stack trace logged somewhere
                return "{ \"error\" : \"" + org.elasticsearch.ExceptionsHelper.detailedMessage(e) + "\"}";
            }
        }
    }

    public static class TransportAction extends TransportMasterNodeReadAction<Request, Response> {

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ClusterService clusterService,
                ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings, GetSchedulersStatsAction.NAME, transportService, clusterService, threadPool, actionFilters,
                    indexNameExpressionResolver, Request::new);
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

        @Override
        protected void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
            logger.debug("Get stats for scheduler '{}'", request.getSchedulerId());

            List<Response.SchedulerStats> stats = new ArrayList<>();
            MlMetadata mlMetadata = state.metaData().custom(MlMetadata.TYPE);
            if (ALL.equals(request.getSchedulerId())) {
                Collection<Scheduler> schedulers = mlMetadata.getSchedulers().values();
                for (Scheduler scheduler : schedulers) {
                    stats.add(new Response.SchedulerStats(scheduler.getId(), scheduler.getStatus()));
                }
            } else {
                Scheduler scheduler = mlMetadata.getScheduler(request.getSchedulerId());
                if (scheduler == null) {
                    throw ExceptionsHelper.missingSchedulerException(request.getSchedulerId());
                }
                stats.add(new Response.SchedulerStats(scheduler.getId(), scheduler.getStatus()));
            }

            QueryPage<Response.SchedulerStats> statsPage = new QueryPage<>(stats, stats.size(), Scheduler.RESULTS_FIELD);
            listener.onResponse(new Response(statsPage));
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        }
    }
}
