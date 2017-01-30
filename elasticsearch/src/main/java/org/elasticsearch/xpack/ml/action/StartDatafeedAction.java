/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedJobRunner;
import org.elasticsearch.xpack.ml.datafeed.DatafeedJobValidator;
import org.elasticsearch.xpack.ml.datafeed.DatafeedStatus;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobStatus;
import org.elasticsearch.xpack.ml.job.metadata.Allocation;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.persistent.PersistentActionRegistry;
import org.elasticsearch.xpack.persistent.PersistentActionRequest;
import org.elasticsearch.xpack.persistent.PersistentActionResponse;
import org.elasticsearch.xpack.persistent.PersistentActionService;
import org.elasticsearch.xpack.persistent.PersistentTask;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress;
import org.elasticsearch.xpack.persistent.TransportPersistentAction;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Predicate;

public class StartDatafeedAction
        extends Action<StartDatafeedAction.Request, PersistentActionResponse, StartDatafeedAction.RequestBuilder> {

    public static final ParseField START_TIME = new ParseField("start");
    public static final ParseField END_TIME = new ParseField("end");
    public static final ParseField START_TIMEOUT = new ParseField("start_timeout");

    public static final StartDatafeedAction INSTANCE = new StartDatafeedAction();
    public static final String NAME = "cluster:admin/ml/datafeeds/start";

    private StartDatafeedAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public PersistentActionResponse newResponse() {
        return new PersistentActionResponse();
    }

    public static class Request extends PersistentActionRequest implements ToXContent {

        public static ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, datafeedId) -> request.datafeedId = datafeedId, DatafeedConfig.ID);
            PARSER.declareLong((request, startTime) -> request.startTime = startTime, START_TIME);
            PARSER.declareLong(Request::setEndTime, END_TIME);
        }

        public static Request parseRequest(String datafeedId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (datafeedId != null) {
                request.datafeedId = datafeedId;
            }
            return request;
        }

        private String datafeedId;
        private long startTime;
        private Long endTime;

        public Request(String datafeedId, long startTime) {
            this.datafeedId = ExceptionsHelper.requireNonNull(datafeedId, DatafeedConfig.ID.getPreferredName());
            this.startTime = startTime;
        }

        public Request(StreamInput in) throws IOException {
            readFrom(in);
        }

        Request() {
        }

        public String getDatafeedId() {
            return datafeedId;
        }

        public long getStartTime() {
            return startTime;
        }

        public Long getEndTime() {
            return endTime;
        }

        public void setEndTime(Long endTime) {
            this.endTime = endTime;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId) {
            return new DatafeedTask(id, type, action, parentTaskId, datafeedId);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            datafeedId = in.readString();
            startTime = in.readVLong();
            endTime = in.readOptionalLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(datafeedId);
            out.writeVLong(startTime);
            out.writeOptionalLong(endTime);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(DatafeedConfig.ID.getPreferredName(), datafeedId);
            builder.field(START_TIME.getPreferredName(), startTime);
            if (endTime != null) {
                builder.field(END_TIME.getPreferredName(), endTime);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeedId, startTime, endTime);
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
            return Objects.equals(datafeedId, other.datafeedId) &&
                    Objects.equals(startTime, other.startTime) &&
                    Objects.equals(endTime, other.endTime);
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, PersistentActionResponse, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, StartDatafeedAction action) {
            super(client, action, new Request());
        }
    }

    public static class DatafeedTask extends PersistentTask {

        private volatile DatafeedJobRunner.Holder holder;

        public DatafeedTask(long id, String type, String action, TaskId parentTaskId, String datafeedId) {
            super(id, type, action, "datafeed-" + datafeedId, parentTaskId);
        }

        public void setHolder(DatafeedJobRunner.Holder holder) {
            this.holder = holder;
        }

        @Override
        public boolean shouldCancelChildrenOnCancellation() {
            return true;
        }

        @Override
        protected void onCancelled() {
            stop();
        }

        /* public for testing */
        public void stop() {
            if (holder == null) {
                throw new IllegalStateException("task cancel ran before datafeed runner assigned the holder");
            }
            holder.stop("cancel", null);
        }
    }

    public static class TransportAction extends TransportPersistentAction<Request> {

        private final DatafeedJobRunner datafeedJobRunner;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                               PersistentActionService persistentActionService, PersistentActionRegistry persistentActionRegistry,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               DatafeedJobRunner datafeedJobRunner) {
            super(settings, NAME, false, threadPool, transportService, persistentActionService, persistentActionRegistry,
                    actionFilters, indexNameExpressionResolver, Request::new, ThreadPool.Names.MANAGEMENT);
            this.datafeedJobRunner = datafeedJobRunner;
        }

        @Override
        public void validate(Request request, ClusterState clusterState) {
            MlMetadata mlMetadata = clusterState.metaData().custom(MlMetadata.TYPE);
            StartDatafeedAction.validate(request.getDatafeedId(), mlMetadata);
            PersistentTasksInProgress persistentTasksInProgress = clusterState.custom(PersistentTasksInProgress.TYPE);
            if (persistentTasksInProgress == null) {
                return;
            }

            Predicate<PersistentTasksInProgress.PersistentTaskInProgress<?>> predicate = taskInProgress -> {
                Request storedRequest = (Request) taskInProgress.getRequest();
                return storedRequest.getDatafeedId().equals(request.getDatafeedId());
            };
            if (persistentTasksInProgress.entriesExist(NAME, predicate)) {
                throw new ElasticsearchStatusException("datafeed already started, expected datafeed status [{}], but got [{}]",
                        RestStatus.CONFLICT, DatafeedStatus.STOPPED, DatafeedStatus.STARTED);
            }
        }

        @Override
        protected void nodeOperation(PersistentTask task, Request request, ActionListener<TransportResponse.Empty> listener) {
            DatafeedTask datafeedTask = (DatafeedTask) task;
            datafeedJobRunner.run(request.getDatafeedId(), request.getStartTime(), request.getEndTime(),
                    datafeedTask,
                    (error) -> {
                        if (error != null) {
                            listener.onFailure(error);
                        } else {
                            listener.onResponse(TransportResponse.Empty.INSTANCE);
                        }
                    });
        }

    }

    public static void validate(String datafeedId, MlMetadata mlMetadata) {
        DatafeedConfig datafeed = mlMetadata.getDatafeed(datafeedId);
        if (datafeed == null) {
            throw ExceptionsHelper.missingDatafeedException(datafeedId);
        }
        Job job = mlMetadata.getJobs().get(datafeed.getJobId());
        if (job == null) {
            throw ExceptionsHelper.missingJobException(datafeed.getJobId());
        }
        Allocation allocation = mlMetadata.getAllocations().get(datafeed.getJobId());
        if (allocation.getStatus() != JobStatus.OPENED) {
            throw new ElasticsearchStatusException("cannot start datafeed, expected job status [{}], but got [{}]",
                    RestStatus.CONFLICT, JobStatus.OPENED, allocation.getStatus());
        }
        DatafeedJobValidator.validate(datafeed, job);
    }
}
