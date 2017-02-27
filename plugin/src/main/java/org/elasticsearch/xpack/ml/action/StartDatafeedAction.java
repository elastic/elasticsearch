/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedJobRunner;
import org.elasticsearch.xpack.ml.datafeed.DatafeedJobValidator;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.elasticsearch.xpack.ml.utils.DatafeedStateObserver;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.persistent.PersistentActionRegistry;
import org.elasticsearch.xpack.persistent.PersistentActionRequest;
import org.elasticsearch.xpack.persistent.PersistentActionResponse;
import org.elasticsearch.xpack.persistent.PersistentActionService;
import org.elasticsearch.xpack.persistent.NodePersistentTask;
import org.elasticsearch.xpack.persistent.PersistentTasks;
import org.elasticsearch.xpack.persistent.PersistentTasks.Assignment;
import org.elasticsearch.xpack.persistent.PersistentTasks.PersistentTask;
import org.elasticsearch.xpack.persistent.TransportPersistentAction;

import java.io.IOException;
import java.util.Objects;

public class StartDatafeedAction
        extends Action<StartDatafeedAction.Request, PersistentActionResponse, StartDatafeedAction.RequestBuilder> {

    public static final ParseField START_TIME = new ParseField("start");
    public static final ParseField END_TIME = new ParseField("end");
    public static final ParseField TIMEOUT = new ParseField("timeout");

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
            PARSER.declareString((request, val) ->
                    request.setTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
        }

        public static Request fromXContent(XContentParser parser) {
            return parseRequest(null, parser);
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
        private TimeValue timeout = TimeValue.timeValueSeconds(20);

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

        public TimeValue getTimeout() {
            return timeout;
        }

        public void setTimeout(TimeValue timeout) {
            this.timeout = timeout;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId) {
            return new DatafeedTask(id, type, action, parentTaskId, this);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            datafeedId = in.readString();
            startTime = in.readVLong();
            endTime = in.readOptionalLong();
            timeout = TimeValue.timeValueMillis(in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(datafeedId);
            out.writeVLong(startTime);
            out.writeOptionalLong(endTime);
            out.writeVLong(timeout.millis());
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
            builder.field(TIMEOUT.getPreferredName(), timeout.getStringRep());
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeedId, startTime, endTime, timeout);
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
                    Objects.equals(endTime, other.endTime) &&
                    Objects.equals(timeout, other.timeout);
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, PersistentActionResponse, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client, StartDatafeedAction action) {
            super(client, action, new Request());
        }
    }

    public static class DatafeedTask extends NodePersistentTask {

        private final String datafeedId;
        private final long startTime;
        private final Long endTime;
        private volatile DatafeedJobRunner.Holder holder;

        public DatafeedTask(long id, String type, String action, TaskId parentTaskId, Request request) {
            super(id, type, action, "datafeed-" + request.getDatafeedId(), parentTaskId);
            this.datafeedId = request.getDatafeedId();
            this.startTime = request.startTime;
            this.endTime = request.endTime;
        }

        public String getDatafeedId() {
            return datafeedId;
        }

        public long getStartTime() {
            return startTime;
        }

        @Nullable
        public Long getEndTime() {
            return endTime;
        }

        public boolean isLookbackOnly() {
            return endTime != null;
        }

        public void setHolder(DatafeedJobRunner.Holder holder) {
            this.holder = holder;
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

        private final DatafeedStateObserver observer;
        private final DatafeedJobRunner datafeedJobRunner;
        private final XPackLicenseState licenseState;
        private final Auditor auditor;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool, XPackLicenseState licenseState,
                               PersistentActionService persistentActionService, PersistentActionRegistry persistentActionRegistry,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               ClusterService clusterService, DatafeedJobRunner datafeedJobRunner, Auditor auditor) {
            super(settings, NAME, false, threadPool, transportService, persistentActionService, persistentActionRegistry,
                    actionFilters, indexNameExpressionResolver, Request::new, ThreadPool.Names.MANAGEMENT);
            this.licenseState = licenseState;
            this.datafeedJobRunner = datafeedJobRunner;
            this.auditor = auditor;
            this.observer = new DatafeedStateObserver(threadPool, clusterService);
        }

        @Override
        protected void doExecute(Request request, ActionListener<PersistentActionResponse> listener) {
            if (licenseState.isMachineLearningAllowed()) {
                ActionListener<PersistentActionResponse> finalListener = ActionListener
                        .wrap(response -> waitForDatafeedStarted(request, response, listener), listener::onFailure);
                super.doExecute(request, finalListener);
            } else {
                listener.onFailure(LicenseUtils.newComplianceException(XPackPlugin.MACHINE_LEARNING));
            }
        }

        void waitForDatafeedStarted(Request request,
                                    PersistentActionResponse response,
                                    ActionListener<PersistentActionResponse> listener) {
            observer.waitForState(request.getDatafeedId(), request.timeout, DatafeedState.STARTED, e -> {
                if (e != null) {
                    listener.onFailure(e);
                } else {
                    listener.onResponse(response);
                }
            });
        }

        @Override
        public Assignment getAssignment(Request request, ClusterState clusterState) {
            Assignment assignment = selectNode(logger, request.getDatafeedId(), clusterState);
            writeAssignmentNotification(request.getDatafeedId(), assignment, clusterState);
            return assignment;
        }

        @Override
        public void validate(Request request, ClusterState clusterState) {
            MlMetadata mlMetadata = clusterState.metaData().custom(MlMetadata.TYPE);
            PersistentTasks tasks = clusterState.getMetaData().custom(PersistentTasks.TYPE);
            DiscoveryNodes nodes = clusterState.getNodes();
            StartDatafeedAction.validate(request.getDatafeedId(), mlMetadata, tasks, nodes);
        }

        @Override
        protected void nodeOperation(NodePersistentTask nodePersistentTask, Request request,
                                     ActionListener<TransportResponse.Empty> listener) {
            DatafeedTask datafeedTask = (DatafeedTask) nodePersistentTask;
            datafeedJobRunner.run(datafeedTask,
                    (error) -> {
                        if (error != null) {
                            listener.onFailure(error);
                        } else {
                            listener.onResponse(TransportResponse.Empty.INSTANCE);
                        }
                    });
        }

        private void writeAssignmentNotification(String datafeedId, Assignment assignment, ClusterState state) {
            // Forking as this code is called from cluster state update thread:
            // Should be ok as auditor uses index api which has its own tp
            threadPool.executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.warn("Failed to write assignment notification for datafeed [" + datafeedId + "]", e);
                }

                @Override
                protected void doRun() throws Exception {
                    MlMetadata mlMetadata = state.metaData().custom(MlMetadata.TYPE);
                    DatafeedConfig datafeed = mlMetadata.getDatafeed(datafeedId);
                    String jobId = datafeed.getJobId();
                    if (assignment.getExecutorNode() == null) {
                        auditor.warning(jobId, "No node found to start datafeed [" + datafeedId +"]. Reasons [" +
                                assignment.getExplanation() + "]");
                    } else {
                        DiscoveryNode node = state.nodes().get(assignment.getExecutorNode());
                        auditor.info(jobId, "Found node [" + node + "] to start datafeed [" + datafeedId + "]");
                    }
                }
            });
        }

    }

    static void validate(String datafeedId, MlMetadata mlMetadata, PersistentTasks tasks, DiscoveryNodes nodes) {
        DatafeedConfig datafeed = mlMetadata.getDatafeed(datafeedId);
        if (datafeed == null) {
            throw ExceptionsHelper.missingDatafeedException(datafeedId);
        }
        Job job = mlMetadata.getJobs().get(datafeed.getJobId());
        if (job == null) {
            throw ExceptionsHelper.missingJobException(datafeed.getJobId());
        }
        DatafeedJobValidator.validate(datafeed, job);
        JobState jobState = MlMetadata.getJobState(datafeed.getJobId(), tasks);
        if (jobState != JobState.OPENED) {
            throw new ElasticsearchStatusException("cannot start datafeed, expected job state [{}], but got [{}]",
                    RestStatus.CONFLICT, JobState.OPENED, jobState);
        }

        PersistentTask<?> datafeedTask = MlMetadata.getDatafeedTask(datafeedId, tasks);
        DatafeedState datafeedState = MlMetadata.getDatafeedState(datafeedId, tasks);
        if (datafeedTask != null && datafeedState == DatafeedState.STARTED) {
            if (datafeedTask.isAssigned() == false) {
                // We can skip the datafeed state check below, because the task got unassigned after we went into
                // started state on a node that disappeared and we didn't have the opportunity to set the status to stopped
                return;
            } else if (nodes.nodeExists(datafeedTask.getExecutorNode()) == false) {
                // The state is started and the node were running on no longer exists.
                // We can skip the datafeed state check below, because when the node
                // disappeared we didn't have time to set the state to stopped.
                return;
            }
        }
        if (datafeedState == DatafeedState.STARTED) {
            throw new ElasticsearchStatusException("datafeed [{}] already started, expected datafeed state [{}], but got [{}]",
                    RestStatus.CONFLICT, datafeedId, DatafeedState.STOPPED, DatafeedState.STARTED);
        }
    }

    public static Assignment selectNode(Logger logger, String datafeedId, ClusterState clusterState) {
        MlMetadata mlMetadata = clusterState.metaData().custom(MlMetadata.TYPE);
        PersistentTasks tasks = clusterState.getMetaData().custom(PersistentTasks.TYPE);
        DatafeedConfig datafeed = mlMetadata.getDatafeed(datafeedId);
        DiscoveryNodes nodes = clusterState.getNodes();

        PersistentTask<?> jobTask = MlMetadata.getJobTask(datafeed.getJobId(), tasks);
        if (jobTask == null) {
            String reason = "cannot start datafeed [" + datafeed.getId() + "], job task doesn't yet exist";
            logger.debug(reason);
            return new Assignment(null, reason);
        }
        if (jobTask.needsReassignment(nodes)) {
            String reason = "cannot start datafeed [" + datafeed.getId() + "], job [" + datafeed.getJobId() +
                    "] is unassigned or unassigned to a non existing node";
            logger.debug(reason);
            return new Assignment(null, reason);
        }
        if (jobTask.getStatus() != JobState.OPENED) {
            // lets try again later when the job has been opened:
            String reason = "cannot start datafeed [" + datafeed.getId() + "], because job's [" + datafeed.getJobId() +
                    "] state is [" + jobTask.getStatus() +  "] while state [" + JobState.OPENED + "] is required";
            logger.debug(reason);
            return new Assignment(null, reason);
        }
        return new Assignment(jobTask.getExecutorNode(), "");
    }

}
