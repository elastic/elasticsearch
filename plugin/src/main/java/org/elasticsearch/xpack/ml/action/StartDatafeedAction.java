/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
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
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
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
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.elasticsearch.xpack.ml.utils.DatafeedStateObserver;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.persistent.AllocatedPersistentTask;
import org.elasticsearch.xpack.persistent.PersistentTaskRequest;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.Assignment;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.xpack.persistent.PersistentTasksExecutor;
import org.elasticsearch.xpack.persistent.PersistentTasksService;
import org.elasticsearch.xpack.persistent.PersistentTasksService.PersistentTaskOperationListener;

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongSupplier;

public class StartDatafeedAction
        extends Action<StartDatafeedAction.Request, StartDatafeedAction.Response, StartDatafeedAction.RequestBuilder> {

    public static final ParseField START_TIME = new ParseField("start");
    public static final ParseField END_TIME = new ParseField("end");
    public static final ParseField TIMEOUT = new ParseField("timeout");

    public static final StartDatafeedAction INSTANCE = new StartDatafeedAction();
    public static final String NAME = "cluster:admin/xpack/ml/datafeeds/start";

    private StartDatafeedAction() {
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

    public static class Request extends PersistentTaskRequest implements ToXContent {

        public static ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, datafeedId) -> request.datafeedId = datafeedId, DatafeedConfig.ID);
            PARSER.declareString((request, startTime) -> request.startTime = parseDateOrThrow(
                    startTime, START_TIME, System::currentTimeMillis), START_TIME);
            PARSER.declareString(Request::setEndTime, END_TIME);
            PARSER.declareString((request, val) ->
                    request.setTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
        }

        static long parseDateOrThrow(String date, ParseField paramName, LongSupplier now) {
            DateMathParser dateMathParser = new DateMathParser(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER);

            try {
                return dateMathParser.parse(date, now);
            } catch (Exception e) {
                String msg = Messages.getMessage(Messages.REST_INVALID_DATETIME_PARAMS, paramName.getPreferredName(), date);
                throw new ElasticsearchParseException(msg, e);
            }
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

        public Request(String datafeedId, String startTime) {
            this(datafeedId, parseDateOrThrow(startTime, START_TIME, System::currentTimeMillis));
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

        public void setEndTime(String endTime) {
            setEndTime(parseDateOrThrow(endTime, END_TIME, System::currentTimeMillis));
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
            ActionRequestValidationException e = null;
            if (endTime != null && endTime <= startTime) {
                e = ValidateActions.addValidationError(START_TIME.getPreferredName() + " ["
                        + startTime + "] must be earlier than " + END_TIME.getPreferredName()
                        + " [" + endTime + "]", e);
            }
            return e;
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
            builder.field(START_TIME.getPreferredName(), String.valueOf(startTime));
            if (endTime != null) {
                builder.field(END_TIME.getPreferredName(), String.valueOf(endTime));
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

    public static class Response extends AcknowledgedResponse {
        public Response() {
            super();
        }

        public Response(boolean acknowledged) {
            super(acknowledged);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            readAcknowledged(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            writeAcknowledged(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AcknowledgedResponse that = (AcknowledgedResponse) o;
            return isAcknowledged() == that.isAcknowledged();
        }

        @Override
        public int hashCode() {
            return Objects.hash(isAcknowledged());
        }

    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client, StartDatafeedAction action) {
            super(client, action, new Request());
        }
    }

    public static class DatafeedTask extends AllocatedPersistentTask {

        private final String datafeedId;
        private final long startTime;
        private final Long endTime;
        /* only pck protected for testing */
        volatile DatafeedJobRunner datafeedJobRunner;

        DatafeedTask(long id, String type, String action, TaskId parentTaskId, Request request) {
            super(id, type, action, "datafeed-" + request.getDatafeedId(), parentTaskId);
            this.datafeedId = request.getDatafeedId();
            this.startTime = request.getStartTime();
            this.endTime = request.getEndTime();
        }

        public String getDatafeedId() {
            return datafeedId;
        }

        public long getDatafeedStartTime() {
            return startTime;
        }

        @Nullable
        public Long getEndTime() {
            return endTime;
        }

        public boolean isLookbackOnly() {
            return endTime != null;
        }

        @Override
        protected void onCancelled() {
            stop(getReasonCancelled());
        }

        public void stop(String reason) {
            stop(reason, TimeValue.timeValueSeconds(20));
        }

        public void stop(String reason, TimeValue timeout) {
            datafeedJobRunner.stopDatafeed(datafeedId, reason, timeout);
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final DatafeedStateObserver observer;
        private final XPackLicenseState licenseState;
        private final PersistentTasksService persistentTasksService;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool, XPackLicenseState licenseState,
                               PersistentTasksService persistentTasksService, ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver, ClusterService clusterService) {
            super(settings, NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, Request::new);
            this.licenseState = licenseState;
            this.persistentTasksService = persistentTasksService;
            this.observer = new DatafeedStateObserver(threadPool, clusterService);
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            if (licenseState.isMachineLearningAllowed()) {
                PersistentTaskOperationListener finalListener = new PersistentTaskOperationListener() {
                    @Override
                    public void onResponse(long taskId) {
                        waitForDatafeedStarted(request, listener);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                };
                persistentTasksService.createPersistentActionTask(NAME, request, finalListener);
            } else {
                listener.onFailure(LicenseUtils.newComplianceException(XPackPlugin.MACHINE_LEARNING));
            }
        }

        void waitForDatafeedStarted(Request request, ActionListener<Response> listener) {
            observer.waitForState(request.getDatafeedId(), request.timeout, DatafeedState.STARTED, e -> {
                if (e != null) {
                    listener.onFailure(e);
                } else {
                    listener.onResponse(new Response(true));
                }
            });
        }
    }

    public static class StartDatafeedPersistentTasksExecutor extends PersistentTasksExecutor<Request> {
        private final DatafeedJobRunner datafeedJobRunner;
        private final XPackLicenseState licenseState;
        private final Auditor auditor;
        private final ThreadPool threadPool;

        public StartDatafeedPersistentTasksExecutor(Settings settings, ThreadPool threadPool, XPackLicenseState licenseState,
                                                    PersistentTasksService persistentTasksService, DatafeedJobRunner datafeedJobRunner,
                                                    Auditor auditor) {
            super(settings, NAME, persistentTasksService, ThreadPool.Names.MANAGEMENT);
            this.licenseState = licenseState;
            this.datafeedJobRunner = datafeedJobRunner;
            this.auditor = auditor;
            this.threadPool = threadPool;
        }

        @Override
        public Assignment getAssignment(Request request, ClusterState clusterState) {
            Assignment assignment = selectNode(logger, request.getDatafeedId(), clusterState);
            writeAssignmentNotification(request.getDatafeedId(), assignment, clusterState);
            return assignment;
        }

        @Override
        public void validate(Request request, ClusterState clusterState) {
            if (licenseState.isMachineLearningAllowed()) {
                MlMetadata mlMetadata = clusterState.metaData().custom(MlMetadata.TYPE);
                PersistentTasksCustomMetaData tasks = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
                StartDatafeedAction.validate(request.getDatafeedId(), mlMetadata, tasks);
            } else {
                throw LicenseUtils.newComplianceException(XPackPlugin.MACHINE_LEARNING);
            }
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask allocatedPersistentTask, Request request,
                                     ActionListener<TransportResponse.Empty> listener) {
            DatafeedTask datafeedTask = (DatafeedTask) allocatedPersistentTask;
            datafeedTask.datafeedJobRunner = datafeedJobRunner;
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

    static void validate(String datafeedId, MlMetadata mlMetadata, PersistentTasksCustomMetaData tasks) {
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
            throw ExceptionsHelper.conflictStatusException("cannot start datafeed [" + datafeedId + "] because job [" + job.getId() +
                    "] is not open");
        }

        PersistentTask<?> datafeedTask = MlMetadata.getDatafeedTask(datafeedId, tasks);
        if (datafeedTask != null) {
            throw ExceptionsHelper.conflictStatusException("cannot start datafeed [" + datafeedId + "] because it has already been started");
        }
    }

    static Assignment selectNode(Logger logger, String datafeedId, ClusterState clusterState) {
        MlMetadata mlMetadata = clusterState.metaData().custom(MlMetadata.TYPE);
        PersistentTasksCustomMetaData tasks = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
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
