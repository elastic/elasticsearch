/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedJobValidator;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;
import org.elasticsearch.xpack.ml.datafeed.DatafeedNodeSelector;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.persistent.AllocatedPersistentTask;
import org.elasticsearch.xpack.persistent.PersistentTaskParams;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.Assignment;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.xpack.persistent.PersistentTasksExecutor;
import org.elasticsearch.xpack.persistent.PersistentTasksService;
import org.elasticsearch.xpack.persistent.PersistentTasksService.WaitForPersistentTaskStatusListener;
import org.elasticsearch.xpack.security.InternalClient;

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

public class StartDatafeedAction
        extends Action<StartDatafeedAction.Request, StartDatafeedAction.Response, StartDatafeedAction.RequestBuilder> {

    public static final ParseField START_TIME = new ParseField("start");
    public static final ParseField END_TIME = new ParseField("end");
    public static final ParseField TIMEOUT = new ParseField("timeout");

    public static final StartDatafeedAction INSTANCE = new StartDatafeedAction();
    public static final String NAME = "cluster:admin/xpack/ml/datafeed/start";
    public static final String TASK_NAME = "xpack/ml/datafeed";

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

    public static class Request extends MasterNodeRequest<Request> implements ToXContentObject {

        public static Request fromXContent(XContentParser parser) {
            return parseRequest(null, parser);
        }

        public static Request parseRequest(String datafeedId, XContentParser parser) {
            DatafeedParams params = DatafeedParams.PARSER.apply(parser, null);
            if (datafeedId != null) {
                params.datafeedId = datafeedId;
            }
            return new Request(params);
        }

        private DatafeedParams params;

        public Request(String datafeedId, long startTime) {
            this.params = new DatafeedParams(datafeedId, startTime);
        }

        public Request(String datafeedId, String startTime) {
            this.params = new DatafeedParams(datafeedId, startTime);
        }

        public Request(DatafeedParams params) {
            this.params = params;
        }

        public Request(StreamInput in) throws IOException {
            readFrom(in);
        }

        Request() {
        }

        public DatafeedParams getParams() {
            return params;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException e = null;
            if (params.endTime != null && params.endTime <= params.startTime) {
                e = ValidateActions.addValidationError(START_TIME.getPreferredName() + " ["
                        + params.startTime + "] must be earlier than " + END_TIME.getPreferredName()
                        + " [" + params.endTime + "]", e);
            }
            return e;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            params = new DatafeedParams(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            params.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            this.params.toXContent(builder, params);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(params);
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
            return Objects.equals(params, other.params);
        }
    }

    public static class DatafeedParams implements PersistentTaskParams {

        public static ObjectParser<DatafeedParams, Void> PARSER = new ObjectParser<>(TASK_NAME, DatafeedParams::new);

        static {
            PARSER.declareString((params, datafeedId) -> params.datafeedId = datafeedId, DatafeedConfig.ID);
            PARSER.declareString((params, startTime) -> params.startTime = parseDateOrThrow(
                    startTime, START_TIME, System::currentTimeMillis), START_TIME);
            PARSER.declareString(DatafeedParams::setEndTime, END_TIME);
            PARSER.declareString((params, val) ->
                    params.setTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
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

        public static DatafeedParams fromXContent(XContentParser parser) {
            return parseRequest(null, parser);
        }

        public static DatafeedParams parseRequest(String datafeedId, XContentParser parser) {
            DatafeedParams params = PARSER.apply(parser, null);
            if (datafeedId != null) {
                params.datafeedId = datafeedId;
            }
            return params;
        }

        public DatafeedParams(String datafeedId, long startTime) {
            this.datafeedId = ExceptionsHelper.requireNonNull(datafeedId, DatafeedConfig.ID.getPreferredName());
            this.startTime = startTime;
        }

        public DatafeedParams(String datafeedId, String startTime) {
            this(datafeedId, parseDateOrThrow(startTime, START_TIME, System::currentTimeMillis));
        }

        public DatafeedParams(StreamInput in) throws IOException {
            datafeedId = in.readString();
            startTime = in.readVLong();
            endTime = in.readOptionalLong();
            timeout = TimeValue.timeValueMillis(in.readVLong());
        }

        DatafeedParams() {
        }

        private String datafeedId;
        private long startTime;
        private Long endTime;
        private TimeValue timeout = TimeValue.timeValueSeconds(20);

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
        public String getWriteableName() {
            return TASK_NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(datafeedId);
            out.writeVLong(startTime);
            out.writeOptionalLong(endTime);
            out.writeVLong(timeout.millis());
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
            DatafeedParams other = (DatafeedParams) obj;
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
        volatile DatafeedManager datafeedManager;

        DatafeedTask(long id, String type, String action, TaskId parentTaskId, DatafeedParams params) {
            super(id, type, action, "datafeed-" + params.getDatafeedId(), parentTaskId);
            this.datafeedId = params.getDatafeedId();
            this.startTime = params.getStartTime();
            this.endTime = params.getEndTime();
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
            // If the persistent task framework wants us to stop then we should do so immediately and
            // we should wait for an existing datafeed import to realize we want it to stop.
            // Note that this only applied when task cancel is invoked and stop datafeed api doesn't use this.
            // Also stop datafeed api will obey the timeout.
            stop(getReasonCancelled(), TimeValue.ZERO);
        }

        public void stop(String reason, TimeValue timeout) {
            if (datafeedManager != null) {
                datafeedManager.stopDatafeed(this, reason, timeout);
            }
        }

        public void isolate() {
            if (datafeedManager != null) {
                datafeedManager.isolateDatafeed(getAllocationId());
            }
        }
    }

    // This class extends from TransportMasterNodeAction for cluster state observing purposes.
    // The stop datafeed api also redirect the elected master node.
    // The master node will wait for the datafeed to be started by checking the persistent task's status and then return.
    // To ensure that a subsequent stop datafeed call will see that same task status (and sanity validation doesn't fail)
    // both start and stop datafeed apis redirect to the elected master node.
    // In case of instability persistent tasks checks may fail and that is ok, in that case all bets are off.
    // The start datafeed api is a low through put api, so the fact that we redirect to elected master node shouldn't be an issue.
    public static class TransportAction extends TransportMasterNodeAction<Request, Response> {

        private final InternalClient client;
        private final XPackLicenseState licenseState;
        private final PersistentTasksService persistentTasksService;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool, ClusterService clusterService,
                               XPackLicenseState licenseState, PersistentTasksService persistentTasksService,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               InternalClient client) {
            super(settings, NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, Request::new);
            this.licenseState = licenseState;
            this.persistentTasksService = persistentTasksService;
            this.client = client;
        }

        @Override
        protected String executor() {
            // This api doesn't do heavy or blocking operations (just delegates PersistentTasksService),
            // so we can do this on the network thread
            return ThreadPool.Names.SAME;
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

        @Override
        protected void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
            DatafeedParams params = request.params;
            if (licenseState.isMachineLearningAllowed()) {
                ActionListener<PersistentTask<DatafeedParams>> finalListener = new ActionListener<PersistentTask<DatafeedParams>>() {
                    @Override
                    public void onResponse(PersistentTask<DatafeedParams> persistentTask) {
                        waitForDatafeedStarted(persistentTask.getId(), params, listener);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (e instanceof ResourceAlreadyExistsException) {
                            logger.debug(e);
                            e = new ElasticsearchStatusException("cannot start datafeed [" + params.getDatafeedId() +
                                    "] because it has already been started", RestStatus.CONFLICT);
                        }
                        listener.onFailure(e);
                    }
                };

                // Verify data extractor factory can be created, then start persistent task
                MlMetadata mlMetadata = state.metaData().custom(MlMetadata.TYPE);
                PersistentTasksCustomMetaData tasks = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
                StartDatafeedAction.validate(params.getDatafeedId(), mlMetadata, tasks);
                DatafeedConfig datafeed = mlMetadata.getDatafeed(params.getDatafeedId());
                Job job = mlMetadata.getJobs().get(datafeed.getJobId());
                DataExtractorFactory.create(client, datafeed, job, ActionListener.wrap(
                        dataExtractorFactory -> persistentTasksService.startPersistentTask(MlMetadata.datafeedTaskId(params.datafeedId),
                                TASK_NAME, params, finalListener)
                        , listener::onFailure));
            } else {
                listener.onFailure(LicenseUtils.newComplianceException(XPackPlugin.MACHINE_LEARNING));
            }
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            // We only delegate here to PersistentTasksService, but if there is a metadata writeblock,
            // then delagating to PersistentTasksService doesn't make a whole lot of sense,
            // because PersistentTasksService will then fail.
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        void waitForDatafeedStarted(String taskId, DatafeedParams params, ActionListener<Response> listener) {
            Predicate<PersistentTask<?>> predicate = persistentTask -> {
                if (persistentTask == null) {
                    return false;
                }
                DatafeedState datafeedState = (DatafeedState) persistentTask.getStatus();
                return datafeedState == DatafeedState.STARTED;
            };
            persistentTasksService.waitForPersistentTaskStatus(taskId, predicate, params.timeout,
                    new WaitForPersistentTaskStatusListener<DatafeedParams>() {
                @Override
                public void onResponse(PersistentTask<DatafeedParams> task) {
                    listener.onResponse(new Response(true));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onFailure(new ElasticsearchException("Starting datafeed ["
                            + params.getDatafeedId() + "] timed out after [" + timeout + "]"));
                }
            });
        }
    }

    public static class StartDatafeedPersistentTasksExecutor extends PersistentTasksExecutor<DatafeedParams> {
        private final DatafeedManager datafeedManager;
        private final IndexNameExpressionResolver resolver;

        public StartDatafeedPersistentTasksExecutor(Settings settings, DatafeedManager datafeedManager) {
            super(settings, TASK_NAME, MachineLearning.UTILITY_THREAD_POOL_NAME);
            this.datafeedManager = datafeedManager;
            this.resolver = new IndexNameExpressionResolver(settings);
        }

        @Override
        public Assignment getAssignment(DatafeedParams params, ClusterState clusterState) {
            return new DatafeedNodeSelector(clusterState, resolver, params.getDatafeedId()).selectNode();
        }

        @Override
        public void validate(DatafeedParams params, ClusterState clusterState) {
            MlMetadata mlMetadata = clusterState.metaData().custom(MlMetadata.TYPE);
            PersistentTasksCustomMetaData tasks = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
            StartDatafeedAction.validate(params.getDatafeedId(), mlMetadata, tasks);
            new DatafeedNodeSelector(clusterState, resolver, params.getDatafeedId()).checkDatafeedTaskCanBeCreated();
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask allocatedPersistentTask, DatafeedParams params) {
            DatafeedTask datafeedTask = (DatafeedTask) allocatedPersistentTask;
            datafeedTask.datafeedManager = datafeedManager;
            datafeedManager.run(datafeedTask,
                    (error) -> {
                        if (error != null) {
                            datafeedTask.markAsFailed(error);
                        } else {
                            datafeedTask.markAsCompleted();
                        }
                    });
        }

        @Override
        protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
                                                     PersistentTask<DatafeedParams> persistentTask) {
            return new DatafeedTask(id, type, action, parentTaskId, persistentTask.getParams());
        }
    }

    static void validate(String datafeedId, MlMetadata mlMetadata, PersistentTasksCustomMetaData tasks) {
        DatafeedConfig datafeed = (mlMetadata == null) ? null : mlMetadata.getDatafeed(datafeedId);
        if (datafeed == null) {
            throw ExceptionsHelper.missingDatafeedException(datafeedId);
        }
        Job job = mlMetadata.getJobs().get(datafeed.getJobId());
        if (job == null) {
            throw ExceptionsHelper.missingJobException(datafeed.getJobId());
        }
        DatafeedJobValidator.validate(datafeed, job);
        JobState jobState = MlMetadata.getJobState(datafeed.getJobId(), tasks);
        if (jobState.isAnyOf(JobState.OPENING, JobState.OPENED) == false) {
            throw ExceptionsHelper.conflictStatusException("cannot start datafeed [" + datafeedId + "] because job [" + job.getId() +
                    "] is " + jobState);
        }
    }
}
