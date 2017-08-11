/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MlMetaIndex;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.config.JobTaskStatus;
import org.elasticsearch.xpack.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager.MAX_OPEN_JOBS_PER_NODE;

public class OpenJobAction extends Action<OpenJobAction.Request, OpenJobAction.Response, OpenJobAction.RequestBuilder> {

    public static final OpenJobAction INSTANCE = new OpenJobAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/open";
    public static final String TASK_NAME = "xpack/ml/job";

    private OpenJobAction() {
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

        public static Request parseRequest(String jobId, XContentParser parser) {
            JobParams jobParams = JobParams.PARSER.apply(parser, null);
            if (jobId != null) {
                jobParams.jobId = jobId;
            }
            return new Request(jobParams);
        }

        private JobParams jobParams;

        public Request(JobParams jobParams) {
            this.jobParams = jobParams;
        }

        public Request(String jobId) {
            this.jobParams = new JobParams(jobId);
        }

        public Request(StreamInput in) throws IOException {
            readFrom(in);
        }

        Request() {
        }

        public JobParams getJobParams() {
            return jobParams;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobParams = new JobParams(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            jobParams.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            jobParams.toXContent(builder, params);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobParams);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            OpenJobAction.Request other = (OpenJobAction.Request) obj;
            return Objects.equals(jobParams, other.jobParams);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public static class JobParams implements PersistentTaskParams {

        /** TODO Remove in 7.0.0 */
        public static final ParseField IGNORE_DOWNTIME = new ParseField("ignore_downtime");

        public static final ParseField TIMEOUT = new ParseField("timeout");
        public static ObjectParser<JobParams, Void> PARSER = new ObjectParser<>(TASK_NAME, JobParams::new);

        static {
            PARSER.declareString(JobParams::setJobId, Job.ID);
            PARSER.declareBoolean((p, v) -> {}, IGNORE_DOWNTIME);
            PARSER.declareString((params, val) ->
                    params.setTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
        }

        public static JobParams fromXContent(XContentParser parser) {
            return parseRequest(null, parser);
        }

        public static JobParams parseRequest(String jobId, XContentParser parser) {
            JobParams params = PARSER.apply(parser, null);
            if (jobId != null) {
                params.jobId = jobId;
            }
            return params;
        }

        private String jobId;
        // A big state can take a while to restore.  For symmetry with the _close endpoint any
        // changes here should be reflected there too.
        private TimeValue timeout = MachineLearning.STATE_PERSIST_RESTORE_TIMEOUT;

        JobParams() {
        }

        public JobParams(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        public JobParams(StreamInput in) throws IOException {
            jobId = in.readString();
            if (in.getVersion().onOrBefore(Version.V_5_5_0)) {
                // Read `ignoreDowntime`
                in.readBoolean();
            }
            timeout = TimeValue.timeValueMillis(in.readVLong());
        }

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
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
            out.writeString(jobId);
            if (out.getVersion().onOrBefore(Version.V_5_5_0)) {
                // Write `ignoreDowntime` - true by default
                out.writeBoolean(true);
            }
            out.writeVLong(timeout.millis());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            builder.field(TIMEOUT.getPreferredName(), timeout.getStringRep());
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, timeout);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            OpenJobAction.JobParams other = (OpenJobAction.JobParams) obj;
            return Objects.equals(jobId, other.jobId) &&
                    Objects.equals(timeout, other.timeout);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
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

    public static class JobTask extends AllocatedPersistentTask {

        private final String jobId;
        private volatile AutodetectProcessManager autodetectProcessManager;

        JobTask(String jobId, long id, String type, String action, TaskId parentTask) {
            super(id, type, action, "job-" + jobId, parentTask);
            this.jobId = jobId;
        }

        public String getJobId() {
            return jobId;
        }

        @Override
        protected void onCancelled() {
            String reason = getReasonCancelled();
            killJob(reason);
        }

        void killJob(String reason) {
            autodetectProcessManager.killProcess(this, false, reason);
        }

        void closeJob(String reason) {
            autodetectProcessManager.closeJob(this, false, reason);
        }

        static boolean match(Task task, String expectedJobId) {
            String expectedDescription = "job-" + expectedJobId;
            return task instanceof JobTask && expectedDescription.equals(task.getDescription());
        }

    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client, OpenJobAction action) {
            super(client, action, new Request());
        }
    }

    // This class extends from TransportMasterNodeAction for cluster state observing purposes.
    // The close job api also redirect the elected master node.
    // The master node will wait for the job to be opened by checking the persistent task's status and then return.
    // To ensure that a subsequent close job call will see that same task status (and sanity validation doesn't fail)
    // both open and close job apis redirect to the elected master node.
    // In case of instability persistent tasks checks may fail and that is ok, in that case all bets are off.
    // The open job api is a low through put api, so the fact that we redirect to elected master node shouldn't be an issue.
    public static class TransportAction extends TransportMasterNodeAction<Request, Response> {

        private final XPackLicenseState licenseState;
        private final PersistentTasksService persistentTasksService;
        private final InternalClient client;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool, XPackLicenseState licenseState,
                               ClusterService clusterService, PersistentTasksService persistentTasksService, ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver, InternalClient client) {
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
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            // We only delegate here to PersistentTasksService, but if there is a metadata writeblock,
            // then delegating to PersistentTasksService doesn't make a whole lot of sense,
            // because PersistentTasksService will then fail.
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        @Override
        protected void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
            JobParams jobParams = request.getJobParams();
            if (licenseState.isMachineLearningAllowed()) {
                // Step 4. Wait for job to be started and respond
                ActionListener<PersistentTask<JobParams>> finalListener = new ActionListener<PersistentTask<JobParams>>() {
                    @Override
                    public void onResponse(PersistentTask<JobParams> task) {
                        waitForJobStarted(task.getId(), jobParams, listener);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (e instanceof ResourceAlreadyExistsException) {
                            e = new ElasticsearchStatusException("Cannot open job [" + jobParams.getJobId() +
                                    "] because it has already been opened", RestStatus.CONFLICT, e);
                        }
                        listener.onFailure(e);
                    }
                };

                // Step 3. Start job task
                ActionListener<Boolean> missingMappingsListener = ActionListener.wrap(
                        response -> persistentTasksService.startPersistentTask(MlMetadata.jobTaskId(jobParams.jobId),
                                TASK_NAME, jobParams, finalListener)
                        , listener::onFailure
                );

                // Step 2. Try adding state doc mapping
                ActionListener<Boolean> resultsPutMappingHandler = ActionListener.wrap(
                        response -> {
                            addDocMappingIfMissing(AnomalyDetectorsIndex.jobStateIndexName(), ElasticsearchMappings::stateMapping,
                                    state, missingMappingsListener);
                        }, listener::onFailure
                );

                // Step 1. Try adding results doc mapping
                addDocMappingIfMissing(AnomalyDetectorsIndex.jobResultsAliasedName(jobParams.jobId), ElasticsearchMappings::docMapping,
                        state, resultsPutMappingHandler);
            } else {
                listener.onFailure(LicenseUtils.newComplianceException(XPackPlugin.MACHINE_LEARNING));
            }
        }

        void waitForJobStarted(String taskId, JobParams jobParams, ActionListener<Response> listener) {
            JobPredicate predicate = new JobPredicate();
            persistentTasksService.waitForPersistentTaskStatus(taskId, predicate, jobParams.timeout,
                    new WaitForPersistentTaskStatusListener<JobParams>() {
                @Override
                public void onResponse(PersistentTask<JobParams> persistentTask) {
                    if (predicate.exception != null) {
                        listener.onFailure(predicate.exception);
                    } else {
                        listener.onResponse(new Response(predicate.opened));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onFailure(new ElasticsearchException("Opening job ["
                            + jobParams.getJobId() + "] timed out after [" + timeout + "]"));
                }
            });
        }

        private void addDocMappingIfMissing(String alias, CheckedSupplier<XContentBuilder, IOException> mappingSupplier, ClusterState state,
                                            ActionListener<Boolean> listener) {
            AliasOrIndex aliasOrIndex = state.metaData().getAliasAndIndexLookup().get(alias);
            if (aliasOrIndex == null) {
                // The index has never been created yet
                listener.onResponse(true);
                return;
            }
            String[] concreteIndices = aliasOrIndex.getIndices().stream().map(IndexMetaData::getIndex).map(Index::getName)
                    .toArray(String[]::new);

            String[] indicesThatRequireAnUpdate = mappingRequiresUpdate(state, concreteIndices, Version.CURRENT, logger);

            if (indicesThatRequireAnUpdate.length > 0) {
                try (XContentBuilder mapping = mappingSupplier.get()) {
                    PutMappingRequest putMappingRequest = new PutMappingRequest(indicesThatRequireAnUpdate);
                    putMappingRequest.type(ElasticsearchMappings.DOC_TYPE);
                    putMappingRequest.source(mapping);
                    client.execute(PutMappingAction.INSTANCE, putMappingRequest, ActionListener.wrap(
                            response -> {
                                if (response.isAcknowledged()) {
                                    listener.onResponse(true);
                                } else {
                                    listener.onFailure(new ElasticsearchException("Attempt to put missing mapping in indices "
                                            + Arrays.toString(indicesThatRequireAnUpdate) + " was not acknowledged"));
                                }
                            }, listener::onFailure));
                } catch (IOException e) {
                    listener.onFailure(e);
                }
            } else {
                logger.trace("Mappings are uptodate.");
                listener.onResponse(true);
            }
        }

        /**
         * Important: the methods of this class must NOT throw exceptions.  If they did then the callers
         * of endpoints waiting for a condition tested by this predicate would never get a response.
         */
        private class JobPredicate implements Predicate<PersistentTask<?>> {

            private volatile boolean opened;
            private volatile Exception exception;

            @Override
            public boolean test(PersistentTask<?> persistentTask) {
                JobState jobState = JobState.CLOSED;
                if (persistentTask != null) {
                    JobTaskStatus jobStateStatus = (JobTaskStatus) persistentTask.getStatus();
                    jobState = jobStateStatus == null ? JobState.OPENING : jobStateStatus.getState();
                }
                switch (jobState) {
                    case OPENING:
                    case CLOSED:
                        return false;
                    case OPENED:
                        opened = true;
                        return true;
                    case CLOSING:
                        exception = ExceptionsHelper.conflictStatusException("The job has been " + JobState.CLOSED + " while waiting to be "
                                + JobState.OPENED);
                        return true;
                    case FAILED:
                    default:
                        exception = ExceptionsHelper.serverError("Unexpected job state [" + jobState
                                + "] while waiting for job to be " + JobState.OPENED);
                        return true;
                }
            }
        }
    }

    public static class OpenJobPersistentTasksExecutor extends PersistentTasksExecutor<JobParams> {

        private final AutodetectProcessManager autodetectProcessManager;

        /**
         * The maximum number of open jobs can be different on each node.  However, nodes on older versions
         * won't add their setting to the cluster state, so for backwards compatibility with these nodes we
         * assume the older node's setting is the same as that of the node running this code.
         * TODO: remove this member in 7.0
         */
        private final int fallbackMaxNumberOfOpenJobs;
        private volatile int maxConcurrentJobAllocations;

        public OpenJobPersistentTasksExecutor(Settings settings, ClusterService clusterService,
                                              AutodetectProcessManager autodetectProcessManager) {
            super(settings, TASK_NAME, MachineLearning.UTILITY_THREAD_POOL_NAME);
            this.autodetectProcessManager = autodetectProcessManager;
            this.fallbackMaxNumberOfOpenJobs = AutodetectProcessManager.MAX_OPEN_JOBS_PER_NODE.get(settings);
            this.maxConcurrentJobAllocations = MachineLearning.CONCURRENT_JOB_ALLOCATIONS.get(settings);
            clusterService.getClusterSettings()
                    .addSettingsUpdateConsumer(MachineLearning.CONCURRENT_JOB_ALLOCATIONS, this::setMaxConcurrentJobAllocations);
        }

        @Override
        public Assignment getAssignment(JobParams params, ClusterState clusterState) {
            return selectLeastLoadedMlNode(params.getJobId(), clusterState, maxConcurrentJobAllocations, fallbackMaxNumberOfOpenJobs,
                    logger);
        }

        @Override
        public void validate(JobParams params, ClusterState clusterState) {
            // If we already know that we can't find an ml node because all ml nodes are running at capacity or
            // simply because there are no ml nodes in the cluster then we fail quickly here:
            MlMetadata mlMetadata = clusterState.metaData().custom(MlMetadata.TYPE);
            OpenJobAction.validate(params.getJobId(), mlMetadata);
            Assignment assignment = selectLeastLoadedMlNode(params.getJobId(), clusterState, maxConcurrentJobAllocations,
                    fallbackMaxNumberOfOpenJobs, logger);
            if (assignment.getExecutorNode() == null) {
                String msg = "Could not open job because no suitable nodes were found, allocation explanation ["
                        + assignment.getExplanation() + "]";
                logger.warn("[{}] {}", params.getJobId(), msg);
                throw new ElasticsearchStatusException(msg, RestStatus.TOO_MANY_REQUESTS);
            }
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, JobParams params) {
            JobTask jobTask = (JobTask) task;
            jobTask.autodetectProcessManager = autodetectProcessManager;
            autodetectProcessManager.openJob(jobTask, e2 -> {
                if (e2 == null) {
                    task.markAsCompleted();
                } else {
                    task.markAsFailed(e2);
                }
            });
        }

        @Override
        protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
                                                     PersistentTask<JobParams> persistentTask) {
             return new JobTask(persistentTask.getParams().getJobId(), id, type, action, parentTaskId);
        }

        void setMaxConcurrentJobAllocations(int maxConcurrentJobAllocations) {
            logger.info("Changing [{}] from [{}] to [{}]", MachineLearning.CONCURRENT_JOB_ALLOCATIONS.getKey(),
                    this.maxConcurrentJobAllocations, maxConcurrentJobAllocations);
            this.maxConcurrentJobAllocations = maxConcurrentJobAllocations;
        }
    }

    /**
     * Validations to fail fast before trying to update the job state on master node:
     * <ul>
     *     <li>check job exists</li>
     *     <li>check job is not marked as deleted</li>
     *     <li>check job's version is supported</li>
     * </ul>
     */
    static void validate(String jobId, MlMetadata mlMetadata) {
        Job job = (mlMetadata == null) ? null : mlMetadata.getJobs().get(jobId);
        if (job == null) {
            throw ExceptionsHelper.missingJobException(jobId);
        }
        if (job.isDeleted()) {
            throw ExceptionsHelper.conflictStatusException("Cannot open job [" + jobId + "] because it has been marked as deleted");
        }
        if (job.getJobVersion() == null) {
            throw ExceptionsHelper.badRequestException("Cannot open job [" + jobId
                    + "] because jobs created prior to version 5.5 are not supported");
        }
    }

    static Assignment selectLeastLoadedMlNode(String jobId, ClusterState clusterState, int maxConcurrentJobAllocations,
                                              int fallbackMaxNumberOfOpenJobs, Logger logger) {
        List<String> unavailableIndices = verifyIndicesPrimaryShardsAreActive(jobId, clusterState);
        if (unavailableIndices.size() != 0) {
            String reason = "Not opening job [" + jobId + "], because not all primary shards are active for the following indices [" +
                    String.join(",", unavailableIndices) + "]";
            logger.debug(reason);
            return new Assignment(null, reason);
        }

        long maxAvailable = Long.MIN_VALUE;
        List<String> reasons = new LinkedList<>();
        DiscoveryNode minLoadedNode = null;
        PersistentTasksCustomMetaData persistentTasks = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        for (DiscoveryNode node : clusterState.getNodes()) {
            Map<String, String> nodeAttributes = node.getAttributes();
            String enabled = nodeAttributes.get(MachineLearning.ML_ENABLED_NODE_ATTR);
            if (Boolean.valueOf(enabled) == false) {
                String reason = "Not opening job [" + jobId + "] on node [" + node + "], because this node isn't a ml node.";
                logger.trace(reason);
                reasons.add(reason);
                continue;
            }

            MlMetadata mlMetadata = clusterState.getMetaData().custom(MlMetadata.TYPE);
            Job job = mlMetadata.getJobs().get(jobId);
            Set<String> compatibleJobTypes = Job.getCompatibleJobTypes(node.getVersion());
            if (compatibleJobTypes.contains(job.getJobType()) == false) {
                String reason = "Not opening job [" + jobId + "] on node [" + node + "], because this node does not support jobs of type ["
                        + job.getJobType() + "]";
                logger.trace(reason);
                reasons.add(reason);
                continue;
            }

            if (nodeSupportsJobVersion(node.getVersion(), job.getJobVersion()) == false) {
                String reason = "Not opening job [" + jobId + "] on node [" + node
                        + "], because this node does not support jobs of version [" + job.getJobVersion() + "]";
                logger.trace(reason);
                reasons.add(reason);
                continue;
            }

            long numberOfAssignedJobs;
            int numberOfAllocatingJobs;
            if (persistentTasks != null) {
                numberOfAssignedJobs = persistentTasks.getNumberOfTasksOnNode(node.getId(), OpenJobAction.TASK_NAME);
                numberOfAllocatingJobs = persistentTasks.findTasks(OpenJobAction.TASK_NAME, task -> {
                    if (node.getId().equals(task.getExecutorNode()) == false) {
                        return false;
                    }
                    JobTaskStatus jobTaskState = (JobTaskStatus) task.getStatus();
                    return jobTaskState == null || // executor node didn't have the chance to set job status to OPENING
                           jobTaskState.isStatusStale(task); // previous executor node failed and
                    // current executor node didn't have the chance to set job status to OPENING
                }).size();
            } else {
                numberOfAssignedJobs = 0;
                numberOfAllocatingJobs = 0;
            }
            if (numberOfAllocatingJobs >= maxConcurrentJobAllocations) {
                String reason = "Not opening job [" + jobId + "] on node [" + node + "], because node exceeds [" + numberOfAllocatingJobs +
                        "] the maximum number of jobs [" + maxConcurrentJobAllocations + "] in opening state";
                logger.trace(reason);
                reasons.add(reason);
                continue;
            }

            String maxNumberOfOpenJobsStr = nodeAttributes.get(MachineLearning.MAX_OPEN_JOBS_NODE_ATTR);
            int maxNumberOfOpenJobs = fallbackMaxNumberOfOpenJobs;
            // TODO: remove leniency and reject the node if the attribute is null in 7.0
            if (maxNumberOfOpenJobsStr != null) {
                try {
                    maxNumberOfOpenJobs = Integer.parseInt(maxNumberOfOpenJobsStr);
                } catch (NumberFormatException e) {
                    String reason = "Not opening job [" + jobId + "] on node [" + node + "], because " +
                            MachineLearning.MAX_OPEN_JOBS_NODE_ATTR + " attribute [" + maxNumberOfOpenJobsStr + "] is not an integer";
                    logger.trace(reason);
                    reasons.add(reason);
                    continue;
                }
            }
            long available = maxNumberOfOpenJobs - numberOfAssignedJobs;
            if (available == 0) {
                String reason = "Not opening job [" + jobId + "] on node [" + node + "], because this node is full. " +
                        "Number of opened jobs [" + numberOfAssignedJobs + "], " + MAX_OPEN_JOBS_PER_NODE.getKey() +
                        " [" + maxNumberOfOpenJobs + "]";
                logger.trace(reason);
                reasons.add(reason);
                continue;
            }

            if (maxAvailable < available) {
                maxAvailable = available;
                minLoadedNode = node;
            }
        }
        if (minLoadedNode != null) {
            logger.debug("selected node [{}] for job [{}]", minLoadedNode, jobId);
            return new Assignment(minLoadedNode.getId(), "");
        } else {
            String explanation = String.join("|", reasons);
            logger.debug("no node selected for job [{}], reasons [{}]", jobId, explanation);
            return new Assignment(null, explanation);
        }
    }

    static String[] indicesOfInterest(ClusterState clusterState, String job) {
        String jobResultIndex = AnomalyDetectorsIndex.getPhysicalIndexFromState(clusterState, job);
        return new String[]{AnomalyDetectorsIndex.jobStateIndexName(), jobResultIndex, MlMetaIndex.INDEX_NAME};
    }

    static List<String> verifyIndicesPrimaryShardsAreActive(String jobId, ClusterState clusterState) {
        String[] indices = indicesOfInterest(clusterState, jobId);
        List<String> unavailableIndices = new ArrayList<>(indices.length);
        for (String index : indices) {
            // Indices are created on demand from templates.
            // It is not an error if the index doesn't exist yet
            if (clusterState.metaData().hasIndex(index) == false) {
                continue;
            }
            IndexRoutingTable routingTable = clusterState.getRoutingTable().index(index);
            if (routingTable == null || routingTable.allPrimaryShardsActive() == false) {
                unavailableIndices.add(index);
            }
        }
        return unavailableIndices;
    }

    static boolean nodeSupportsJobVersion(Version nodeVersion, Version jobVersion) {
        return nodeVersion.onOrAfter(Version.V_5_5_0);
    }

    static String[] mappingRequiresUpdate(ClusterState state, String[] concreteIndices, Version minVersion, Logger logger) {
        List<String> indicesToUpdate = new ArrayList<>();

        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> currentMapping = state.metaData().findMappings(concreteIndices,
                new String[] { ElasticsearchMappings.DOC_TYPE });

        for (String index : concreteIndices) {
            ImmutableOpenMap<String, MappingMetaData> innerMap = currentMapping.get(index);
            if (innerMap != null) {
                MappingMetaData metaData = innerMap.get(ElasticsearchMappings.DOC_TYPE);
                try {
                    Map<String, Object> meta = (Map<String, Object>) metaData.sourceAsMap().get("_meta");
                    if (meta != null) {
                        String versionString = (String) meta.get("version");
                        if (versionString == null) {
                            logger.info("Version of mappings for [{}] not found, recreating", index);
                            indicesToUpdate.add(index);
                            continue;
                        }

                        Version mappingVersion = Version.fromString(versionString);

                        if (mappingVersion.onOrAfter(minVersion)) {
                            continue;
                        } else {
                            logger.info("Mappings for [{}] are outdated [{}], updating it[{}].", index, mappingVersion, Version.CURRENT);
                            indicesToUpdate.add(index);
                            continue;
                        }
                    } else {
                        logger.info("Version of mappings for [{}] not found, recreating", index);
                        indicesToUpdate.add(index);
                        continue;
                    }
                } catch (Exception e) {
                    logger.error(new ParameterizedMessage("Failed to retrieve mapping version for [{}], recreating", index), e);
                    indicesToUpdate.add(index);
                    continue;
                }
            } else {
                logger.info("No mappings found for [{}], recreating", index);
                indicesToUpdate.add(index);
            }
        }
        return indicesToUpdate.toArray(new String[indicesToUpdate.size()]);
    }
}
