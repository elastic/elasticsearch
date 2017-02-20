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
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
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
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.utils.JobStateObserver;
import org.elasticsearch.xpack.persistent.PersistentActionRegistry;
import org.elasticsearch.xpack.persistent.PersistentActionRequest;
import org.elasticsearch.xpack.persistent.PersistentActionResponse;
import org.elasticsearch.xpack.persistent.PersistentActionService;
import org.elasticsearch.xpack.persistent.PersistentTask;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress.PersistentTaskInProgress;
import org.elasticsearch.xpack.persistent.TransportPersistentAction;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager.MAX_RUNNING_JOBS_PER_NODE;

public class OpenJobAction extends Action<OpenJobAction.Request, PersistentActionResponse, OpenJobAction.RequestBuilder> {

    public static final OpenJobAction INSTANCE = new OpenJobAction();
    public static final String NAME = "cluster:admin/ml/anomaly_detectors/open";

    private OpenJobAction() {
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

    public static class Request extends PersistentActionRequest {

        public static final ParseField IGNORE_DOWNTIME = new ParseField("ignore_downtime");
        public static final ParseField TIMEOUT = new ParseField("timeout");
        public static ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString(Request::setJobId, Job.ID);
            PARSER.declareBoolean(Request::setIgnoreDowntime, IGNORE_DOWNTIME);
            PARSER.declareString((request, val) ->
                    request.setTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
        }

        public static Request parseRequest(String jobId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (jobId != null) {
                request.jobId = jobId;
            }
            return request;
        }

        private String jobId;
        private boolean ignoreDowntime = true;
        private TimeValue timeout = TimeValue.timeValueSeconds(20);

        public Request(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        public Request(StreamInput in) throws IOException {
            readFrom(in);
        }

       Request() {}

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public boolean isIgnoreDowntime() {
            return ignoreDowntime;
        }

        public void setIgnoreDowntime(boolean ignoreDowntime) {
            this.ignoreDowntime = ignoreDowntime;
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        public void setTimeout(TimeValue timeout) {
            this.timeout = timeout;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId) {
            return new JobTask(getJobId(), id, type, action, parentTaskId);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobId = in.readString();
            ignoreDowntime = in.readBoolean();
            timeout = TimeValue.timeValueMillis(in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeBoolean(ignoreDowntime);
            out.writeVLong(timeout.millis());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            builder.field(IGNORE_DOWNTIME.getPreferredName(), ignoreDowntime);
            builder.field(TIMEOUT.getPreferredName(), timeout.getStringRep());
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, ignoreDowntime, timeout);
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
            return Objects.equals(jobId, other.jobId) &&
                    Objects.equals(ignoreDowntime, other.ignoreDowntime) &&
                    Objects.equals(timeout, other.timeout);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public static class JobTask extends PersistentTask {

        private final String jobId;
        private volatile Consumer<String> cancelHandler;

        JobTask(String jobId, long id, String type, String action, TaskId parentTask) {
            super(id, type, action, "job-" + jobId, parentTask);
            this.jobId = jobId;
        }

        public String getJobId() {
            return jobId;
        }

        @Override
        protected void onCancelled() {
            String reason = CancelTasksRequest.DEFAULT_REASON.equals(getReasonCancelled()) ? null : getReasonCancelled();
            cancelHandler.accept(reason);
        }

        static boolean match(Task task, String expectedJobId) {
            String expectedDescription = "job-" + expectedJobId;
            return task instanceof JobTask && expectedDescription.equals(task.getDescription());
        }

    }

    static class RequestBuilder extends ActionRequestBuilder<Request, PersistentActionResponse, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client, OpenJobAction action) {
            super(client, action, new Request());
        }
    }

    public static class TransportAction extends TransportPersistentAction<Request> {

        private final JobStateObserver observer;
        private final ClusterService clusterService;
        private final AutodetectProcessManager autodetectProcessManager;
        private XPackLicenseState licenseState;

        private volatile int maxConcurrentJobAllocations;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool, XPackLicenseState licenseState,
                               PersistentActionService persistentActionService, PersistentActionRegistry persistentActionRegistry,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               ClusterService clusterService, AutodetectProcessManager autodetectProcessManager) {
            super(settings, OpenJobAction.NAME, false, threadPool, transportService, persistentActionService,
                    persistentActionRegistry, actionFilters, indexNameExpressionResolver, Request::new, ThreadPool.Names.MANAGEMENT);
            this.licenseState = licenseState;
            this.clusterService = clusterService;
            this.autodetectProcessManager = autodetectProcessManager;
            this.observer = new JobStateObserver(threadPool, clusterService);
            this.maxConcurrentJobAllocations = MachineLearning.CONCURRENT_JOB_ALLOCATIONS.get(settings);
            clusterService.getClusterSettings()
                    .addSettingsUpdateConsumer(MachineLearning.CONCURRENT_JOB_ALLOCATIONS, this::setMaxConcurrentJobAllocations);
        }

        @Override
        protected void doExecute(Request request, ActionListener<PersistentActionResponse> listener) {
            if (licenseState.isMachineLearningAllowed()) {
                // If we already know that we can't find an ml node because all ml nodes are running at capacity or
                // simply because there are no ml nodes in the cluster then we fail quickly here:
                ClusterState clusterState = clusterService.state();
                if (selectLeastLoadedMlNode(request.getJobId(), clusterState, maxConcurrentJobAllocations, logger) == null) {
                    throw new ElasticsearchStatusException("no nodes available to open job [" + request.getJobId() + "]",
                            RestStatus.TOO_MANY_REQUESTS);
                }

                ActionListener<PersistentActionResponse> finalListener =
                         ActionListener.wrap(response -> waitForJobStarted(request, response, listener), listener::onFailure);
                super.doExecute(request, finalListener);
            } else {
                listener.onFailure(LicenseUtils.newComplianceException(XPackPlugin.MACHINE_LEARNING));
            }
        }

        void waitForJobStarted(Request request, PersistentActionResponse response, ActionListener<PersistentActionResponse> listener) {
            observer.waitForState(request.getJobId(), request.timeout, JobState.OPENED, e -> {
                if (e != null) {
                    listener.onFailure(e);
                } else {
                    listener.onResponse(response);
                }
            });
        }

        @Override
        public DiscoveryNode executorNode(Request request, ClusterState clusterState) {
            return selectLeastLoadedMlNode(request.getJobId(), clusterState, maxConcurrentJobAllocations, logger);
        }

        @Override
        public void validate(Request request, ClusterState clusterState) {
            MlMetadata mlMetadata = clusterState.metaData().custom(MlMetadata.TYPE);
            PersistentTasksInProgress tasks = clusterState.getMetaData().custom(PersistentTasksInProgress.TYPE);
            OpenJobAction.validate(request.getJobId(), mlMetadata, tasks, clusterState.nodes());
        }

        @Override
        protected void nodeOperation(PersistentTask task, Request request, ActionListener<TransportResponse.Empty> listener) {
            autodetectProcessManager.setJobState(task.getPersistentTaskId(), JobState.OPENING, e1 -> {
                if (e1 != null) {
                    listener.onFailure(e1);
                    return;
                }

                JobTask jobTask = (JobTask) task;
                jobTask.cancelHandler = (reason) -> autodetectProcessManager.closeJob(request.getJobId(), reason);
                autodetectProcessManager.openJob(request.getJobId(), task.getPersistentTaskId(), request.isIgnoreDowntime(), e2 -> {
                    if (e2 == null) {
                        listener.onResponse(new TransportResponse.Empty());
                    } else {
                        listener.onFailure(e2);
                    }
                });
            });
        }

        void setMaxConcurrentJobAllocations(int maxConcurrentJobAllocations) {
            logger.info("Changing [{}] from [{}] to [{}]", MachineLearning.CONCURRENT_JOB_ALLOCATIONS.getKey(),
                    this.maxConcurrentJobAllocations, maxConcurrentJobAllocations);
            this.maxConcurrentJobAllocations = maxConcurrentJobAllocations;
        }
    }

    /**
     * Fail fast before trying to update the job state on master node if the job doesn't exist or its state
     * is not what it should be.
     */
    static void validate(String jobId, MlMetadata mlMetadata, @Nullable PersistentTasksInProgress tasks, DiscoveryNodes nodes) {
        Job job = mlMetadata.getJobs().get(jobId);
        if (job == null) {
            throw ExceptionsHelper.missingJobException(jobId);
        }
        if (job.isDeleted()) {
            throw new ElasticsearchStatusException("Cannot open job [" + jobId + "] because it has been marked as deleted",
                    RestStatus.CONFLICT);
        }
        PersistentTaskInProgress<?> task = MlMetadata.getJobTask(jobId, tasks);
        JobState jobState = MlMetadata.getJobState(jobId, tasks);
        if (task != null && jobState == JobState.OPENED) {
            if (task.getExecutorNode() == null) {
                // We can skip the job state check below, because the task got unassigned after we went into
                // opened state on a node that disappeared and we didn't have the opportunity to set the status to failed
                return;
            } else if (nodes.nodeExists(task.getExecutorNode()) == false) {
                // The state is open and the node were running on no longer exists.
                // We can skip the job state check below, because when the node
                // disappeared we didn't have time to set the status to failed.
                return;
            }
        }
        if (jobState.isAnyOf(JobState.CLOSED, JobState.FAILED) == false) {
            throw new ElasticsearchStatusException("[" + jobId + "] expected state [" + JobState.CLOSED
                    + "] or [" + JobState.FAILED + "], but got [" + jobState +"]", RestStatus.CONFLICT);
        }
    }

    static DiscoveryNode selectLeastLoadedMlNode(String jobId, ClusterState clusterState, int maxConcurrentJobAllocations,
                                                 Logger logger) {
        long maxAvailable = Long.MIN_VALUE;
        List<String> reasons = new LinkedList<>();
        DiscoveryNode minLoadedNode = null;
        PersistentTasksInProgress persistentTasksInProgress = clusterState.getMetaData().custom(PersistentTasksInProgress.TYPE);
        for (DiscoveryNode node : clusterState.getNodes()) {
            Map<String, String> nodeAttributes = node.getAttributes();
            String maxNumberOfOpenJobsStr = nodeAttributes.get(AutodetectProcessManager.MAX_RUNNING_JOBS_PER_NODE.getKey());
            if (maxNumberOfOpenJobsStr == null) {
                String reason = "Not opening job [" + jobId + "] on node [" + node + "], because this node isn't a ml node.";
                logger.debug(reason);
                reasons.add(reason);
                continue;
            }

            long numberOfAssignedJobs;
            int numberOfAllocatingJobs;
            if (persistentTasksInProgress != null) {
                numberOfAssignedJobs = persistentTasksInProgress.getNumberOfTasksOnNode(node.getId(), OpenJobAction.NAME);
                numberOfAllocatingJobs = persistentTasksInProgress.findTasks(OpenJobAction.NAME, task -> {
                    if (node.getId().equals(task.getExecutorNode()) == false) {
                        return false;
                    }
                    JobState jobTaskState = (JobState) task.getStatus();
                    return jobTaskState == null || // executor node didn't have the chance to set job status to OPENING
                            jobTaskState == JobState.OPENING || // executor node is busy starting the cpp process
                            task.isCurrentStatus() == false; // previous executor node failed and
                            // current executor node didn't have the chance to set job status to OPENING
                }).size();
            } else {
                numberOfAssignedJobs = 0;
                numberOfAllocatingJobs = 0;
            }
            if (numberOfAllocatingJobs >= maxConcurrentJobAllocations) {
                String reason = "Not opening job [" + jobId + "] on node [" + node + "], because node exceeds [" + numberOfAllocatingJobs +
                        "] the maximum number of jobs [" + maxConcurrentJobAllocations + "] in opening state";
                logger.debug(reason);
                reasons.add(reason);
                continue;
            }

            long maxNumberOfOpenJobs = Long.parseLong(maxNumberOfOpenJobsStr);
            long available = maxNumberOfOpenJobs - numberOfAssignedJobs;
            if (available == 0) {
                String reason = "Not opening job [" + jobId + "] on node [" + node + "], because this node is full. " +
                        "Number of opened jobs [" + numberOfAssignedJobs + "], " + MAX_RUNNING_JOBS_PER_NODE.getKey() +
                        " [" + maxNumberOfOpenJobs + "]";
                logger.debug(reason);
                reasons.add(reason);
                continue;
            }

            if (maxAvailable < available) {
                maxAvailable = available;
                minLoadedNode = node;
            }
        }
        if (minLoadedNode != null) {
            logger.info("selected node [{}] for job [{}]", minLoadedNode, jobId);
        } else {
            logger.info("no node selected for job [{}], reasons [{}]", jobId, String.join(",\n", reasons));
        }
        return minLoadedNode;
    }
}
