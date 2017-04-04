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
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
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
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.persistent.AllocatedPersistentTask;
import org.elasticsearch.xpack.persistent.PersistentTaskRequest;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.Assignment;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.xpack.persistent.PersistentTasksExecutor;
import org.elasticsearch.xpack.persistent.PersistentTasksService;
import org.elasticsearch.xpack.persistent.PersistentTasksService.PersistentTaskOperationListener;
import org.elasticsearch.xpack.persistent.PersistentTasksService.WaitForPersistentTaskStatusListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager.MAX_RUNNING_JOBS_PER_NODE;

public class OpenJobAction extends Action<OpenJobAction.Request, OpenJobAction.Response, OpenJobAction.RequestBuilder> {

    public static final OpenJobAction INSTANCE = new OpenJobAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/open";

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

    public static class Request extends PersistentTaskRequest {

        public static final ParseField IGNORE_DOWNTIME = new ParseField("ignore_downtime");
        public static final ParseField TIMEOUT = new ParseField("timeout");
        public static ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString(Request::setJobId, Job.ID);
            PARSER.declareBoolean(Request::setIgnoreDowntime, IGNORE_DOWNTIME);
            PARSER.declareString((request, val) ->
                    request.setTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
        }

        public static Request fromXContent(XContentParser parser) {
            return parseRequest(null, parser);
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

        Request() {
        }

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
            closeJob(reason);
        }

        void closeJob(String reason) {
            autodetectProcessManager.closeJob(jobId, false, reason);
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

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final XPackLicenseState licenseState;
        private final PersistentTasksService persistentTasksService;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool, XPackLicenseState licenseState,
                               PersistentTasksService persistentTasksService, ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings, NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, Request::new);
            this.licenseState = licenseState;
            this.persistentTasksService = persistentTasksService;
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            if (licenseState.isMachineLearningAllowed()) {
                PersistentTaskOperationListener finalListener = new PersistentTaskOperationListener() {
                    @Override
                    public void onResponse(long taskId) {
                        waitForJobStarted(taskId, request, listener);
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

        void waitForJobStarted(long taskId, Request request, ActionListener<Response> listener) {
            JobPredicate predicate = new JobPredicate();
            persistentTasksService.waitForPersistentTaskStatus(taskId, predicate, request.timeout,
                    new WaitForPersistentTaskStatusListener() {
                @Override
                public void onResponse(long taskId) {
                    listener.onResponse(new Response(predicate.opened));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }

        private class JobPredicate implements Predicate<PersistentTask<?>> {

            private volatile boolean opened;

            @Override
            public boolean test(PersistentTask<?> persistentTask) {
                if (persistentTask == null) {
                    return false;
                }
                JobState jobState = (JobState) persistentTask.getStatus();
                if (jobState == null) {
                    return false;
                }
                switch (jobState) {
                    case OPENED:
                        opened = true;
                        return true;
                    case FAILED:
                        return true;
                    default:
                        throw new IllegalStateException("Unexpected job state [" + jobState + "]");

                }
            }
        }
    }

    public static class OpenJobPersistentTasksExecutor extends PersistentTasksExecutor<Request> {

        private final AutodetectProcessManager autodetectProcessManager;
        private final XPackLicenseState licenseState;
        private final Auditor auditor;
        private final ThreadPool threadPool;

        private volatile int maxConcurrentJobAllocations;

        public OpenJobPersistentTasksExecutor(Settings settings, ThreadPool threadPool, XPackLicenseState licenseState,
                                              PersistentTasksService persistentTasksService, ClusterService clusterService,
                                              AutodetectProcessManager autodetectProcessManager, Auditor auditor) {
            super(settings, NAME, persistentTasksService, ThreadPool.Names.MANAGEMENT);
            this.licenseState = licenseState;
            this.autodetectProcessManager = autodetectProcessManager;
            this.auditor = auditor;
            this.threadPool = threadPool;
            this.maxConcurrentJobAllocations = MachineLearning.CONCURRENT_JOB_ALLOCATIONS.get(settings);
            clusterService.getClusterSettings()
                    .addSettingsUpdateConsumer(MachineLearning.CONCURRENT_JOB_ALLOCATIONS, this::setMaxConcurrentJobAllocations);
        }

        @Override
        public Assignment getAssignment(Request request, ClusterState clusterState) {
            Assignment assignment = selectLeastLoadedMlNode(request.getJobId(), clusterState, maxConcurrentJobAllocations, logger);
            writeAssignmentNotification(request.getJobId(), assignment, clusterState);
            return assignment;
        }

        @Override
        public void validate(Request request, ClusterState clusterState) {
            if (licenseState.isMachineLearningAllowed()) {
                // If we already know that we can't find an ml node because all ml nodes are running at capacity or
                // simply because there are no ml nodes in the cluster then we fail quickly here:
                MlMetadata mlMetadata = clusterState.metaData().custom(MlMetadata.TYPE);
                PersistentTasksCustomMetaData tasks = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
                OpenJobAction.validate(request.getJobId(), mlMetadata, tasks);
                Assignment assignment = selectLeastLoadedMlNode(request.getJobId(), clusterState, maxConcurrentJobAllocations, logger);
                if (assignment.getExecutorNode() == null) {
                    throw new ElasticsearchStatusException("cannot open job [" + request.getJobId() + "], no suitable nodes found, " +
                            "allocation explanation [{}]", RestStatus.TOO_MANY_REQUESTS, assignment.getExplanation());
                }
            } else {
                throw LicenseUtils.newComplianceException(XPackPlugin.MACHINE_LEARNING);
            }
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, Request request, ActionListener<TransportResponse.Empty> listener) {
            JobTask jobTask = (JobTask) task;
            jobTask.autodetectProcessManager = autodetectProcessManager;
            autodetectProcessManager.openJob(request.getJobId(), jobTask, request.isIgnoreDowntime(), e2 -> {
                if (e2 == null) {
                    listener.onResponse(new TransportResponse.Empty());
                } else {
                    listener.onFailure(e2);
                }
            });
        }

        void setMaxConcurrentJobAllocations(int maxConcurrentJobAllocations) {
            logger.info("Changing [{}] from [{}] to [{}]", MachineLearning.CONCURRENT_JOB_ALLOCATIONS.getKey(),
                    this.maxConcurrentJobAllocations, maxConcurrentJobAllocations);
            this.maxConcurrentJobAllocations = maxConcurrentJobAllocations;
        }

        private void writeAssignmentNotification(String jobId, Assignment assignment, ClusterState state) {
            // Forking as this code is called from cluster state update thread:
            // Should be ok as auditor uses index api which has its own tp
            threadPool.executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.warn("Failed to write assignment notification for job [" + jobId + "]", e);
                }

                @Override
                protected void doRun() throws Exception {
                    if (assignment.getExecutorNode() == null) {
                        auditor.warning(jobId, "No node found to open job. Reasons [" + assignment.getExplanation() + "]");
                    } else {
                        DiscoveryNode node = state.nodes().get(assignment.getExecutorNode());
                        auditor.info(jobId, "Found node [" + node + "] to open job");
                    }
                }
            });
        }
    }

    /**
     * Fail fast before trying to update the job state on master node if the job doesn't exist or its state
     * is not what it should be.
     */
    static void validate(String jobId, MlMetadata mlMetadata, @Nullable PersistentTasksCustomMetaData tasks) {
        Job job = mlMetadata.getJobs().get(jobId);
        if (job == null) {
            throw ExceptionsHelper.missingJobException(jobId);
        }
        if (job.isDeleted()) {
            throw ExceptionsHelper.conflictStatusException("Cannot open job [" + jobId + "] because it has been marked as deleted");
        }
        PersistentTask<?> task = MlMetadata.getJobTask(jobId, tasks);
        if (task != null) {
            throw ExceptionsHelper.conflictStatusException("Cannot open job [" + jobId + "] because it has already been opened");
        }
    }

    static Assignment selectLeastLoadedMlNode(String jobId, ClusterState clusterState, int maxConcurrentJobAllocations,
                                              Logger logger) {
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
            String maxNumberOfOpenJobsStr = nodeAttributes.get(AutodetectProcessManager.MAX_RUNNING_JOBS_PER_NODE.getKey());
            if (maxNumberOfOpenJobsStr == null) {
                String reason = "Not opening job [" + jobId + "] on node [" + node + "], because this node isn't a ml node.";
                logger.trace(reason);
                reasons.add(reason);
                continue;
            }

            long numberOfAssignedJobs;
            int numberOfAllocatingJobs;
            if (persistentTasks != null) {
                numberOfAssignedJobs = persistentTasks.getNumberOfTasksOnNode(node.getId(), OpenJobAction.NAME);
                numberOfAllocatingJobs = persistentTasks.findTasks(OpenJobAction.NAME, task -> {
                    if (node.getId().equals(task.getExecutorNode()) == false) {
                        return false;
                    }
                    JobState jobTaskState = (JobState) task.getStatus();
                    return jobTaskState == null || // executor node didn't have the chance to set job status to OPENING
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
                logger.trace(reason);
                reasons.add(reason);
                continue;
            }

            long maxNumberOfOpenJobs = Long.parseLong(maxNumberOfOpenJobsStr);
            long available = maxNumberOfOpenJobs - numberOfAssignedJobs;
            if (available == 0) {
                String reason = "Not opening job [" + jobId + "] on node [" + node + "], because this node is full. " +
                        "Number of opened jobs [" + numberOfAssignedJobs + "], " + MAX_RUNNING_JOBS_PER_NODE.getKey() +
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
        return new String[]{AnomalyDetectorsIndex.jobStateIndexName(), jobResultIndex, AnomalyDetectorsIndex.ML_META_INDEX};
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
}
