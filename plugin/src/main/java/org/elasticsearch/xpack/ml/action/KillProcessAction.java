/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;

import java.io.IOException;
import java.util.Objects;

public class KillProcessAction extends Action<KillProcessAction.Request, KillProcessAction.Response,
        KillProcessAction.RequestBuilder> {

    public static final KillProcessAction INSTANCE = new KillProcessAction();
    public static final String NAME = "cluster:internal/xpack/ml/job/kill/process";

    private KillProcessAction() {
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

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client, KillProcessAction action) {
            super(client, action, new Request());
        }
    }

    public static class Request extends TransportJobTaskAction.JobTaskRequest<Request> {

        public Request(String jobId) {
            super(jobId);
        }

        Request() {
            super();
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable {

        private boolean killed;

        Response() {
            super(null, null);
        }

        Response(StreamInput in) throws IOException {
            super(null, null);
            readFrom(in);
        }

        Response(boolean killed) {
            super(null, null);
            this.killed = killed;
        }

        public boolean isKilled() {
            return killed;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            killed = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(killed);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return killed == response.killed;
        }

        @Override
        public int hashCode() {
            return Objects.hash(killed);
        }
    }

    public static class TransportAction extends TransportJobTaskAction<Request, Response> {

        private final Auditor auditor;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool, ClusterService clusterService,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               AutodetectProcessManager processManager, Auditor auditor) {
            super(settings, NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                    Request::new, Response::new, MachineLearning.UTILITY_THREAD_POOL_NAME, processManager);
            this.auditor = auditor;
        }

        @Override
        protected void taskOperation(Request request, OpenJobAction.JobTask jobTask, ActionListener<Response> listener) {
            logger.info("[{}] Killing job", jobTask.getJobId());
            auditor.info(jobTask.getJobId(), Messages.JOB_AUDIT_KILLING);

            try {
                processManager.killProcess(jobTask, true, null);
                listener.onResponse(new Response(true));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            DiscoveryNodes nodes = clusterService.state().nodes();
            PersistentTasksCustomMetaData tasks = clusterService.state().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
            PersistentTasksCustomMetaData.PersistentTask<?> jobTask = MlMetadata.getJobTask(request.getJobId(), tasks);
            if (jobTask == null || jobTask.getExecutorNode() == null) {
                logger.debug("[{}] Cannot kill the process because job is not open", request.getJobId());
                listener.onResponse(new Response(false));
                return;
            }

            DiscoveryNode executorNode = nodes.get(jobTask.getExecutorNode());
            if (executorNode == null) {
                listener.onFailure(ExceptionsHelper.conflictStatusException("Cannot kill process for job {} as" +
                        "executor node {} cannot be found", request.getJobId(), jobTask.getExecutorNode()));
                return;
            }

            Version nodeVersion = executorNode.getVersion();
            if (nodeVersion.before(Version.V_5_5_0)) {
                listener.onFailure(new ElasticsearchException("Cannot kill the process on node with version " + nodeVersion));
                return;
            }

            super.doExecute(task, request, listener);
        }


        @Override
        protected Response readTaskResponse(StreamInput in) throws IOException {
            return new Response(in);
        }
    }
}
