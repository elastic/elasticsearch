/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.cluster.coordination;

import co.elastic.elasticsearch.stateless.Stateless;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Transport action used to do a consistent cluster state read against the currently elected master.
 * This action relies on the blob store term lease to ensure that the reading node is still member of the cluster,
 * waiting to re-join otherwise. Once the blob store term lease (term, node-left-generation) are aligned the action
 * tries to read the current (term, version) from the latest known elected master. Knowing the elected master (term, version)
 * the local waits until the locally applied cluster state is at least on the (term, version).
 */
public final class TransportConsistentClusterStateReadAction extends TransportAction<
    TransportConsistentClusterStateReadAction.Request,
    TransportConsistentClusterStateReadAction.Response> {

    public static final String NAME = "internal:admin/" + Stateless.NAME + "/coordination/consistent_cluster_state_read";
    // visible for testing
    static final String MASTER_NODE_ACTION = NAME + "[m]";

    public static final ActionType<Response> TYPE = ActionType.localOnly(NAME);

    private final Logger logger = LogManager.getLogger(TransportConsistentClusterStateReadAction.class);
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final Executor executor;
    private final StatelessClusterConsistencyService consistencyService;
    private final ThreadPool threadPool;

    @Inject
    public TransportConsistentClusterStateReadAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        StatelessClusterConsistencyService consistencyService
    ) {
        super(NAME, actionFilters, transportService.getTaskManager());
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.executor = EsExecutors.DIRECT_EXECUTOR_SERVICE;
        this.consistencyService = consistencyService;
        this.threadPool = clusterService.threadPool();
        transportService.registerRequestHandler(
            MASTER_NODE_ACTION,
            executor,
            ReadMasterTermAndVersionRequest::new,
            (request, channel, task) -> readMasterClusterStateTermAndVersion(task, request, new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected void doExecute(
        Task task,
        TransportConsistentClusterStateReadAction.Request request,
        ActionListener<TransportConsistentClusterStateReadAction.Response> listener
    ) {
        request.incRef();
        if (task != null) {
            request.setParentTask(clusterService.localNode().getId(), task.getId());
        }

        SubscribableListener.<Void>newForked(l -> consistencyService.ensureClusterStateConsistentWithRootBlob(l, TimeValue.MAX_VALUE))
            .<ReadMasterTermAndVersionResponse>andThen((l, unused) -> getCurrentMasterClusterStateTermAndVersion(l, task))
            .<Response>andThen((l, response) -> waitForClusterStateToBeAppliedLocally(l, response, task))
            .addListener(ActionListener.runBefore(listener, request::decRef));
    }

    private void getCurrentMasterClusterStateTermAndVersion(ActionListener<ReadMasterTermAndVersionResponse> listener, Task task) {
        if (isTaskCancelled(task)) {
            listener.onFailure(new TaskCancelledException("Task was cancelled"));
            return;
        }

        var state = consistencyService.state();
        var currentMasterNode = state.nodes().getMasterNode();

        // Once the local node is in the same (term, node-left-generation) as the root blob;
        // we need to ensure that the locally applied state is at least the same as the latest
        // known (term, version) to the elected master (as viewed by the local node) and wait
        // until it's applied if the local node is behind.
        if (currentMasterNode == null) {
            listener.onFailure(new MasterNotDiscoveredException());
        } else {
            // It's possible that the node that we're sending the request to is not the elected master
            // anymore. But we're only interested in knowing what is its latest (term, version) so the
            // local node can wait until it has applied at least the cluster state with the received (term, version)
            transportService.sendRequest(
                currentMasterNode,
                MASTER_NODE_ACTION,
                ReadMasterTermAndVersionRequest.INSTANCE,
                new ActionListenerResponseHandler<>(listener, ReadMasterTermAndVersionResponse::new, executor)
            );
        }
    }

    private void waitForClusterStateToBeAppliedLocally(
        ActionListener<Response> listener,
        ReadMasterTermAndVersionResponse response,
        Task task
    ) {
        if (isTaskCancelled(task)) {
            listener.onFailure(new TaskCancelledException("Task was cancelled"));
            return;
        }
        ClusterStateObserver.waitForState(clusterService, threadPool.getThreadContext(), new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                listener.onResponse(new Response(state));
            }

            @Override
            public void onClusterServiceClose() {
                listener.onFailure(new NodeClosedException(clusterService.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                listener.onFailure(new RuntimeException());
            }
        }, state -> state.term() >= response.getTerm() && state.version() >= response.getVersion(), TimeValue.MAX_VALUE, logger);
    }

    private void readMasterClusterStateTermAndVersion(
        Task task,
        ReadMasterTermAndVersionRequest request,
        ActionListener<ReadMasterTermAndVersionResponse> listener
    ) {
        request.incRef();

        try {
            if (isTaskCancelled(task)) {
                listener.onFailure(new TaskCancelledException("Task was cancelled"));
                return;
            }

            var state = clusterService.state();
            var blockException = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
            if (blockException != null) {
                listener.onFailure(blockException);
                return;
            }

            listener.onResponse(new ReadMasterTermAndVersionResponse(state.term(), state.version()));
        } finally {
            request.decRef();
        }
    }

    private boolean isTaskCancelled(Task task) {
        return task instanceof CancellableTask && ((CancellableTask) task).isCancelled();
    }

    private static class ReadMasterTermAndVersionRequest extends ActionRequest {
        private static final ReadMasterTermAndVersionRequest INSTANCE = new ReadMasterTermAndVersionRequest();

        ReadMasterTermAndVersionRequest() {}

        ReadMasterTermAndVersionRequest(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    private static class ReadMasterTermAndVersionResponse extends ActionResponse {
        private final long term;
        private final long version;

        private ReadMasterTermAndVersionResponse(long term, long version) {
            this.term = term;
            this.version = version;
        }

        ReadMasterTermAndVersionResponse(StreamInput in) throws IOException {
            super(in);
            term = in.readLong();
            version = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(term);
            out.writeLong(version);
        }

        public long getTerm() {
            return term;
        }

        public long getVersion() {
            return version;
        }
    }

    public static class Request extends ActionRequest {
        public Request() {}

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse {
        private final ClusterState state;

        public Response(ClusterState state) {
            this.state = state;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }

        public ClusterState getState() {
            return state;
        }
    }
}
