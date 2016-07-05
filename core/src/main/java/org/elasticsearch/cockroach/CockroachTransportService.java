/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cockroach;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

import static org.elasticsearch.cluster.ClusterState.registerPrototype;

/**
 * Service responsible for executing restartable actions that can survive disappearance of a coordinating and executor nodes.
 * <p>
 * In order to be resilient to node restarts, the cockroach actions are using the cluster state instead of a transport service to send
 * requests and responses.
 */
public class CockroachTransportService extends AbstractLifecycleComponent {

    static {
        registerPrototype(CockroachTasksInProgress.TYPE, CockroachTasksInProgress.PROTO);
    }

    public static final String START_COCKROACH_ACTION_NAME = "internal:cluster/cockroach/start";
    public static final String UPDATE_COCKROACH_ACTION_NAME = "internal:cluster/cockroach/update";
    public static final String REMOVE_COCKROACH_ACTION_NAME = "internal:cluster/cockroach/remove";

    private final ClusterService clusterService;
    private final TransportService transportService;

    @Nullable // it should only exist on the master-eligible nodes
    private final CockroachActionCoordinator cockroachActionCoordinator;

    @Inject
    public CockroachTransportService(Settings settings, ClusterService clusterService, TransportService transportService,
                                     CockroachActionRegistry cockroachActionRegistry) {
        super(settings);
        this.clusterService = clusterService;
        this.transportService = transportService;
        if (DiscoveryNode.isMasterNode(settings)) {
            this.cockroachActionCoordinator = new CockroachActionCoordinator(settings, clusterService, cockroachActionRegistry);
            this.clusterService.add(this.cockroachActionCoordinator);
            // This needs to run only on nodes that can become masters
            transportService.registerRequestHandler(START_COCKROACH_ACTION_NAME, StartCockroachTaskRequest::new, ThreadPool.Names.SAME,
                new StartCockroachTaskRequestHandler());
            transportService.registerRequestHandler(UPDATE_COCKROACH_ACTION_NAME, UpdateCockroachTaskRequest::new, ThreadPool.Names.SAME,
                new UpdateCockroachTaskRequestHandler());
            transportService.registerRequestHandler(REMOVE_COCKROACH_ACTION_NAME, RemoveCockroachTaskRequest::new, ThreadPool.Names.SAME,
                new RemoveCockroachTaskRequestHandler());
        } else {
            this.cockroachActionCoordinator = null;
        }

    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {
        clusterService.remove(cockroachActionCoordinator);
    }

    /**
     * Sends a request to the master node to register a cockroach task in the cluster state
     *
     * @param task     the task id of the caller
     * @param action   the action name
     * @param request  cockroach action request
     * @param listener the listener that will be called when the task is registered
     */
    public <Request extends CockroachRequest<Request>> void sendRequest(final Task task, final String action, final Request request,
                                                                        ActionListener<Empty> listener) {
        try {
            String nodeId = clusterService.state().getNodes().getLocalNodeId();
            if (clusterService.state().nodes().isLocalNodeElectedMaster()) {
                cockroachActionCoordinator.createCockroachTask(new TaskId(nodeId, task.getId()), action, nodeId, request, listener);
            } else {
                StartCockroachTaskRequest<Request> startRequest = new StartCockroachTaskRequest<>(nodeId, action, request);
                startRequest.setParentTask(nodeId, task.getId());
                transportService.sendRequest(clusterService.state().nodes().getMasterNode(), START_COCKROACH_ACTION_NAME, startRequest,
                    new ActionListenerEmptyTransportResponseHandler(ThreadPool.Names.SAME, listener));
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Asks the master node to assign a response to the running cockroach task
     * <p>
     * This method is used to start a cockroach task.
     *
     * @param task     the caller task
     * @param uuid     the uuid of the running task
     * @param response response that should be assigned to the task
     * @param listener the listener that will be called when the task is updated
     */
    public <Response extends CockroachResponse> void sendResponse(final Task task, final String uuid, final Response response,
                                                                  final ActionListener<Empty> listener) {
        sendResponse(task, uuid, response, null, listener);
    }

    /**
     * Asks the master node to assign a failure to the running cockroach task
     * <p>
     * This method is used to to indicate that the executor node successfully finished the running task.
     *
     * @param task     the caller task
     * @param uuid     the uuid of the running task
     * @param failure  failure that should be assigned to the task
     * @param listener the listener that will be called when the task is updated
     */
    public void sendFailure(final Task task, final String uuid, final Exception failure, final ActionListener<Empty> listener) {
        sendResponse(task, uuid, null, failure, listener);
    }

    private <Response extends CockroachResponse> void sendResponse(final Task task, final String uuid, final Response response,
                                                                   final Exception failure, final ActionListener<Empty> listener) {
        try {
            String nodeId = clusterService.state().getNodes().getLocalNodeId();
            if (clusterService.state().nodes().isLocalNodeElectedMaster()) {
                cockroachActionCoordinator.finishCockroachTask(uuid, response, failure, listener);
            } else {
                UpdateCockroachTaskRequest<Response> startRequest = new UpdateCockroachTaskRequest<>(uuid, response, failure);
                startRequest.setParentTask(nodeId, task.getId());
                transportService.sendRequest(clusterService.state().nodes().getMasterNode(), UPDATE_COCKROACH_ACTION_NAME, startRequest,
                    new ActionListenerEmptyTransportResponseHandler(ThreadPool.Names.GENERIC, listener));
            }
        } catch (Exception t) {
            listener.onFailure(t);
        }
    }

    /**
     * Asks the master node to remove the running cockroach task
     * <p>
     * This method is called when the caller node has acknowledged the receive the response.
     *
     * @param task     the caller task
     * @param uuid     the uuid of the running task
     * @param listener the listener that will be called when the task is removed
     */
    public void acknowledgeResponse(final Task task, final String uuid, final ActionListener<Empty> listener) {
        try {
            String nodeId = clusterService.state().getNodes().getLocalNodeId();
            if (clusterService.state().nodes().isLocalNodeElectedMaster()) {
                cockroachActionCoordinator.removeCockroachTask(uuid, listener);
            } else {
                RemoveCockroachTaskRequest removeRequest = new RemoveCockroachTaskRequest(uuid);
                removeRequest.setParentTask(nodeId, task.getId());
                transportService.sendRequest(clusterService.state().nodes().getMasterNode(), REMOVE_COCKROACH_ACTION_NAME, removeRequest,
                    new ActionListenerEmptyTransportResponseHandler(ThreadPool.Names.GENERIC, listener));
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private static class StartCockroachTaskRequest<Request extends CockroachRequest<Request>>
        extends TransportRequest {
        private String action;
        private String callerNodeId;
        private Request request;

        private StartCockroachTaskRequest() {

        }

        public StartCockroachTaskRequest(String callerNodeId, String action, Request request) {
            this.callerNodeId = callerNodeId;
            this.action = action;
            this.request = request;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            callerNodeId = in.readString();
            action = in.readString();
            request = (Request) in.readNamedWriteable(CockroachRequest.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(callerNodeId);
            out.writeString(action);
            out.writeNamedWriteable(request);
        }

        public String getAction() {
            return action;
        }

        public Request getRequest() {
            return request;
        }

        public String getCallerNodeId() {
            return callerNodeId;
        }
    }

    /**
     * Transport request handler that is used to send changes in snapshot status to master
     */
    class StartCockroachTaskRequestHandler implements TransportRequestHandler<StartCockroachTaskRequest> {
        @Override
        public void messageReceived(StartCockroachTaskRequest request, final TransportChannel channel) throws Exception {
            assert cockroachActionCoordinator != null;
            cockroachActionCoordinator.createCockroachTask(request.getParentTask(), request.getAction(), request.getCallerNodeId(),
                request.getRequest(), new EmptyActionListener(channel));

        }
    }

    private class EmptyActionListener implements ActionListener<Empty> {
        private final TransportChannel channel;

        public EmptyActionListener(final TransportChannel channel) {
            this.channel = channel;
        }

        @Override
        public void onResponse(final Empty empty) {
            try {
                channel.sendResponse(Empty.INSTANCE);
            } catch (IOException e) {
                onFailure(e);
            }
        }

        @Override
        public void onFailure(final Exception e) {
            try {
                channel.sendResponse(e);
            } catch (IOException e1) {
                logger.warn("failed to send response", e);
            }
        }
    }

    private static class UpdateCockroachTaskRequest<Response extends CockroachResponse> extends TransportRequest {
        private String uuid;
        @Nullable
        private Exception failure;
        @Nullable
        private Response response;

        private UpdateCockroachTaskRequest() {

        }

        public UpdateCockroachTaskRequest(String uuid, Response response, Exception failure) {
            this.uuid = uuid;
            this.response = response;
            this.failure = failure;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            uuid = in.readString();
            failure = in.readException();
            response = (Response) in.readOptionalNamedWriteable(CockroachResponse.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(uuid);
            out.writeException(failure);
            out.writeOptionalNamedWriteable(response);
        }

        public String getUuid() {
            return uuid;
        }

        public Exception getFailure() {
            return failure;
        }

        public Response getResponse() {
            return response;
        }
    }

    /**
     * Transport request handler that is used to send changes in snapshot status to master
     */
    class UpdateCockroachTaskRequestHandler implements TransportRequestHandler<UpdateCockroachTaskRequest> {
        @Override
        public void messageReceived(UpdateCockroachTaskRequest request, final TransportChannel channel) throws Exception {
            assert cockroachActionCoordinator != null;
            cockroachActionCoordinator.finishCockroachTask(request.getUuid(), request.getResponse(), request.getFailure(),
                new EmptyActionListener(channel));
        }
    }

    private static class RemoveCockroachTaskRequest extends TransportRequest {
        private String uuid;

        private RemoveCockroachTaskRequest() {

        }

        public RemoveCockroachTaskRequest(String uuid) {
            this.uuid = uuid;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            uuid = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(uuid);
        }

        public String getUuid() {
            return uuid;
        }
    }

    /**
     * Transport request handler that is used to send changes in snapshot status to master
     */
    class RemoveCockroachTaskRequestHandler implements TransportRequestHandler<RemoveCockroachTaskRequest> {
        @Override
        public void messageReceived(RemoveCockroachTaskRequest request, final TransportChannel channel) throws Exception {
            assert cockroachActionCoordinator != null;
            cockroachActionCoordinator.removeCockroachTask(request.getUuid(), new EmptyActionListener(channel));
        }
    }

    private static final class ActionListenerEmptyTransportResponseHandler extends EmptyTransportResponseHandler {

        private final ActionListener<Empty> listener;

        public ActionListenerEmptyTransportResponseHandler(final String executor, final ActionListener<Empty> listener) {
            super(executor);
            this.listener = listener;
        }

        @Override
        public void handleResponse(Empty response) {
            super.handleResponse(response);
            listener.onResponse(response);
        }

        @Override
        public void handleException(TransportException exp) {
            super.handleException(exp);
            listener.onFailure(exp);
        }
    }
}
