/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugin.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class TransportTestAction extends HandledTransportAction<TestRequest, TestResponse> {
    static final Logger logger = LogManager.getLogger(TransportTestAction.class);
    static final ActionType<TestResponse> ACTION = new ActionType<>("internal::test_action", TestResponse::new);
    final TransportService transportService;
    final Map<String, CountDownLatch> latches = ConcurrentCollections.newConcurrentMap();
    final ClusterService clusterService;

    @Inject
    public TransportTestAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService) {
        super(ACTION.name(), transportService, actionFilters, TestRequest::new, ThreadPool.Names.GENERIC);
        this.transportService = transportService;
        this.clusterService = clusterService;
        TransportActionProxy.registerProxyAction(transportService, ACTION.name(), TestResponse::new);
    }

    @Override
    protected void doExecute(Task task, TestRequest request, ActionListener<TestResponse> origListener) {
        ActionListener<TestResponse> groupedListener = new GroupedActionListener<>(
            ActionListener.map(origListener, resp -> new TestResponse(resp.stream().allMatch(AcknowledgedResponse::isAcknowledged))),
            request.targets.size()
        );
        for (TestRequest.Target target : request.targets) {
            if (target.nodeId.equals(transportService.getLocalNode().getId())) {
                executeSubRequestLocally((BlockingCancellableTask) task, request.id, groupedListener);
            } else {
                dispatchSubRequest(task, request, target, groupedListener);
            }
        }
    }

    void dispatchSubRequest(Task parentTask, TestRequest request, TestRequest.Target target, ActionListener<TestResponse> listener) {
        if (Strings.isEmpty(target.clusterAlias)) {
            final DiscoveryNode targetNode = clusterService.state().nodes().get(target.nodeId);
            if (targetNode == null) {
                listener.onFailure(new IllegalAccessException("node" + target.nodeId + " not found"));
                return;
            }
            final TestRequest subRequest = new TestRequest(request.id, Collections.singleton(target));
            logger.info("dispatch sub request {} to target node {}", subRequest, target);
            transportService.sendChildRequest(
                targetNode,
                actionName,
                subRequest,
                parentTask,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(listener, TestResponse::new)
            );
        } else {
            if (transportService.getLocalNode().isRemoteClusterClient()) {
                // strip out the cluster alias
                final TestRequest subRequest = new TestRequest(request.id, Collections.singleton(target));
                logger.info("dispatch sub request {} to remote cluster", subRequest);
                dispatchSubRequestToRemoteCluster(parentTask, request, target, listener);
            } else {
                final TestRequest subRequest = new TestRequest(request.id, Collections.singleton(target));
                logger.info("reroute sub request {} to node with the remote cluster client role", subRequest);
                for (DiscoveryNode node : clusterService.state().nodes()) {
                    if (node.isRemoteClusterClient()) {
                        transportService.sendChildRequest(
                            node,
                            actionName,
                            subRequest,
                            parentTask,
                            TransportRequestOptions.EMPTY,
                            new ActionListenerResponseHandler<>(listener, TestResponse::new)
                        );
                        return;
                    }
                }
                listener.onFailure(new IllegalAccessException("can't find node with the remote cluster client role"));
            }
        }
    }

    void dispatchSubRequestToRemoteCluster(
        Task parentTask,
        TestRequest subRequest,
        TestRequest.Target target,
        ActionListener<TestResponse> origListener
    ) {
        transportService.getRemoteClusterService()
            .collectNodes(Set.of(target.clusterAlias), ActionListener.delegateFailure(origListener, (listener, nodes) -> {
                final Transport.Connection connection;
                try {
                    final DiscoveryNode node = nodes.apply(target.clusterAlias, target.nodeId);
                    connection = transportService.getRemoteClusterService().getConnection(node, target.clusterAlias);
                } catch (Exception e) {
                    listener.onFailure(e);
                    return;
                }
                transportService.sendChildRequest(
                    connection,
                    actionName,
                    subRequest,
                    parentTask,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(listener, TestResponse::new)
                );
            }));
    }

    void executeSubRequestLocally(BlockingCancellableTask task, String requestId, ActionListener<TestResponse> listener) {
        logger.info("execute sub request {} on node {}", requestId, transportService.getLocalNode().getName());
        final CountDownLatch latch = latches.computeIfAbsent(requestId, k -> new CountDownLatch(1));
        task.setOnCancel(latch::countDown);
        transportService.getThreadPool().executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            protected void doRun() throws Exception {
                latch.await();
                if (task.isCancelled()) {
                    throw new TaskCancelledException("task was cancelled");
                }
                listener.onResponse(new TestResponse(true));
            }
        });
    }
}
