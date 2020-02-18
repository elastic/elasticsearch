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

package org.elasticsearch.index.reindex;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;

// todo: change to empty response since tasks API is low-level
public class TransportRethrottlePersistentReindexAction extends
    HandledTransportAction<RethrottlePersistentReindexAction.Request, ListTasksResponse> {

    private final ReindexIndexClient reindexIndexClient;
    private final TransportService transportService;

    @Inject
    public TransportRethrottlePersistentReindexAction(TransportService transportService, ActionFilters actionFilters, Client client,
                                                      ClusterService clusterService, NamedXContentRegistry xContentRegistry) {
        super(RethrottlePersistentReindexAction.NAME, transportService, actionFilters, RethrottlePersistentReindexAction.Request::new);
        this.transportService = transportService;
        this.reindexIndexClient = new ReindexIndexClient(client, clusterService, xContentRegistry);
    }

    @Override
    protected void doExecute(Task task, RethrottlePersistentReindexAction.Request request,
                             ActionListener<ListTasksResponse> listener) {
        tryUpdateStateDoc(request, 3, task, listener);
    }

    private void tryUpdateStateDoc(RethrottlePersistentReindexAction.Request request,
                                   int retries, Task rethrottleTask, ActionListener<ListTasksResponse> listener) {
        reindexIndexClient.getReindexTaskDoc(request.getPersistentTaskId(), ActionListener.delegateFailure(listener,
            (delegate, state) -> {
                ReindexTaskStateDoc newStateDoc = state.getStateDoc().withRequestsPerSecond(request.getRequestsPerSecond());
                reindexIndexClient.updateReindexTaskDoc(request.getPersistentTaskId(), newStateDoc,
                    state.getPrimaryTerm(), state.getSeqNo(), new ActionListener<>() {
                        @Override
                        public void onResponse(ReindexTaskState reindexTaskState) {
                            rethrottleAllocatedTask(reindexTaskState.getStateDoc(), request,
                                rethrottleTask, delegate);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (e instanceof VersionConflictEngineException && retries > 0) {
                                tryUpdateStateDoc(request, retries - 1, rethrottleTask, delegate);
                            } else {
                                listener.onFailure(e);
                            }
                        }
                    });
            }));
    }

    private void rethrottleAllocatedTask(ReindexTaskStateDoc stateDoc, RethrottlePersistentReindexAction.Request request,
                                         Task parentTask, ActionListener<ListTasksResponse> listener) {
        TaskId ephemeralTaskId = stateDoc.getEphemeralTaskId();
        if (ephemeralTaskId == null) {
            // not started, this should be successful (though getting here is difficult).
            listener.onResponse(new ListTasksResponse(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
        } else if (stateDoc.getReindexResponse() != null || stateDoc.getException() != null) {
            // completed, fail
            listener.onFailure(new ResourceNotFoundException("Task {} already completed", request.getPersistentTaskId()));
        } else {
            transportService.sendChildRequest(transportService.getLocalNode(), RethrottleAction.NAME,
                new RethrottleRequest().setRequestsPerSecond(request.getRequestsPerSecond()).setTaskId(ephemeralTaskId), parentTask,
                TransportRequestOptions.EMPTY, new ActionListenerResponseHandler<>(listener, ListTasksResponse::new));
        }
    }
}
