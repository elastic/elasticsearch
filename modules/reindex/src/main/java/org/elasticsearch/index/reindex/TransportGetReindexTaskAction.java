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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportGetReindexTaskAction extends HandledTransportAction<GetReindexTaskAction.Request, GetReindexTaskAction.Response> {

    private final ReindexIndexClient reindexIndexClient;

    @Inject
    public TransportGetReindexTaskAction(Client client, TransportService transportService, ActionFilters actionFilters,
                                            ClusterService clusterService, NamedXContentRegistry xContentRegistry) {
        super(GetReindexTaskAction.NAME, transportService, actionFilters, GetReindexTaskAction.Request::new);
        this.reindexIndexClient = new ReindexIndexClient(client, clusterService, xContentRegistry);
    }

    @Override
    protected void doExecute(Task task, GetReindexTaskAction.Request request, ActionListener<GetReindexTaskAction.Response> listener) {
        String id = request.getId();
        reindexIndexClient.getReindexTaskDoc(id, ActionListener.map(listener,
            (state) -> TransportGetReindexTaskAction.toResponse(id, state)));
    }

    private static GetReindexTaskAction.Response toResponse(String id, ReindexTaskState taskState) {
        ReindexTaskStateDoc stateDoc = taskState.getStateDoc();
        return new GetReindexTaskAction.Response(id, stateDoc.getStartTimeMillis(), stateDoc.getEndTimeMillis(),
            stateDoc.getReindexResponse(), stateDoc.getException());
    }
}
