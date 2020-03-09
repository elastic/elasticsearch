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

package org.elasticsearch.logstash.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportDeletePipelineAction extends HandledTransportAction<DeletePipelineRequest, DeletePipelineResponse> {

    private final Client client;

    @Inject
    public TransportDeletePipelineAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(DeletePipelineAction.NAME, transportService, actionFilters, DeletePipelineRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, DeletePipelineRequest request, ActionListener<DeletePipelineResponse> listener) {
        client.prepareDelete(".logstash", request.id())
            .execute(
                ActionListener.wrap(
                    deleteResponse -> listener.onResponse(new DeletePipelineResponse(deleteResponse.getResult() == Result.DELETED)),
                    listener::onFailure
                )
            );
    }
}
