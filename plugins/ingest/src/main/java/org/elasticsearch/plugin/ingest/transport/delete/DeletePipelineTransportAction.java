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

package org.elasticsearch.plugin.ingest.transport.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugin.ingest.PipelineStore;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeletePipelineTransportAction extends HandledTransportAction<DeletePipelineRequest, DeletePipelineResponse> {

    private final TransportDeleteAction deleteAction;

    @Inject
    public DeletePipelineTransportAction(Settings settings, ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, TransportDeleteAction deleteAction) {
        super(settings, DeletePipelineAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, DeletePipelineRequest::new);
        this.deleteAction = deleteAction;
    }

    @Override
    protected void doExecute(DeletePipelineRequest request, ActionListener<DeletePipelineResponse> listener) {
        DeleteRequest deleteRequest = new DeleteRequest();
        deleteRequest.index(PipelineStore.INDEX);
        deleteRequest.type(PipelineStore.TYPE);
        deleteRequest.id(request.id());
        deleteRequest.refresh(true);
        deleteAction.execute(deleteRequest, new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                listener.onResponse(new DeletePipelineResponse(deleteResponse.getId(), deleteResponse.isFound()));
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }
}
