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

package org.elasticsearch.plugin.ingest.transport.simulate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.ingest.Data;
import org.elasticsearch.plugin.ingest.PipelineExecutionService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class SimulatePipelineTransportAction extends HandledTransportAction<SimulatePipelineRequest, SimulatePipelineResponse> {

    private final PipelineExecutionService executionService;

    @Inject
    public SimulatePipelineTransportAction(Settings settings, ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, PipelineExecutionService executionService) {
        super(settings, SimulatePipelineAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, SimulatePipelineRequest::new);
        this.executionService = executionService;
    }

    @Override
    protected void doExecute(SimulatePipelineRequest request, ActionListener<SimulatePipelineResponse> listener) {
        Map<String, Object> source = XContentHelper.convertToMap(request.source(), false).v2();

        SimulatePipelineRequestPayload payload;
        SimulatePipelineRequestPayload.Factory factory = new SimulatePipelineRequestPayload.Factory();
        try {
            payload = factory.create(request.id(), source, executionService);
        } catch (IOException e) {
            listener.onFailure(e);
            return;
        }

        final AtomicArray<SimulatedItemResponse> responses = new AtomicArray<>(payload.size());
        final AtomicInteger counter = new AtomicInteger(payload.size());

        for (int i = 0; i < payload.size(); i++) {
            final int index = i;

            executionService.execute(payload.getDocument(index), payload.pipeline(), new PipelineExecutionService.Listener() {
                @Override
                public void executed(Data data) {
                    responses.set(index, new SimulatedItemResponse(data));

                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void failed(Exception e) {
                    logger.error("failed to execute pipeline [{}]", e, payload.pipelineId());
                    responses.set(index, new SimulatedItemResponse(e));

                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                public void finishHim() {
                    SimulatedItemResponse[] responseArray = new SimulatedItemResponse[responses.length()];
                    responses.toArray(responseArray);

                    SimulatePipelineResponse response = new SimulatePipelineResponse()
                            .pipelineId(payload.pipelineId())
                            .responses(responseArray);

                    listener.onResponse(response);
                }
            });
        }
    }
}
