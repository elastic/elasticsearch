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

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.ingest.Data;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;

public class SimulateExecutionService {

    static final String THREAD_POOL_NAME = ThreadPool.Names.MANAGEMENT;

    private final ThreadPool threadPool;

    @Inject
    public SimulateExecutionService(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }


    SimulatedItemResponse executeItem(Pipeline pipeline, Data data) {
        try {
            pipeline.execute(data);
            return new SimulatedItemResponse(data);
        } catch (Exception e) {
            return new SimulatedItemResponse(e);
        }

    }

    SimulatedItemResponse executeVerboseItem(Pipeline pipeline, Data data) {
        List<ProcessorResult> processorResultList = new ArrayList<>();
        Data currentData = new Data(data);
        for (int i = 0; i < pipeline.getProcessors().size(); i++) {
            Processor processor = pipeline.getProcessors().get(i);
            String processorId = "processor[" + processor.getType() + "]-" + i;

            try {
                processor.execute(currentData);
                processorResultList.add(new ProcessorResult(processorId, currentData));
            } catch (Exception e) {
                processorResultList.add(new ProcessorResult(processorId, e));
            }

            currentData = new Data(currentData);
        }
        return new SimulatedItemResponse(processorResultList);
    }

    SimulatePipelineResponse execute(ParsedSimulateRequest request) {
        List<SimulatedItemResponse> responses = new ArrayList<>();
        for (Data data : request.getDocuments()) {
            if (request.isVerbose()) {
                responses.add(executeVerboseItem(request.getPipeline(), data));
            } else {
                responses.add(executeItem(request.getPipeline(), data));
            }
        }
        return new SimulatePipelineResponse(request.getPipeline().getId(), responses);
    }

    public void execute(ParsedSimulateRequest request, Listener listener) {
        threadPool.executor(THREAD_POOL_NAME).execute(new Runnable() {
            @Override
            public void run() {
                SimulatePipelineResponse response = execute(request);
                listener.onResponse(response);
            }
        });
    }

    public interface Listener {
        void onResponse(SimulatePipelineResponse response);
    }
}
