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

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.ingest.CompoundProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.ingest.TrackingResultProcessor.decorate;

class SimulateExecutionService {

    private static final String THREAD_POOL_NAME = ThreadPool.Names.MANAGEMENT;

    private final ThreadPool threadPool;

    SimulateExecutionService(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    SimulateDocumentResult executeDocument(Pipeline pipeline, IngestDocument ingestDocument, boolean verbose) {
        if (verbose) {
            List<SimulateProcessorResult> processorResultList = new ArrayList<>();
            CompoundProcessor verbosePipelineProcessor = decorate(pipeline.getCompoundProcessor(), processorResultList);
            try {
                Pipeline verbosePipeline = new Pipeline(pipeline.getId(), pipeline.getDescription(), pipeline.getVersion(),
                    verbosePipelineProcessor);
                ingestDocument.executePipeline(verbosePipeline);
                return new SimulateDocumentVerboseResult(processorResultList);
            } catch (Exception e) {
                return new SimulateDocumentVerboseResult(processorResultList);
            }
        } else {
            try {
                IngestDocument result = pipeline.execute(ingestDocument);
                return new SimulateDocumentBaseResult(result);
            } catch (Exception e) {
                return new SimulateDocumentBaseResult(e);
            }
        }
    }

    public void execute(SimulatePipelineRequest.Parsed request, ActionListener<SimulatePipelineResponse> listener) {
        threadPool.executor(THREAD_POOL_NAME).execute(ActionRunnable.wrap(listener, l -> {
                List<SimulateDocumentResult> responses = new ArrayList<>();
                for (IngestDocument ingestDocument : request.getDocuments()) {
                    SimulateDocumentResult response = executeDocument(request.getPipeline(), ingestDocument, request.isVerbose());
                    if (response != null) {
                        responses.add(response);
                    }
                }
                l.onResponse(new SimulatePipelineResponse(request.getPipeline().getId(), request.isVerbose(), responses));
        }));
    }
}
