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
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.Pipeline;
import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.ingest.core.CompoundProcessor;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;

class SimulateExecutionService {

    private static final String THREAD_POOL_NAME = ThreadPool.Names.MANAGEMENT;

    private final ThreadPool threadPool;

    SimulateExecutionService(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    void executeVerboseDocument(Processor processor, IngestDocument ingestDocument, List<SimulateProcessorResult> processorResultList) throws Exception {
        if (processor instanceof CompoundProcessor) {
            CompoundProcessor cp = (CompoundProcessor) processor;
            try {
                for (Processor p : cp.getProcessors()) {
                    executeVerboseDocument(p, ingestDocument, processorResultList);
                }
            } catch (Exception e) {
                for (Processor p : cp.getOnFailureProcessors()) {
                    executeVerboseDocument(p, ingestDocument, processorResultList);
                }
            }
        } else {
            try {
                processor.execute(ingestDocument);
                processorResultList.add(new SimulateProcessorResult(processor.getTag(), new IngestDocument(ingestDocument)));
            } catch (Exception e) {
                processorResultList.add(new SimulateProcessorResult(processor.getTag(), e));
                throw e;
            }
        }
    }

    SimulateDocumentResult executeDocument(Pipeline pipeline, IngestDocument ingestDocument, boolean verbose) {
        if (verbose) {
            List<SimulateProcessorResult> processorResultList = new ArrayList<>();
            IngestDocument currentIngestDocument = new IngestDocument(ingestDocument);
            CompoundProcessor pipelineProcessor = new CompoundProcessor(pipeline.getProcessors(), pipeline.getOnFailureProcessors());
            try {
                executeVerboseDocument(pipelineProcessor, currentIngestDocument, processorResultList);
            } catch (Exception e) {
                return new SimulateDocumentSimpleResult(e);
            }
            return new SimulateDocumentVerboseResult(processorResultList);
        } else {
            try {
                pipeline.execute(ingestDocument);
                return new SimulateDocumentSimpleResult(ingestDocument);
            } catch (Exception e) {
                return new SimulateDocumentSimpleResult(e);
            }
        }
    }

    public void execute(SimulatePipelineRequest.Parsed request, ActionListener<SimulatePipelineResponse> listener) {
        threadPool.executor(THREAD_POOL_NAME).execute(new ActionRunnable<SimulatePipelineResponse>(listener) {
            @Override
            protected void doRun() throws Exception {
                List<SimulateDocumentResult> responses = new ArrayList<>();
                for (IngestDocument ingestDocument : request.getDocuments()) {
                    responses.add(executeDocument(request.getPipeline(), ingestDocument, request.isVerbose()));
                }
                listener.onResponse(new SimulatePipelineResponse(request.getPipeline().getId(), request.isVerbose(), responses));
            }
        });
    }
}
