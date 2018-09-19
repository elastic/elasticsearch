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

package org.elasticsearch.ingest;

import org.elasticsearch.action.ingest.SimulateProcessorResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Processor to be used within Simulate API to keep track of processors executed in pipeline.
 */
public final class TrackingResultProcessor implements Processor {

    private final Processor actualProcessor;
    private final List<SimulateProcessorResult> processorResultList;
    private final boolean ignoreFailure;

    TrackingResultProcessor(boolean ignoreFailure, Processor actualProcessor, List<SimulateProcessorResult> processorResultList) {
        this.ignoreFailure = ignoreFailure;
        this.processorResultList = processorResultList;
        this.actualProcessor = actualProcessor;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        try {
            actualProcessor.execute(ingestDocument);
            processorResultList.add(new SimulateProcessorResult(actualProcessor.getTag(), new IngestDocument(ingestDocument)));
        } catch (Exception e) {
            if (ignoreFailure) {
                processorResultList.add(new SimulateProcessorResult(actualProcessor.getTag(), new IngestDocument(ingestDocument), e));
            } else {
                processorResultList.add(new SimulateProcessorResult(actualProcessor.getTag(), e));
            }
            throw e;
        }
        return ingestDocument;
    }

    @Override
    public String getType() {
        return actualProcessor.getType();
    }

    @Override
    public String getTag() {
        return actualProcessor.getTag();
    }

    public static CompoundProcessor decorate(CompoundProcessor compoundProcessor, List<SimulateProcessorResult> processorResultList,
                                             Set<PipelineProcessor> pipelinesSeen) {
        List<Processor> processors = new ArrayList<>(compoundProcessor.getProcessors().size());
        for (Processor processor : compoundProcessor.getProcessors()) {
            if (processor instanceof PipelineProcessor) {
                PipelineProcessor pipelineProcessor = ((PipelineProcessor) processor);
                if (pipelinesSeen.add(pipelineProcessor) == false) {
                    throw new IllegalStateException("Cycle detected for pipeline: " + pipelineProcessor.getPipeline().getId());
                }
                processors.add(decorate(pipelineProcessor.getPipeline().getCompoundProcessor(), processorResultList, pipelinesSeen));
                pipelinesSeen.remove(pipelineProcessor);
            } else if (processor instanceof CompoundProcessor) {
                processors.add(decorate((CompoundProcessor) processor, processorResultList, pipelinesSeen));
            } else {
                processors.add(new TrackingResultProcessor(compoundProcessor.isIgnoreFailure(), processor, processorResultList));
            }
        }
        List<Processor> onFailureProcessors = new ArrayList<>(compoundProcessor.getProcessors().size());
        for (Processor processor : compoundProcessor.getOnFailureProcessors()) {
            if (processor instanceof PipelineProcessor) {
                PipelineProcessor pipelineProcessor = ((PipelineProcessor) processor);
                if (pipelinesSeen.add(pipelineProcessor) == false) {
                    throw new IllegalStateException("Cycle detected for pipeline: " + pipelineProcessor.getPipeline().getId());
                }
                onFailureProcessors.add(decorate(pipelineProcessor.getPipeline().getCompoundProcessor(), processorResultList,
                    pipelinesSeen));
                pipelinesSeen.remove(pipelineProcessor);
            } else if (processor instanceof CompoundProcessor) {
                onFailureProcessors.add(decorate((CompoundProcessor) processor, processorResultList, pipelinesSeen));
            } else {
                onFailureProcessors.add(new TrackingResultProcessor(compoundProcessor.isIgnoreFailure(), processor, processorResultList));
            }
        }
        return new CompoundProcessor(compoundProcessor.isIgnoreFailure(), processors, onFailureProcessors);
    }
}

