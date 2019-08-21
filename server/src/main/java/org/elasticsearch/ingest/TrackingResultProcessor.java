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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ingest.SimulateProcessorResult;

import java.util.ArrayList;
import java.util.List;

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
        Processor processor = actualProcessor;
        try {
            if (processor instanceof ConditionalProcessor) {
                ConditionalProcessor conditionalProcessor = (ConditionalProcessor) processor;
                if (conditionalProcessor.evaluate(ingestDocument) == false) {
                    return ingestDocument;
                }
                if (conditionalProcessor.getInnerProcessor() instanceof PipelineProcessor) {
                    processor = conditionalProcessor.getInnerProcessor();
                }
            }
            if (processor instanceof PipelineProcessor) {
                PipelineProcessor pipelineProcessor = ((PipelineProcessor) processor);
                Pipeline pipeline = pipelineProcessor.getPipeline();
                //runtime check for cycles against a copy of the document. This is needed to properly handle conditionals around pipelines
                try {
                    IngestDocument ingestDocumentCopy = new IngestDocument(ingestDocument);
                    ingestDocumentCopy.executePipeline(pipelineProcessor.getPipeline());
                } catch (ElasticsearchException elasticsearchException) {
                    if (elasticsearchException.getCause().getCause() instanceof IllegalStateException) {
                        throw elasticsearchException;
                    }
                    //else do nothing, let the tracking processors throw the exception while recording the path up to the failure
                } catch (Exception e) {
                    // do nothing, let the tracking processors throw the exception while recording the path up to the failure
                }
                //now that we know that there are no cycles between pipelines, decorate the processors for this pipeline and execute it
                CompoundProcessor verbosePipelineProcessor = decorate(pipeline.getCompoundProcessor(), processorResultList);
                Pipeline verbosePipeline = new Pipeline(pipeline.getId(), pipeline.getDescription(), pipeline.getVersion(),
                    verbosePipelineProcessor);
                ingestDocument.executePipeline(verbosePipeline);
            } else {
                IngestDocument result = processor.execute(ingestDocument);
                if (result != null) {
                    processorResultList.add(new SimulateProcessorResult(processor.getTag(), new IngestDocument(ingestDocument)));
                } else {
                    processorResultList.add(new SimulateProcessorResult(processor.getTag()));
                    return null;
                }
            }
        } catch (Exception e) {
            if (ignoreFailure) {
                processorResultList.add(new SimulateProcessorResult(processor.getTag(), new IngestDocument(ingestDocument), e));
            } else {
                processorResultList.add(new SimulateProcessorResult(processor.getTag(), e));
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

    public static CompoundProcessor decorate(CompoundProcessor compoundProcessor, List<SimulateProcessorResult> processorResultList) {
        List<Processor> processors = new ArrayList<>(compoundProcessor.getProcessors().size());
        for (Processor processor : compoundProcessor.getProcessors()) {
            if (processor instanceof CompoundProcessor) {
                processors.add(decorate((CompoundProcessor) processor, processorResultList));
            } else {
                processors.add(new TrackingResultProcessor(compoundProcessor.isIgnoreFailure(), processor, processorResultList));
            }
        }
        List<Processor> onFailureProcessors = new ArrayList<>(compoundProcessor.getProcessors().size());
        for (Processor processor : compoundProcessor.getOnFailureProcessors()) {
            if (processor instanceof CompoundProcessor) {
                onFailureProcessors.add(decorate((CompoundProcessor) processor, processorResultList));
            } else {
                onFailureProcessors.add(new TrackingResultProcessor(compoundProcessor.isIgnoreFailure(), processor, processorResultList));
            }
        }
        return new CompoundProcessor(compoundProcessor.isIgnoreFailure(), processors, onFailureProcessors);
    }
}

