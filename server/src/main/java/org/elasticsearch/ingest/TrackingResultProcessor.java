/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ingest.SimulateProcessorResult;
import org.elasticsearch.core.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import static org.elasticsearch.ingest.IngestDocument.PIPELINE_CYCLE_ERROR_MESSAGE;

/**
 * Processor to be used within Simulate API to keep track of processors executed in pipeline.
 */
public final class TrackingResultProcessor implements Processor {

    private final Processor actualProcessor;
    private final ConditionalProcessor conditionalProcessor;
    private final List<SimulateProcessorResult> processorResultList;
    private final boolean ignoreFailure;

    TrackingResultProcessor(boolean ignoreFailure, Processor actualProcessor, ConditionalProcessor conditionalProcessor,
                            List<SimulateProcessorResult> processorResultList) {
        this.ignoreFailure = ignoreFailure;
        this.processorResultList = processorResultList;
        this.actualProcessor = actualProcessor;
        this.conditionalProcessor = conditionalProcessor;
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        Tuple<String, Boolean> conditionalWithResult;
        if (conditionalProcessor != null) {
            if (conditionalProcessor.evaluate(ingestDocument) == false) {
                conditionalWithResult = new Tuple<>(conditionalProcessor.getCondition(), Boolean.FALSE);
                processorResultList.add(new SimulateProcessorResult(actualProcessor.getType(), actualProcessor.getTag(),
                    actualProcessor.getDescription(), conditionalWithResult));
                handler.accept(ingestDocument, null);
                return;
            } else {
                conditionalWithResult = new Tuple<>(conditionalProcessor.getCondition(), Boolean.TRUE);
            }
        } else {
            conditionalWithResult = null; //no condition
        }

        if (actualProcessor instanceof PipelineProcessor) {
            PipelineProcessor pipelineProcessor = ((PipelineProcessor) actualProcessor);
            Pipeline pipeline = pipelineProcessor.getPipeline(ingestDocument);
            //runtime check for cycles against a copy of the document. This is needed to properly handle conditionals around pipelines
            IngestDocument ingestDocumentCopy = new IngestDocument(ingestDocument);
            Pipeline pipelineToCall = pipelineProcessor.getPipeline(ingestDocument);
            if (pipelineToCall == null) {
                throw new IllegalArgumentException("Pipeline processor configured for non-existent pipeline [" +
                    pipelineProcessor.getPipelineToCallName(ingestDocument) + ']');
            }
            ingestDocumentCopy.executePipeline(pipelineToCall, (result, e) -> {
                // special handling for pipeline cycle errors
                if (e instanceof ElasticsearchException &&
                    e.getCause() instanceof IllegalStateException &&
                    e.getCause().getMessage().startsWith(PIPELINE_CYCLE_ERROR_MESSAGE)) {
                    if (ignoreFailure) {
                        processorResultList.add(new SimulateProcessorResult(pipelineProcessor.getType(), pipelineProcessor.getTag(),
                            pipelineProcessor.getDescription(), new IngestDocument(ingestDocument), e, conditionalWithResult));
                    } else {
                        processorResultList.add(new SimulateProcessorResult(pipelineProcessor.getType(), pipelineProcessor.getTag(),
                            pipelineProcessor.getDescription(), e, conditionalWithResult));
                    }
                    handler.accept(null, e);
                } else {
                    //now that we know that there are no cycles between pipelines, decorate the processors for this pipeline and execute it
                    CompoundProcessor verbosePipelineProcessor = decorate(pipeline.getCompoundProcessor(), null, processorResultList);
                    //add the pipeline process to the results
                    processorResultList.add(new SimulateProcessorResult(actualProcessor.getType(), actualProcessor.getTag(),
                        actualProcessor.getDescription(), conditionalWithResult));
                    Pipeline verbosePipeline = new Pipeline(pipeline.getId(), pipeline.getDescription(), pipeline.getVersion(),
                        verbosePipelineProcessor);
                    ingestDocument.executePipeline(verbosePipeline, handler);
                }
            });
            return;
        }

        actualProcessor.execute(ingestDocument, (result, e) -> {
            if (e != null) {
                if (ignoreFailure) {
                    processorResultList.add(new SimulateProcessorResult(actualProcessor.getType(), actualProcessor.getTag(),
                        actualProcessor.getDescription(), new IngestDocument(ingestDocument), e, conditionalWithResult));
                } else {
                    processorResultList.add(new SimulateProcessorResult(actualProcessor.getType(), actualProcessor.getTag(),
                        actualProcessor.getDescription(), e, conditionalWithResult));
                }
                handler.accept(null, e);
            } else {
                if (result != null) {
                    processorResultList.add(new SimulateProcessorResult(actualProcessor.getType(), actualProcessor.getTag(),
                        actualProcessor.getDescription(), new IngestDocument(ingestDocument), conditionalWithResult));
                    handler.accept(result, null);
                } else {
                    processorResultList.add(new SimulateProcessorResult(actualProcessor.getType(), actualProcessor.getTag(),
                        actualProcessor.getDescription(), conditionalWithResult));
                    handler.accept(null, null);
                }
            }
        });
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getType() {
        return actualProcessor.getType();
    }

    @Override
    public String getTag() {
        return actualProcessor.getTag();
    }

    @Override
    public String getDescription() {
        return actualProcessor.getDescription();
    }

    public static CompoundProcessor decorate(CompoundProcessor compoundProcessor, ConditionalProcessor parentCondition,
                                             List<SimulateProcessorResult> processorResultList) {
        List<Processor> processors = new ArrayList<>();
        for (Processor processor : compoundProcessor.getProcessors()) {
            ConditionalProcessor conditionalProcessor = parentCondition;
            if (processor instanceof ConditionalProcessor) {
                conditionalProcessor = (ConditionalProcessor) processor;
                processor = conditionalProcessor.getInnerProcessor();
            }
            if (processor instanceof CompoundProcessor) {
                processors.add(decorate((CompoundProcessor) processor, conditionalProcessor, processorResultList));
            } else {
                processors.add(
                    new TrackingResultProcessor(compoundProcessor.isIgnoreFailure(), processor, conditionalProcessor, processorResultList));
            }
        }
        List<Processor> onFailureProcessors = new ArrayList<>(compoundProcessor.getProcessors().size());
        for (Processor processor : compoundProcessor.getOnFailureProcessors()) {
            ConditionalProcessor conditionalProcessor = null;
            if (processor instanceof ConditionalProcessor) {
                conditionalProcessor = (ConditionalProcessor) processor;
                processor = conditionalProcessor.getInnerProcessor();
            }
            if (processor instanceof CompoundProcessor) {
                onFailureProcessors.add(decorate((CompoundProcessor) processor, conditionalProcessor, processorResultList));
            } else {
                onFailureProcessors.add(
                    new TrackingResultProcessor(compoundProcessor.isIgnoreFailure(), processor, conditionalProcessor, processorResultList));
            }
        }
        return new CompoundProcessor(compoundProcessor.isIgnoreFailure(), processors, onFailureProcessors);
    }
}

