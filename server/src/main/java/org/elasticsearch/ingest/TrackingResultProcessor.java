/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.action.ingest.SimulateProcessorResult;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ElasticsearchException;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Processor to be used within Simulate API to keep track of processors executed in pipeline.
 */
public final class TrackingResultProcessor implements Processor {

    private final Processor actualProcessor;
    private final ConditionalProcessor conditionalProcessor;
    private final List<SimulateProcessorResult> processorResultList;
    private final boolean ignoreFailure;
    // This field indicates that this processor needs to run the initial check for cycles in pipeline processors:
    private final boolean performCycleCheck;

    TrackingResultProcessor(
        boolean ignoreFailure,
        Processor actualProcessor,
        ConditionalProcessor conditionalProcessor,
        List<SimulateProcessorResult> processorResultList,
        boolean performCycleCheck
    ) {
        this.ignoreFailure = ignoreFailure;
        this.processorResultList = processorResultList;
        this.actualProcessor = actualProcessor;
        this.conditionalProcessor = conditionalProcessor;
        this.performCycleCheck = performCycleCheck;
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        Tuple<String, Boolean> conditionalWithResult;
        if (conditionalProcessor != null) {
            if (conditionalProcessor.evaluate(ingestDocument) == false) {
                conditionalWithResult = new Tuple<>(conditionalProcessor.getCondition(), Boolean.FALSE);
                processorResultList.add(
                    new SimulateProcessorResult(
                        actualProcessor.getType(),
                        actualProcessor.getTag(),
                        actualProcessor.getDescription(),
                        conditionalWithResult
                    )
                );
                handler.accept(ingestDocument, null);
                return;
            } else {
                conditionalWithResult = new Tuple<>(conditionalProcessor.getCondition(), Boolean.TRUE);
            }
        } else {
            conditionalWithResult = null; // no condition
        }

        if (actualProcessor instanceof PipelineProcessor pipelineProcessor) {
            Pipeline pipeline = pipelineProcessor.getPipeline(ingestDocument);
            // runtime check for cycles against a copy of the document. This is needed to properly handle conditionals around pipelines
            IngestDocument ingestDocumentCopy = new IngestDocument(ingestDocument);
            Pipeline pipelineToCall = pipelineProcessor.getPipeline(ingestDocument);
            if (pipelineToCall == null) {
                IllegalArgumentException e = new IllegalArgumentException(
                    "Pipeline processor configured for non-existent pipeline ["
                        + pipelineProcessor.getPipelineToCallName(ingestDocument)
                        + ']'
                );
                // Add error as processor result, otherwise this gets lost in SimulateExecutionService#execute(...) and
                // an empty response gets returned by the ingest simulate api.
                processorResultList.add(
                    new SimulateProcessorResult(
                        pipelineProcessor.getType(),
                        pipelineProcessor.getTag(),
                        pipelineProcessor.getDescription(),
                        pipelineProcessor.isIgnoreMissingPipeline() ? ingestDocument : null,
                        e,
                        conditionalWithResult
                    )
                );

                if (pipelineProcessor.isIgnoreMissingPipeline()) {
                    handler.accept(ingestDocument, null);
                    return;
                } else {
                    throw e;
                }
            }
            if (performCycleCheck) {
                ingestDocumentCopy.executePipeline(pipelineToCall, (result, e) -> {
                    // special handling for pipeline cycle errors
                    if (e instanceof ElasticsearchException && e.getCause() instanceof GraphStructureException) {
                        if (ignoreFailure) {
                            processorResultList.add(
                                new SimulateProcessorResult(
                                    pipelineProcessor.getType(),
                                    pipelineProcessor.getTag(),
                                    pipelineProcessor.getDescription(),
                                    ingestDocument,
                                    e,
                                    conditionalWithResult
                                )
                            );
                        } else {
                            processorResultList.add(
                                new SimulateProcessorResult(
                                    pipelineProcessor.getType(),
                                    pipelineProcessor.getTag(),
                                    pipelineProcessor.getDescription(),
                                    e,
                                    conditionalWithResult
                                )
                            );
                        }
                        handler.accept(null, e);
                    } else {
                        // now that we know that there are no cycles between pipelines, decorate the processors for this pipeline and
                        // execute it
                        decorateAndExecutePipeline(pipeline, ingestDocument, conditionalWithResult, handler);
                    }
                });
            } else {
                // The cycle check has been done before, so we can just decorate the pipeline with our instrumentation and execute it:
                decorateAndExecutePipeline(pipeline, ingestDocument, conditionalWithResult, handler);
            }

            return;
        }

        executeProcessor(actualProcessor, ingestDocument, (result, e) -> {
            if (e != null) {
                if (ignoreFailure) {
                    processorResultList.add(
                        new SimulateProcessorResult(
                            actualProcessor.getType(),
                            actualProcessor.getTag(),
                            actualProcessor.getDescription(),
                            ingestDocument,
                            e,
                            conditionalWithResult
                        )
                    );
                } else {
                    processorResultList.add(
                        new SimulateProcessorResult(
                            actualProcessor.getType(),
                            actualProcessor.getTag(),
                            actualProcessor.getDescription(),
                            e,
                            conditionalWithResult
                        )
                    );
                }
                handler.accept(null, e);
            } else {
                if (result != null) {
                    processorResultList.add(
                        new SimulateProcessorResult(
                            actualProcessor.getType(),
                            actualProcessor.getTag(),
                            actualProcessor.getDescription(),
                            ingestDocument,
                            conditionalWithResult
                        )
                    );
                    handler.accept(result, null);
                } else {
                    processorResultList.add(
                        new SimulateProcessorResult(
                            actualProcessor.getType(),
                            actualProcessor.getTag(),
                            actualProcessor.getDescription(),
                            conditionalWithResult
                        )
                    );
                    handler.accept(null, null);
                }
            }
        });
    }

    /*
     * This method decorates the pipeline's compound processor with a new TrackingResultProcessor that does not do cycle checking, and
     * executes the pipeline using that TrackingResultProcessor.
     */
    private void decorateAndExecutePipeline(
        Pipeline pipeline,
        IngestDocument ingestDocument,
        Tuple<String, Boolean> conditionalWithResult,
        BiConsumer<IngestDocument, Exception> handler
    ) {
        CompoundProcessor verbosePipelineProcessor = decorateNoCycleCheck(pipeline.getCompoundProcessor(), null, processorResultList);
        // add the pipeline process to the results
        processorResultList.add(
            new SimulateProcessorResult(
                actualProcessor.getType(),
                actualProcessor.getTag(),
                actualProcessor.getDescription(),
                conditionalWithResult
            )
        );
        Pipeline verbosePipeline = new Pipeline(
            pipeline.getId(),
            pipeline.getDescription(),
            pipeline.getVersion(),
            pipeline.getMetadata(),
            verbosePipelineProcessor,
            pipeline.getDeprecated()
        );
        ingestDocument.executePipeline(verbosePipeline, handler);
    }

    private static void executeProcessor(Processor p, IngestDocument doc, BiConsumer<IngestDocument, Exception> handler) {
        if (p.isAsync()) {
            p.execute(doc, handler);
        } else {
            try {
                IngestDocument result = p.execute(doc);
                handler.accept(result, null);
            } catch (Exception e) {
                handler.accept(null, e);
            }
        }
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

    @Override
    public boolean isAsync() {
        return true;
    }

    /*
     * This decorates the existing processor with a new TrackingResultProcessor that does _not_ do checks for cycles in pipeline
     * processors, with the assumption that that check has already been done.
     */
    private static CompoundProcessor decorateNoCycleCheck(
        CompoundProcessor compoundProcessor,
        ConditionalProcessor parentCondition,
        List<SimulateProcessorResult> processorResultList
    ) {
        return decorate(compoundProcessor, parentCondition, processorResultList, false);
    }

    public static CompoundProcessor decorate(
        CompoundProcessor compoundProcessor,
        ConditionalProcessor parentCondition,
        List<SimulateProcessorResult> processorResultList
    ) {
        return decorate(compoundProcessor, parentCondition, processorResultList, true);
    }

    private static CompoundProcessor decorate(
        CompoundProcessor compoundProcessor,
        ConditionalProcessor parentCondition,
        List<SimulateProcessorResult> processorResultList,
        boolean performCycleCheck
    ) {
        List<Processor> processors = new ArrayList<>();
        for (Processor processor : compoundProcessor.getProcessors()) {
            ConditionalProcessor conditionalProcessor = parentCondition;
            if (processor instanceof ConditionalProcessor cp) {
                conditionalProcessor = cp;
                processor = conditionalProcessor.getInnerProcessor();
            }
            if (processor instanceof CompoundProcessor cp) {
                processors.add(decorate(cp, conditionalProcessor, processorResultList));
            } else {
                processors.add(
                    new TrackingResultProcessor(
                        compoundProcessor.isIgnoreFailure(),
                        processor,
                        conditionalProcessor,
                        processorResultList,
                        performCycleCheck
                    )
                );
            }
        }
        List<Processor> onFailureProcessors = new ArrayList<>(compoundProcessor.getProcessors().size());
        for (Processor processor : compoundProcessor.getOnFailureProcessors()) {
            ConditionalProcessor conditionalProcessor = null;
            if (processor instanceof ConditionalProcessor cp) {
                conditionalProcessor = cp;
                processor = conditionalProcessor.getInnerProcessor();
            }
            if (processor instanceof CompoundProcessor cp) {
                onFailureProcessors.add(decorate(cp, conditionalProcessor, processorResultList));
            } else {
                onFailureProcessors.add(
                    new TrackingResultProcessor(
                        compoundProcessor.isIgnoreFailure(),
                        processor,
                        conditionalProcessor,
                        processorResultList,
                        performCycleCheck
                    )
                );
            }
        }
        return new CompoundProcessor(compoundProcessor.isIgnoreFailure(), processors, onFailureProcessors);
    }
}
