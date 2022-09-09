/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * A Processor that executes a list of other "processors". It executes a separate list of
 * "onFailureProcessors" when any of the processors throw an {@link Exception}.
 */
public class CompoundProcessor implements Processor {
    public static final String ON_FAILURE_MESSAGE_FIELD = "on_failure_message";
    public static final String ON_FAILURE_PROCESSOR_TYPE_FIELD = "on_failure_processor_type";
    public static final String ON_FAILURE_PROCESSOR_TAG_FIELD = "on_failure_processor_tag";
    public static final String ON_FAILURE_PIPELINE_FIELD = "on_failure_pipeline";

    private final boolean ignoreFailure;
    private final List<Processor> processors;
    private final List<Processor> onFailureProcessors;
    private final List<Tuple<Processor, IngestMetric>> processorsWithMetrics;
    private final LongSupplier relativeTimeProvider;
    private final boolean isAsync;

    CompoundProcessor(LongSupplier relativeTimeProvider, boolean ignoreFailure, Processor... processor) {
        this(ignoreFailure, Arrays.asList(processor), Collections.emptyList(), relativeTimeProvider);
    }

    public CompoundProcessor(Processor... processor) {
        this(false, Arrays.asList(processor), Collections.emptyList());
    }

    public CompoundProcessor(boolean ignoreFailure, List<Processor> processors, List<Processor> onFailureProcessors) {
        this(ignoreFailure, processors, onFailureProcessors, System::nanoTime);
    }

    CompoundProcessor(
        boolean ignoreFailure,
        List<Processor> processors,
        List<Processor> onFailureProcessors,
        LongSupplier relativeTimeProvider
    ) {
        super();
        this.ignoreFailure = ignoreFailure;
        this.processors = processors;
        this.onFailureProcessors = onFailureProcessors;
        this.relativeTimeProvider = relativeTimeProvider;
        this.processorsWithMetrics = new ArrayList<>(processors.size());
        this.isAsync = flattenProcessors().stream().anyMatch(Processor::isAsync);
        processors.forEach(p -> processorsWithMetrics.add(new Tuple<>(p, new IngestMetric())));
    }

    List<Tuple<Processor, IngestMetric>> getProcessorsWithMetrics() {
        return processorsWithMetrics;
    }

    public boolean isIgnoreFailure() {
        return ignoreFailure;
    }

    public List<Processor> getOnFailureProcessors() {
        return onFailureProcessors;
    }

    public List<Processor> getProcessors() {
        return processors;
    }

    public List<Processor> flattenProcessors() {
        List<Processor> allProcessors = new ArrayList<>(flattenProcessors(processors));
        allProcessors.addAll(flattenProcessors(onFailureProcessors));
        return allProcessors;
    }

    private static List<Processor> flattenProcessors(List<Processor> processors) {
        List<Processor> flattened = new ArrayList<>();
        for (Processor processor : processors) {
            if (processor instanceof CompoundProcessor compoundProcessor) {
                flattened.addAll(compoundProcessor.flattenProcessors());
            } else {
                flattened.add(processor);
            }
        }
        return flattened;
    }

    @Override
    public String getType() {
        return "compound";
    }

    @Override
    public String getTag() {
        return "CompoundProcessor-"
            + flattenProcessors().stream().map(CompoundProcessor::processorDescription).collect(Collectors.joining("-"));
    }

    private static String processorDescription(Processor p) {
        return p.getTag() != null ? p.getTag() : p.getType();
    }

    @Override
    public String getDescription() {
        return null;
    }

    @Override
    public boolean isAsync() {
        return isAsync;
    }

    @Override
    public IngestDocument execute(IngestDocument document) throws Exception {
        assert isAsync == false; // must not be executed if there are async processors

        IngestDocument[] docHolder = new IngestDocument[1];
        Exception[] exHolder = new Exception[1];
        innerExecute(0, document, (result, e) -> {
            docHolder[0] = result;
            exHolder[0] = e;
        });

        if (exHolder[0] != null) {
            throw exHolder[0];
        }

        return docHolder[0];
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        innerExecute(0, ingestDocument, handler);
    }

    void innerExecute(int currentProcessor, IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        if (currentProcessor == processorsWithMetrics.size()) {
            handler.accept(ingestDocument, null);
            return;
        }

        Tuple<Processor, IngestMetric> processorWithMetric;
        Processor processor;
        IngestMetric metric;
        long startTimeInNanos = 0;
        // iteratively execute any sync processors
        while (currentProcessor < processorsWithMetrics.size() && processorsWithMetrics.get(currentProcessor).v1().isAsync() == false) {
            processorWithMetric = processorsWithMetrics.get(currentProcessor);
            processor = processorWithMetric.v1();
            metric = processorWithMetric.v2();
            startTimeInNanos = relativeTimeProvider.getAsLong();
            metric.preIngest();

            try {
                ingestDocument = processor.execute(ingestDocument);
                long ingestTimeInNanos = relativeTimeProvider.getAsLong() - startTimeInNanos;
                metric.postIngest(ingestTimeInNanos);
                if (ingestDocument == null) {
                    handler.accept(null, null);
                    return;
                }
            } catch (Exception e) {
                metric.postIngest(relativeTimeProvider.getAsLong() - startTimeInNanos);
                executeOnFailureOuter(currentProcessor, ingestDocument, handler, processor, metric, e);
                return;
            }

            currentProcessor++;
        }

        if (currentProcessor >= processorsWithMetrics.size()) {
            handler.accept(ingestDocument, null);
        } else {
            final int finalCurrentProcessor = currentProcessor + 1;
            final int nextProcessor = currentProcessor + 1;
            final long finalStartTimeInNanos = startTimeInNanos;
            final IngestMetric finalMetric = processorsWithMetrics.get(currentProcessor).v2();
            final Processor finalProcessor = processorsWithMetrics.get(currentProcessor).v1();
            final IngestDocument finalIngestDocument = ingestDocument;
            finalMetric.preIngest();
            try {
                finalProcessor.execute(ingestDocument, (result, e) -> {
                    long ingestTimeInNanos = relativeTimeProvider.getAsLong() - finalStartTimeInNanos;
                    finalMetric.postIngest(ingestTimeInNanos);

                    if (e != null) {
                        executeOnFailureOuter(finalCurrentProcessor, finalIngestDocument, handler, finalProcessor, finalMetric, e);
                    } else {
                        if (result != null) {
                            innerExecute(nextProcessor, result, handler);
                        } else {
                            handler.accept(null, null);
                        }
                    }
                });
            } catch (Exception e) {
                long ingestTimeInNanos = relativeTimeProvider.getAsLong() - startTimeInNanos;
                finalMetric.postIngest(ingestTimeInNanos);
                executeOnFailureOuter(currentProcessor, finalIngestDocument, handler, finalProcessor, finalMetric, e);
            }
        }
    }

    private void executeOnFailureOuter(
        int currentProcessor,
        IngestDocument ingestDocument,
        BiConsumer<IngestDocument, Exception> handler,
        Processor processor,
        IngestMetric metric,
        Exception e
    ) {
        metric.ingestFailed();
        if (ignoreFailure) {
            innerExecute(currentProcessor + 1, ingestDocument, handler);
        } else {
            IngestProcessorException compoundProcessorException = newCompoundProcessorException(e, processor, ingestDocument);
            if (onFailureProcessors.isEmpty()) {
                handler.accept(null, compoundProcessorException);
            } else {
                executeOnFailure(0, ingestDocument, compoundProcessorException, handler);
            }
        }
    }

    void executeOnFailure(
        int currentOnFailureProcessor,
        IngestDocument ingestDocument,
        ElasticsearchException exception,
        BiConsumer<IngestDocument, Exception> handler
    ) {
        if (currentOnFailureProcessor == 0) {
            putFailureMetadata(ingestDocument, exception);
        }

        if (currentOnFailureProcessor == onFailureProcessors.size()) {
            removeFailureMetadata(ingestDocument);
            handler.accept(ingestDocument, null);
            return;
        }

        final Processor onFailureProcessor = onFailureProcessors.get(currentOnFailureProcessor);
        if (onFailureProcessor.isAsync()) {
            final IngestDocument finalDoc = ingestDocument;
            onFailureProcessor.execute(finalDoc, (result, e) -> {
                if (e != null) {
                    removeFailureMetadata(finalDoc);
                    handler.accept(null, newCompoundProcessorException(e, onFailureProcessor, finalDoc));
                    return;
                }
                if (result == null) {
                    removeFailureMetadata(finalDoc);
                    handler.accept(null, null);
                    return;
                }
                executeOnFailure(currentOnFailureProcessor + 1, finalDoc, exception, handler);
            });
        } else {
            try {
                ingestDocument = onFailureProcessor.execute(ingestDocument);
                if (ingestDocument == null) {
                    handler.accept(null, null);
                    return;
                }
            } catch (Exception e) {
                if (ingestDocument != null) {
                    removeFailureMetadata(ingestDocument);
                }
                handler.accept(null, newCompoundProcessorException(e, onFailureProcessor, ingestDocument));
                return;
            }
            executeOnFailure(currentOnFailureProcessor + 1, ingestDocument, exception, handler);
        }
    }

    private static void putFailureMetadata(IngestDocument ingestDocument, ElasticsearchException cause) {
        List<String> processorTypeHeader = cause.getHeader("processor_type");
        List<String> processorTagHeader = cause.getHeader("processor_tag");
        List<String> processorOriginHeader = cause.getHeader("pipeline_origin");
        String failedProcessorType = (processorTypeHeader != null) ? processorTypeHeader.get(0) : null;
        String failedProcessorTag = (processorTagHeader != null) ? processorTagHeader.get(0) : null;
        String failedPipelineId = (processorOriginHeader != null) ? processorOriginHeader.get(0) : null;
        Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
        ingestMetadata.put(ON_FAILURE_MESSAGE_FIELD, cause.getRootCause().getMessage());
        ingestMetadata.put(ON_FAILURE_PROCESSOR_TYPE_FIELD, failedProcessorType);
        ingestMetadata.put(ON_FAILURE_PROCESSOR_TAG_FIELD, failedProcessorTag);
        if (failedPipelineId != null) {
            ingestMetadata.put(ON_FAILURE_PIPELINE_FIELD, failedPipelineId);
        }
    }

    private static void removeFailureMetadata(IngestDocument ingestDocument) {
        Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
        ingestMetadata.remove(ON_FAILURE_MESSAGE_FIELD);
        ingestMetadata.remove(ON_FAILURE_PROCESSOR_TYPE_FIELD);
        ingestMetadata.remove(ON_FAILURE_PROCESSOR_TAG_FIELD);
        ingestMetadata.remove(ON_FAILURE_PIPELINE_FIELD);
    }

    static IngestProcessorException newCompoundProcessorException(Exception e, Processor processor, IngestDocument document) {
        if (e instanceof IngestProcessorException ipe && ipe.getHeader("processor_type") != null) {
            return ipe;
        }

        IngestProcessorException exception = new IngestProcessorException(e);

        String processorType = processor.getType();
        if (processorType != null) {
            exception.addHeader("processor_type", processorType);
        }
        String processorTag = processor.getTag();
        if (processorTag != null) {
            exception.addHeader("processor_tag", processorTag);
        }
        if (document != null) {
            List<String> pipelineStack = document.getPipelineStack();
            if (pipelineStack.isEmpty() == false) {
                exception.addHeader("pipeline_origin", pipelineStack);
            }
        }

        return exception;
    }

}
