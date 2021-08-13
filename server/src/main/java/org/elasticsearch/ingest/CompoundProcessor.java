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

    CompoundProcessor(LongSupplier relativeTimeProvider, Processor... processor) {
        this(false, Arrays.asList(processor), Collections.emptyList(), relativeTimeProvider);
    }

    public CompoundProcessor(Processor... processor) {
        this(false, Arrays.asList(processor), Collections.emptyList());
    }

    public CompoundProcessor(boolean ignoreFailure, List<Processor> processors, List<Processor> onFailureProcessors) {
        this(ignoreFailure, processors, onFailureProcessors, System::nanoTime);
    }
    CompoundProcessor(boolean ignoreFailure, List<Processor> processors, List<Processor> onFailureProcessors,
                      LongSupplier relativeTimeProvider) {
        super();
        this.ignoreFailure = ignoreFailure;
        this.processors = processors;
        this.onFailureProcessors = onFailureProcessors;
        this.relativeTimeProvider = relativeTimeProvider;
        this.processorsWithMetrics = new ArrayList<>(processors.size());
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
            if (processor instanceof CompoundProcessor) {
                flattened.addAll(((CompoundProcessor) processor).flattenProcessors());
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
        return "CompoundProcessor-" + flattenProcessors().stream().map(Processor::getTag).collect(Collectors.joining("-"));
    }

    @Override
    public String getDescription() {
        return null;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        throw new UnsupportedOperationException("this method should not get executed");
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

        Tuple<Processor, IngestMetric> processorWithMetric = processorsWithMetrics.get(currentProcessor);
        final Processor processor = processorWithMetric.v1();
        final IngestMetric metric = processorWithMetric.v2();
        final long startTimeInNanos = relativeTimeProvider.getAsLong();
        metric.preIngest();
        processor.execute(ingestDocument, (result, e) -> {
            long ingestTimeInNanos = relativeTimeProvider.getAsLong() - startTimeInNanos;
            metric.postIngest(ingestTimeInNanos);

            if (e != null) {
                metric.ingestFailed();
                if (ignoreFailure) {
                    innerExecute(currentProcessor + 1, ingestDocument, handler);
                } else {
                    IngestProcessorException compoundProcessorException =
                        newCompoundProcessorException(e, processor, ingestDocument);
                    if (onFailureProcessors.isEmpty()) {
                        handler.accept(null, compoundProcessorException);
                    } else {
                        executeOnFailureAsync(0, ingestDocument, compoundProcessorException, handler);
                    }
                }
            } else {
                if (result != null) {
                    innerExecute(currentProcessor + 1, result, handler);
                } else {
                    handler.accept(null, null);
                }
            }
        });
    }

    void executeOnFailureAsync(int currentOnFailureProcessor, IngestDocument ingestDocument, ElasticsearchException exception,
                               BiConsumer<IngestDocument, Exception> handler) {
        if (currentOnFailureProcessor == 0) {
            putFailureMetadata(ingestDocument, exception);
        }

        if (currentOnFailureProcessor == onFailureProcessors.size()) {
            removeFailureMetadata(ingestDocument);
            handler.accept(ingestDocument, null);
            return;
        }

        final Processor onFailureProcessor = onFailureProcessors.get(currentOnFailureProcessor);
        onFailureProcessor.execute(ingestDocument, (result, e) -> {
            if (e != null) {
                removeFailureMetadata(ingestDocument);
                handler.accept(null, newCompoundProcessorException(e, onFailureProcessor, ingestDocument));
                return;
            }
            if (result == null) {
                removeFailureMetadata(ingestDocument);
                handler.accept(null, null);
                return;
            }
            executeOnFailureAsync(currentOnFailureProcessor + 1, ingestDocument, exception, handler);
        });
    }

    private void putFailureMetadata(IngestDocument ingestDocument, ElasticsearchException cause) {
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

    private void removeFailureMetadata(IngestDocument ingestDocument) {
        Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
        ingestMetadata.remove(ON_FAILURE_MESSAGE_FIELD);
        ingestMetadata.remove(ON_FAILURE_PROCESSOR_TYPE_FIELD);
        ingestMetadata.remove(ON_FAILURE_PROCESSOR_TAG_FIELD);
        ingestMetadata.remove(ON_FAILURE_PIPELINE_FIELD);
    }

    static IngestProcessorException newCompoundProcessorException(Exception e, Processor processor, IngestDocument document) {
        if (e instanceof IngestProcessorException && ((IngestProcessorException) e).getHeader("processor_type") != null) {
            return (IngestProcessorException) e;
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
        List<String> pipelineStack = document.getPipelineStack();
        if (pipelineStack.size() > 1) {
            exception.addHeader("pipeline_origin", pipelineStack);
        }

        return exception;
    }

}
