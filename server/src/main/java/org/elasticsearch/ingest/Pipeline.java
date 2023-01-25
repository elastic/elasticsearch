/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.script.ScriptService;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * A pipeline is a list of {@link Processor} instances grouped under a unique id.
 */
public final class Pipeline {

    public static final String DESCRIPTION_KEY = "description";
    public static final String PROCESSORS_KEY = "processors";
    public static final String VERSION_KEY = "version";
    public static final String ON_FAILURE_KEY = "on_failure";
    public static final String META_KEY = "_meta";

    private final String id;
    @Nullable
    private final String description;
    @Nullable
    private final Integer version;
    @Nullable
    private final Map<String, Object> metadata;
    private final CompoundProcessor compoundProcessor;
    private final IngestMetric metrics;
    private final LongSupplier relativeTimeProvider;

    public Pipeline(
        String id,
        @Nullable String description,
        @Nullable Integer version,
        @Nullable Map<String, Object> metadata,
        CompoundProcessor compoundProcessor
    ) {
        this(id, description, version, metadata, compoundProcessor, System::nanoTime);
    }

    // package private for testing
    Pipeline(
        String id,
        @Nullable String description,
        @Nullable Integer version,
        @Nullable Map<String, Object> metadata,
        CompoundProcessor compoundProcessor,
        LongSupplier relativeTimeProvider
    ) {
        this.id = id;
        this.description = description;
        this.metadata = metadata;
        this.compoundProcessor = compoundProcessor;
        this.version = version;
        this.metrics = new IngestMetric();
        this.relativeTimeProvider = relativeTimeProvider;
    }

    public static Pipeline create(
        String id,
        Map<String, Object> config,
        Map<String, Processor.Factory> processorFactories,
        ScriptService scriptService
    ) throws Exception {
        String description = ConfigurationUtils.readOptionalStringProperty(null, null, config, DESCRIPTION_KEY);
        Integer version = ConfigurationUtils.readIntProperty(null, null, config, VERSION_KEY, null);
        Map<String, Object> metadata = ConfigurationUtils.readOptionalMap(null, null, config, META_KEY);
        List<Map<String, Object>> processorConfigs = ConfigurationUtils.readList(null, null, config, PROCESSORS_KEY);
        List<Processor> processors = ConfigurationUtils.readProcessorConfigs(processorConfigs, scriptService, processorFactories);
        List<Map<String, Object>> onFailureProcessorConfigs = ConfigurationUtils.readOptionalList(null, null, config, ON_FAILURE_KEY);
        List<Processor> onFailureProcessors = ConfigurationUtils.readProcessorConfigs(
            onFailureProcessorConfigs,
            scriptService,
            processorFactories
        );
        if (config.isEmpty() == false) {
            throw new ElasticsearchParseException(
                "pipeline ["
                    + id
                    + "] doesn't support one or more provided configuration parameters "
                    + Arrays.toString(config.keySet().toArray())
            );
        }
        if (onFailureProcessorConfigs != null && onFailureProcessors.isEmpty()) {
            throw new ElasticsearchParseException("pipeline [" + id + "] cannot have an empty on_failure option defined");
        }
        CompoundProcessor compoundProcessor = new CompoundProcessor(false, processors, onFailureProcessors);
        return new Pipeline(id, description, version, metadata, compoundProcessor);
    }

    /**
     * Modifies the data of a document to be indexed based on the processor this pipeline holds
     *
     * If <code>null</code> is returned then this document will be dropped and not indexed, otherwise
     * this document will be kept and indexed.
     */
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        final long startTimeInNanos = relativeTimeProvider.getAsLong();
        metrics.preIngest();
        compoundProcessor.execute(ingestDocument, (result, e) -> {
            long ingestTimeInNanos = relativeTimeProvider.getAsLong() - startTimeInNanos;
            metrics.postIngest(ingestTimeInNanos);
            if (e != null) {
                metrics.ingestFailed();
            }
            handler.accept(result, e);
        });
    }

    /**
     * The unique id of this pipeline
     */
    public String getId() {
        return id;
    }

    /**
     * An optional description of what this pipeline is doing to the data gets processed by this pipeline.
     */
    @Nullable
    public String getDescription() {
        return description;
    }

    /**
     * An optional version stored with the pipeline so that it can be used to determine if the pipeline should be updated / replaced.
     *
     * @return {@code null} if not supplied.
     */
    @Nullable
    public Integer getVersion() {
        return version;
    }

    @Nullable
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    /**
     * Get the underlying {@link CompoundProcessor} containing the Pipeline's processors
     */
    public CompoundProcessor getCompoundProcessor() {
        return compoundProcessor;
    }

    /**
     * Unmodifiable list containing each processor that operates on the data.
     */
    public List<Processor> getProcessors() {
        return compoundProcessor.getProcessors();
    }

    /**
     * Unmodifiable list containing each on_failure processor that operates on the data in case of
     * exception thrown in pipeline processors
     */
    public List<Processor> getOnFailureProcessors() {
        return compoundProcessor.getOnFailureProcessors();
    }

    /**
     * Flattens the normal and on failure processors into a single list. The original order is lost.
     * This can be useful for pipeline validation purposes.
     */
    public List<Processor> flattenAllProcessors() {
        return compoundProcessor.flattenProcessors();
    }

    /**
     * The metrics associated with this pipeline.
     */
    public IngestMetric getMetrics() {
        return metrics;
    }
}
