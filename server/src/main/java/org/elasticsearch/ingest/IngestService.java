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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

/**
 * Holder class for several ingest related services.
 */
public class IngestService implements ClusterStateApplier {

    public static final String NOOP_PIPELINE_NAME = "_none";

    private static final Logger logger = LogManager.getLogger(IngestService.class);

    private final ClusterService clusterService;
    private final ScriptService scriptService;
    private final Map<String, Processor.Factory> processorFactories;
    // Ideally this should be in IngestMetadata class, but we don't have the processor factories around there.
    // We know of all the processor factories when a node with all its plugin have been initialized. Also some
    // processor factories rely on other node services. Custom metadata is statically registered when classes
    // are loaded, so in the cluster state we just save the pipeline config and here we keep the actual pipelines around.
    private volatile Map<String, PipelineHolder> pipelines = Map.of();
    private final ThreadPool threadPool;
    private final IngestMetric totalMetrics = new IngestMetric();
    private final List<Consumer<ClusterState>> ingestClusterStateListeners = new CopyOnWriteArrayList<>();

    public IngestService(ClusterService clusterService, ThreadPool threadPool,
                         Environment env, ScriptService scriptService, AnalysisRegistry analysisRegistry,
                         List<IngestPlugin> ingestPlugins, Client client) {
        this.clusterService = clusterService;
        this.scriptService = scriptService;
        this.processorFactories = processorFactories(
            ingestPlugins,
            new Processor.Parameters(
                env, scriptService, analysisRegistry,
                threadPool.getThreadContext(), threadPool::relativeTimeInMillis,
                (delay, command) -> threadPool.schedule(
                    command, TimeValue.timeValueMillis(delay), ThreadPool.Names.GENERIC
                ), this, client, threadPool.generic()::execute
            )
        );

        this.threadPool = threadPool;
    }

    private static Map<String, Processor.Factory> processorFactories(List<IngestPlugin> ingestPlugins,
        Processor.Parameters parameters) {
        Map<String, Processor.Factory> processorFactories = new HashMap<>();
        for (IngestPlugin ingestPlugin : ingestPlugins) {
            Map<String, Processor.Factory> newProcessors = ingestPlugin.getProcessors(parameters);
            for (Map.Entry<String, Processor.Factory> entry : newProcessors.entrySet()) {
                if (processorFactories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Ingest processor [" + entry.getKey() + "] is already registered");
                }
            }
        }
        return Collections.unmodifiableMap(processorFactories);
    }

    public ClusterService getClusterService() {
        return clusterService;
    }

    public ScriptService getScriptService() {
        return scriptService;
    }

    /**
     * Deletes the pipeline specified by id in the request.
     */
    public void delete(DeletePipelineRequest request, ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask("delete-pipeline-" + request.getId(),
                new AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

            @Override
            protected AcknowledgedResponse newResponse(boolean acknowledged) {
                return new AcknowledgedResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                return innerDelete(request, currentState);
            }
        });
    }

    static ClusterState innerDelete(DeletePipelineRequest request, ClusterState currentState) {
        IngestMetadata currentIngestMetadata = currentState.metaData().custom(IngestMetadata.TYPE);
        if (currentIngestMetadata == null) {
            return currentState;
        }
        Map<String, PipelineConfiguration> pipelines = currentIngestMetadata.getPipelines();
        Set<String> toRemove = new HashSet<>();
        for (String pipelineKey : pipelines.keySet()) {
            if (Regex.simpleMatch(request.getId(), pipelineKey)) {
                toRemove.add(pipelineKey);
            }
        }
        if (toRemove.isEmpty() && Regex.isMatchAllPattern(request.getId()) == false) {
            throw new ResourceNotFoundException("pipeline [{}] is missing", request.getId());
        } else if (toRemove.isEmpty()) {
            return currentState;
        }
        final Map<String, PipelineConfiguration> pipelinesCopy = new HashMap<>(pipelines);
        for (String key : toRemove) {
            pipelinesCopy.remove(key);
        }
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metaData(MetaData.builder(currentState.getMetaData())
                .putCustom(IngestMetadata.TYPE, new IngestMetadata(pipelinesCopy))
                .build());
        return newState.build();
    }

    /**
     * @return pipeline configuration specified by id. If multiple ids or wildcards are specified multiple pipelines
     * may be returned
     */
    // Returning PipelineConfiguration instead of Pipeline, because Pipeline and Processor interface don't
    // know how to serialize themselves.
    public static List<PipelineConfiguration> getPipelines(ClusterState clusterState, String... ids) {
        IngestMetadata ingestMetadata = clusterState.getMetaData().custom(IngestMetadata.TYPE);
        return innerGetPipelines(ingestMetadata, ids);
    }

    static List<PipelineConfiguration> innerGetPipelines(IngestMetadata ingestMetadata, String... ids) {
        if (ingestMetadata == null) {
            return Collections.emptyList();
        }

        // if we didn't ask for _any_ ID, then we get them all (this is the same as if they ask for '*')
        if (ids.length == 0) {
            return new ArrayList<>(ingestMetadata.getPipelines().values());
        }

        List<PipelineConfiguration> result = new ArrayList<>(ids.length);
        for (String id : ids) {
            if (Regex.isSimpleMatchPattern(id)) {
                for (Map.Entry<String, PipelineConfiguration> entry : ingestMetadata.getPipelines().entrySet()) {
                    if (Regex.simpleMatch(id, entry.getKey())) {
                        result.add(entry.getValue());
                    }
                }
            } else {
                PipelineConfiguration pipeline = ingestMetadata.getPipelines().get(id);
                if (pipeline != null) {
                    result.add(pipeline);
                }
            }
        }
        return result;
    }

    /**
     * Stores the specified pipeline definition in the request.
     */
    public void putPipeline(Map<DiscoveryNode, IngestInfo> ingestInfos, PutPipelineRequest request,
        ActionListener<AcknowledgedResponse> listener) throws Exception {
            // validates the pipeline and processor configuration before submitting a cluster update task:
            validatePipeline(ingestInfos, request);
            clusterService.submitStateUpdateTask("put-pipeline-" + request.getId(),
                new AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

                    @Override
                    protected AcknowledgedResponse newResponse(boolean acknowledged) {
                        return new AcknowledgedResponse(acknowledged);
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return innerPut(request, currentState);
                    }
                });
    }

    /**
     * Returns the pipeline by the specified id
     */
    public Pipeline getPipeline(String id) {
        PipelineHolder holder = pipelines.get(id);
        if (holder != null) {
            return holder.pipeline;
        } else {
            return null;
        }
    }

    public Map<String, Processor.Factory> getProcessorFactories() {
        return processorFactories;
    }

    public IngestInfo info() {
        Map<String, Processor.Factory> processorFactories = getProcessorFactories();
        List<ProcessorInfo> processorInfoList = new ArrayList<>(processorFactories.size());
        for (Map.Entry<String, Processor.Factory> entry : processorFactories.entrySet()) {
            processorInfoList.add(new ProcessorInfo(entry.getKey()));
        }
        return new IngestInfo(processorInfoList);
    }

    Map<String, PipelineHolder> pipelines() {
        return pipelines;
    }

    /**
     * Recursive method to obtain all of the non-failure processors for given compoundProcessor. Since conditionals are implemented as
     * wrappers to the actual processor, always prefer the actual processor's metric over the conditional processor's metric.
     * @param compoundProcessor The compound processor to start walking the non-failure processors
     * @param processorMetrics The list of {@link Processor} {@link IngestMetric} tuples.
     * @return the processorMetrics for all non-failure processor that belong to the original compoundProcessor
     */
    private static List<Tuple<Processor, IngestMetric>> getProcessorMetrics(CompoundProcessor compoundProcessor,
                                                                    List<Tuple<Processor, IngestMetric>> processorMetrics) {
        //only surface the top level non-failure processors, on-failure processor times will be included in the top level non-failure
        for (Tuple<Processor, IngestMetric> processorWithMetric : compoundProcessor.getProcessorsWithMetrics()) {
            Processor processor = processorWithMetric.v1();
            IngestMetric metric = processorWithMetric.v2();
            if (processor instanceof CompoundProcessor) {
                getProcessorMetrics((CompoundProcessor) processor, processorMetrics);
            } else {
                //Prefer the conditional's metric since it only includes metrics when the conditional evaluated to true.
                if (processor instanceof ConditionalProcessor) {
                    metric = ((ConditionalProcessor) processor).getMetric();
                }
                processorMetrics.add(new Tuple<>(processor, metric));
            }
        }
        return processorMetrics;
    }

    static ClusterState innerPut(PutPipelineRequest request, ClusterState currentState) {
        IngestMetadata currentIngestMetadata = currentState.metaData().custom(IngestMetadata.TYPE);
        Map<String, PipelineConfiguration> pipelines;
        if (currentIngestMetadata != null) {
            pipelines = new HashMap<>(currentIngestMetadata.getPipelines());
        } else {
            pipelines = new HashMap<>();
        }

        pipelines.put(request.getId(), new PipelineConfiguration(request.getId(), request.getSource(), request.getXContentType()));
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metaData(MetaData.builder(currentState.getMetaData())
            .putCustom(IngestMetadata.TYPE, new IngestMetadata(pipelines))
            .build());
        return newState.build();
    }

    void validatePipeline(Map<DiscoveryNode, IngestInfo> ingestInfos, PutPipelineRequest request) throws Exception {
        if (ingestInfos.isEmpty()) {
            throw new IllegalStateException("Ingest info is empty");
        }

        Map<String, Object> pipelineConfig = XContentHelper.convertToMap(request.getSource(), false, request.getXContentType()).v2();
        Pipeline pipeline = Pipeline.create(request.getId(), pipelineConfig, processorFactories, scriptService);
        List<Exception> exceptions = new ArrayList<>();
        for (Processor processor : pipeline.flattenAllProcessors()) {
            for (Map.Entry<DiscoveryNode, IngestInfo> entry : ingestInfos.entrySet()) {
                String type = processor.getType();
                if (entry.getValue().containsProcessor(type) == false && ConditionalProcessor.TYPE.equals(type) == false) {
                    String message = "Processor type [" + processor.getType() + "] is not installed on node [" + entry.getKey() + "]";
                    exceptions.add(
                        ConfigurationUtils.newConfigurationException(processor.getType(), processor.getTag(), null, message)
                    );
                }
            }
        }
        ExceptionsHelper.rethrowAndSuppress(exceptions);
    }

    public void executeBulkRequest(int numberOfActionRequests,
                                   Iterable<DocWriteRequest<?>> actionRequests,
                                   BiConsumer<Integer, Exception> onFailure,
                                   BiConsumer<Thread, Exception> onCompletion,
                                   IntConsumer onDropped) {

        threadPool.executor(ThreadPool.Names.WRITE).execute(new AbstractRunnable() {

            @Override
            public void onFailure(Exception e) {
                onCompletion.accept(null, e);
            }

            @Override
            protected void doRun() {
                final Thread originalThread = Thread.currentThread();
                final AtomicInteger counter = new AtomicInteger(numberOfActionRequests);
                int i = 0;
                for (DocWriteRequest<?> actionRequest : actionRequests) {
                    IndexRequest indexRequest = TransportBulkAction.getIndexWriteRequest(actionRequest);
                    if (indexRequest == null) {
                        if (counter.decrementAndGet() == 0){
                            onCompletion.accept(originalThread, null);
                        }
                        assert counter.get() >= 0;
                        continue;
                    }

                    final String pipelineId = indexRequest.getPipeline();
                    indexRequest.setPipeline(NOOP_PIPELINE_NAME);
                    final String finalPipelineId = indexRequest.getFinalPipeline();
                    indexRequest.setFinalPipeline(NOOP_PIPELINE_NAME);
                    final List<String> pipelines;
                    if (IngestService.NOOP_PIPELINE_NAME.equals(pipelineId) == false
                        && IngestService.NOOP_PIPELINE_NAME.equals(finalPipelineId) == false) {
                        pipelines = List.of(pipelineId, finalPipelineId);
                    } else if (IngestService.NOOP_PIPELINE_NAME.equals(pipelineId) == false ) {
                        pipelines = List.of(pipelineId);
                    } else if (IngestService.NOOP_PIPELINE_NAME.equals(finalPipelineId) == false) {
                        pipelines = List.of(finalPipelineId);
                    } else {
                        if (counter.decrementAndGet() == 0) {
                            onCompletion.accept(originalThread, null);
                        }
                        assert counter.get() >= 0;
                        continue;
                    }

                    executePipelines(i, pipelines.iterator(), indexRequest, onDropped, onFailure, counter, onCompletion, originalThread);

                    i++;
                }
            }
        });
    }

    private void executePipelines(
        final int slot,
        final Iterator<String> it,
        final IndexRequest indexRequest,
        final IntConsumer onDropped,
        final BiConsumer<Integer, Exception> onFailure,
        final AtomicInteger counter,
        final BiConsumer<Thread, Exception> onCompletion,
        final Thread originalThread
    ) {
        while (it.hasNext()) {
            final String pipelineId = it.next();
            try {
                PipelineHolder holder = pipelines.get(pipelineId);
                if (holder == null) {
                    throw new IllegalArgumentException("pipeline with id [" + pipelineId + "] does not exist");
                }
                Pipeline pipeline = holder.pipeline;
                innerExecute(slot, indexRequest, pipeline, onDropped, e -> {
                    if (e != null) {
                        onFailure.accept(slot, e);
                    }

                    if (it.hasNext()) {
                        executePipelines(slot, it, indexRequest, onDropped, onFailure, counter, onCompletion, originalThread);
                    } else {
                        if (counter.decrementAndGet() == 0) {
                            onCompletion.accept(originalThread, null);
                        }
                        assert counter.get() >= 0;
                    }
                });
            } catch (Exception e) {
                onFailure.accept(slot, e);
                if (counter.decrementAndGet() == 0) {
                    onCompletion.accept(originalThread, null);
                }
                assert counter.get() >= 0;
                break;
            }
        }
    }

    public IngestStats stats() {
        IngestStats.Builder statsBuilder = new IngestStats.Builder();
        statsBuilder.addTotalMetrics(totalMetrics);
        pipelines.forEach((id, holder) -> {
            Pipeline pipeline = holder.pipeline;
            CompoundProcessor rootProcessor = pipeline.getCompoundProcessor();
            statsBuilder.addPipelineMetrics(id, pipeline.getMetrics());
            List<Tuple<Processor, IngestMetric>> processorMetrics = new ArrayList<>();
            getProcessorMetrics(rootProcessor, processorMetrics);
            processorMetrics.forEach(t -> {
                Processor processor = t.v1();
                IngestMetric processorMetric = t.v2();
                statsBuilder.addProcessorMetrics(id, getProcessorName(processor), processor.getType(), processorMetric);
            });
        });
        return statsBuilder.build();
    }

    /**
     * Adds a listener that gets invoked with the current cluster state before processor factories
     * get invoked.
     *
     * This is useful for components that are used by ingest processors, so that they have the opportunity to update
     * before these components get used by the ingest processor factory.
     */
    public void addIngestClusterStateListener(Consumer<ClusterState> listener) {
        ingestClusterStateListeners.add(listener);
    }

    //package private for testing
    static String getProcessorName(Processor processor) {
        // conditionals are implemented as wrappers around the real processor, so get the real processor for the correct type for the name
        if(processor instanceof ConditionalProcessor){
            processor = ((ConditionalProcessor) processor).getInnerProcessor();
        }
        StringBuilder sb = new StringBuilder(5);
        sb.append(processor.getType());

        if(processor instanceof PipelineProcessor){
            String pipelineName = ((PipelineProcessor) processor).getPipelineTemplate().newInstance(Map.of()).execute();
            sb.append(":");
            sb.append(pipelineName);
        }
        String tag = processor.getTag();
        if(tag != null && !tag.isEmpty()){
            sb.append(":");
            sb.append(tag);
        }
        return sb.toString();
    }

    private void innerExecute(int slot, IndexRequest indexRequest, Pipeline pipeline, IntConsumer itemDroppedHandler,
                              Consumer<Exception> handler) {
        if (pipeline.getProcessors().isEmpty()) {
            handler.accept(null);
            return;
        }

        long startTimeInNanos = System.nanoTime();
        // the pipeline specific stat holder may not exist and that is fine:
        // (e.g. the pipeline may have been removed while we're ingesting a document
        totalMetrics.preIngest();
        String index = indexRequest.index();
        String id = indexRequest.id();
        String routing = indexRequest.routing();
        Long version = indexRequest.version();
        VersionType versionType = indexRequest.versionType();
        Map<String, Object> sourceAsMap = indexRequest.sourceAsMap();
        IngestDocument ingestDocument = new IngestDocument(index, id, routing, version, versionType, sourceAsMap);
        ingestDocument.executePipeline(pipeline, (result, e) -> {
            long ingestTimeInMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeInNanos);
            totalMetrics.postIngest(ingestTimeInMillis);
            if (e != null) {
                totalMetrics.ingestFailed();
                handler.accept(e);
            } else if (result == null) {
                itemDroppedHandler.accept(slot);
                handler.accept(null);
            } else {
                Map<IngestDocument.MetaData, Object> metadataMap = ingestDocument.extractMetadata();
                //it's fine to set all metadata fields all the time, as ingest document holds their starting values
                //before ingestion, which might also get modified during ingestion.
                indexRequest.index((String) metadataMap.get(IngestDocument.MetaData.INDEX));
                indexRequest.id((String) metadataMap.get(IngestDocument.MetaData.ID));
                indexRequest.routing((String) metadataMap.get(IngestDocument.MetaData.ROUTING));
                indexRequest.version(((Number) metadataMap.get(IngestDocument.MetaData.VERSION)).longValue());
                if (metadataMap.get(IngestDocument.MetaData.VERSION_TYPE) != null) {
                    indexRequest.versionType(VersionType.fromString((String) metadataMap.get(IngestDocument.MetaData.VERSION_TYPE)));
                }
                indexRequest.source(ingestDocument.getSourceAndMetadata(), indexRequest.getContentType());
                handler.accept(null);
            }
        });
    }

    @Override
    public void applyClusterState(final ClusterChangedEvent event) {
        ClusterState state = event.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        // Publish cluster state to components that are used by processor factories before letting
        // processor factories create new processor instances.
        // (Note that this needs to be done also in the case when there is no change to ingest metadata, because in the case
        // when only the part of the cluster state that a component is interested in, is updated.)
        ingestClusterStateListeners.forEach(consumer -> consumer.accept(state));

        IngestMetadata newIngestMetadata = state.getMetaData().custom(IngestMetadata.TYPE);
        if (newIngestMetadata == null) {
            return;
        }

        try {
            innerUpdatePipelines(newIngestMetadata);
        } catch (ElasticsearchParseException e) {
            logger.warn("failed to update ingest pipelines", e);
        }
    }

    void innerUpdatePipelines(IngestMetadata newIngestMetadata) {
        Map<String, PipelineHolder> existingPipelines = this.pipelines;

        // Lazy initialize these variables in order to favour the most like scenario that there are no pipeline changes:
        Map<String, PipelineHolder> newPipelines = null;
        List<ElasticsearchParseException> exceptions = null;
        // Iterate over pipeline configurations in ingest metadata and constructs a new pipeline if there is no pipeline
        // or the pipeline configuration has been modified
        for (PipelineConfiguration newConfiguration : newIngestMetadata.getPipelines().values()) {
            PipelineHolder previous = existingPipelines.get(newConfiguration.getId());
            if (previous != null && previous.configuration.equals(newConfiguration)) {
                continue;
            }

            if (newPipelines == null) {
                newPipelines = new HashMap<>(existingPipelines);
            }
            try {
                Pipeline newPipeline =
                    Pipeline.create(newConfiguration.getId(), newConfiguration.getConfigAsMap(), processorFactories, scriptService);
                newPipelines.put(
                    newConfiguration.getId(),
                    new PipelineHolder(newConfiguration, newPipeline)
                );

                if (previous == null) {
                    continue;
                }
                Pipeline oldPipeline = previous.pipeline;
                newPipeline.getMetrics().add(oldPipeline.getMetrics());
                List<Tuple<Processor, IngestMetric>> oldPerProcessMetrics = new ArrayList<>();
                List<Tuple<Processor, IngestMetric>> newPerProcessMetrics = new ArrayList<>();
                getProcessorMetrics(oldPipeline.getCompoundProcessor(), oldPerProcessMetrics);
                getProcessorMetrics(newPipeline.getCompoundProcessor(), newPerProcessMetrics);
                //Best attempt to populate new processor metrics using a parallel array of the old metrics. This is not ideal since
                //the per processor metrics may get reset when the arrays don't match. However, to get to an ideal model, unique and
                //consistent id's per processor and/or semantic equals for each processor will be needed.
                if (newPerProcessMetrics.size() == oldPerProcessMetrics.size()) {
                    Iterator<Tuple<Processor, IngestMetric>> oldMetricsIterator = oldPerProcessMetrics.iterator();
                    for (Tuple<Processor, IngestMetric> compositeMetric : newPerProcessMetrics) {
                        String type = compositeMetric.v1().getType();
                        IngestMetric metric = compositeMetric.v2();
                        if (oldMetricsIterator.hasNext()) {
                            Tuple<Processor, IngestMetric> oldCompositeMetric = oldMetricsIterator.next();
                            String oldType = oldCompositeMetric.v1().getType();
                            IngestMetric oldMetric = oldCompositeMetric.v2();
                            if (type.equals(oldType)) {
                                metric.add(oldMetric);
                            }
                        }
                    }
                }
            } catch (ElasticsearchParseException e) {
                Pipeline pipeline = substitutePipeline(newConfiguration.getId(), e);
                newPipelines.put(newConfiguration.getId(), new PipelineHolder(newConfiguration, pipeline));
                if (exceptions == null) {
                    exceptions = new ArrayList<>();
                }
                exceptions.add(e);
            } catch (Exception e) {
                ElasticsearchParseException parseException = new ElasticsearchParseException(
                    "Error updating pipeline with id [" + newConfiguration.getId() + "]", e);
                Pipeline pipeline = substitutePipeline(newConfiguration.getId(), parseException);
                newPipelines.put(newConfiguration.getId(), new PipelineHolder(newConfiguration, pipeline));
                if (exceptions == null) {
                    exceptions = new ArrayList<>();
                }
                exceptions.add(parseException);
            }
        }

        // Iterate over the current active pipelines and check whether they are missing in the pipeline configuration and
        // if so delete the pipeline from new Pipelines map:
        for (Map.Entry<String, PipelineHolder> entry : existingPipelines.entrySet()) {
            if (newIngestMetadata.getPipelines().get(entry.getKey()) == null) {
                if (newPipelines == null) {
                    newPipelines = new HashMap<>(existingPipelines);
                }
                newPipelines.remove(entry.getKey());
            }
        }

        if (newPipelines != null) {
            // Update the pipelines:
            this.pipelines = Map.copyOf(newPipelines);

            // Rethrow errors that may have occurred during creating new pipeline instances:
            if (exceptions != null) {
                ExceptionsHelper.rethrowAndSuppress(exceptions);
            }
        }
    }

    /**
     * Gets all the Processors of the given type from within a Pipeline.
     * @param pipelineId the pipeline to inspect
     * @param clazz the Processor class to look for
     * @return True if the pipeline contains an instance of the Processor class passed in
     */
    public<P extends Processor> List<P> getProcessorsInPipeline(String pipelineId, Class<P> clazz) {
        Pipeline pipeline = getPipeline(pipelineId);
        if (pipeline == null) {
            throw new IllegalArgumentException("pipeline with id [" + pipelineId + "] does not exist");
        }

        List<P> processors = new ArrayList<>();
        for (Processor processor: pipeline.flattenAllProcessors()) {
            if (clazz.isAssignableFrom(processor.getClass())) {
                processors.add(clazz.cast(processor));
            }

            while (processor instanceof WrappingProcessor) {
                WrappingProcessor wrappingProcessor = (WrappingProcessor) processor;
                if (clazz.isAssignableFrom(wrappingProcessor.getInnerProcessor().getClass())) {
                    processors.add(clazz.cast(wrappingProcessor.getInnerProcessor()));
                }
                processor = wrappingProcessor.getInnerProcessor();
                // break in the case of self referencing processors in the event a processor author creates a
                // wrapping processor that has its inner processor refer to itself.
                if (wrappingProcessor == processor) {
                    break;
                }
            }
        }

        return processors;
    }

    private static Pipeline substitutePipeline(String id, ElasticsearchParseException e) {
        String tag = e.getHeaderKeys().contains("processor_tag") ? e.getHeader("processor_tag").get(0) : null;
        String type = e.getHeaderKeys().contains("processor_type") ? e.getHeader("processor_type").get(0) : "unknown";
        String errorMessage = "pipeline with id [" + id + "] could not be loaded, caused by [" + e.getDetailedMessage() + "]";
        Processor failureProcessor = new AbstractProcessor(tag) {
            @Override
            public IngestDocument execute(IngestDocument ingestDocument) {
                throw new IllegalStateException(errorMessage);
            }

            @Override
            public String getType() {
                return type;
            }
        };
        String description = "this is a place holder pipeline, because pipeline with id [" +  id + "] could not be loaded";
        return new Pipeline(id, description, null, new CompoundProcessor(failureProcessor));
    }

    static class PipelineHolder {

        final PipelineConfiguration configuration;
        final Pipeline pipeline;

        PipelineHolder(PipelineConfiguration configuration, Pipeline pipeline) {
            this.configuration = Objects.requireNonNull(configuration);
            this.pipeline = Objects.requireNonNull(pipeline);
        }
    }

}
