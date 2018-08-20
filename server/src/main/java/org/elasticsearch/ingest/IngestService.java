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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
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

/**
 * Holder class for several ingest related services.
 */
public class IngestService implements ClusterStateApplier {

    public static final String NOOP_PIPELINE_NAME = "_none";

    private final ClusterService clusterService;
    private final PipelineStore pipelineStore;
    private final PipelineExecutionService pipelineExecutionService;

    public IngestService(ClusterService clusterService, ThreadPool threadPool,
                         Environment env, ScriptService scriptService, AnalysisRegistry analysisRegistry,
                         List<IngestPlugin> ingestPlugins) {
        this.clusterService = clusterService;
        this.pipelineStore = new PipelineStore(
            processorFactories(
                ingestPlugins,
                new Processor.Parameters(
                    env, scriptService, analysisRegistry,
                    threadPool.getThreadContext(), threadPool::relativeTimeInMillis,
                    (delay, command) -> threadPool.schedule(
                        TimeValue.timeValueMillis(delay), ThreadPool.Names.GENERIC, command
                    )
                )
            )
        );
        this.pipelineExecutionService = new PipelineExecutionService(pipelineStore, threadPool);
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

    /**
     * Deletes the pipeline specified by id in the request.
     */
    public void delete(DeletePipelineRequest request,
        ActionListener<AcknowledgedResponse> listener) {
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

    public void executeBulkRequest(Iterable<DocWriteRequest<?>> actionRequests, BiConsumer<IndexRequest, Exception> itemFailureHandler,
        Consumer<Exception> completionHandler) {
        pipelineExecutionService.executeBulkRequest(actionRequests, itemFailureHandler, completionHandler);
    }

    public IngestStats stats() {
        return pipelineExecutionService.stats();
    }

    /**
     * Stores the specified pipeline definition in the request.
     */
    public void putPipeline(Map<DiscoveryNode, IngestInfo> ingestInfos, PutPipelineRequest request,
        ActionListener<AcknowledgedResponse> listener) throws Exception {
        pipelineStore.put(clusterService, ingestInfos, request, listener);
    }

    /**
     * Returns the pipeline by the specified id
     */
    public Pipeline getPipeline(String id) {
        return pipelineStore.get(id);
    }

    public Map<String, Processor.Factory> getProcessorFactories() {
        return pipelineStore.getProcessorFactories();
    }

    public IngestInfo info() {
        Map<String, Processor.Factory> processorFactories = getProcessorFactories();
        List<ProcessorInfo> processorInfoList = new ArrayList<>(processorFactories.size());
        for (Map.Entry<String, Processor.Factory> entry : processorFactories.entrySet()) {
            processorInfoList.add(new ProcessorInfo(entry.getKey()));
        }
        return new IngestInfo(processorInfoList);
    }

    Map<String, Pipeline> pipelines() {
        return pipelineStore.pipelines;
    }

    void validatePipeline(Map<DiscoveryNode, IngestInfo> ingestInfos, PutPipelineRequest request) throws Exception {
        pipelineStore.validatePipeline(ingestInfos, request);
    }

    void updatePipelineStats(IngestMetadata ingestMetadata) {
        pipelineExecutionService.updatePipelineStats(ingestMetadata);
    }

    @Override
    public void applyClusterState(final ClusterChangedEvent event) {
        ClusterState state = event.state();
        pipelineStore.innerUpdatePipelines(event.previousState(), state);
        IngestMetadata ingestMetadata = state.getMetaData().custom(IngestMetadata.TYPE);
        if (ingestMetadata != null) {
            pipelineExecutionService.updatePipelineStats(ingestMetadata);
        }
    }

    public static final class PipelineStore {

        private final Map<String, Processor.Factory> processorFactories;

        // Ideally this should be in IngestMetadata class, but we don't have the processor factories around there.
        // We know of all the processor factories when a node with all its plugin have been initialized. Also some
        // processor factories rely on other node services. Custom metadata is statically registered when classes
        // are loaded, so in the cluster state we just save the pipeline config and here we keep the actual pipelines around.
        volatile Map<String, Pipeline> pipelines = new HashMap<>();

        private PipelineStore(Map<String, Processor.Factory> processorFactories) {
            this.processorFactories = processorFactories;
        }

        void innerUpdatePipelines(ClusterState previousState, ClusterState state) {
            if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
                return;
            }

            IngestMetadata ingestMetadata = state.getMetaData().custom(IngestMetadata.TYPE);
            IngestMetadata previousIngestMetadata = previousState.getMetaData().custom(IngestMetadata.TYPE);
            if (Objects.equals(ingestMetadata, previousIngestMetadata)) {
                return;
            }

            Map<String, Pipeline> pipelines = new HashMap<>();
            List<ElasticsearchParseException> exceptions = new ArrayList<>();
            for (PipelineConfiguration pipeline : ingestMetadata.getPipelines().values()) {
                try {
                    pipelines.put(pipeline.getId(), Pipeline.create(pipeline.getId(), pipeline.getConfigAsMap(), processorFactories));
                } catch (ElasticsearchParseException e) {
                    pipelines.put(pipeline.getId(), substitutePipeline(pipeline.getId(), e));
                    exceptions.add(e);
                } catch (Exception e) {
                    ElasticsearchParseException parseException = new ElasticsearchParseException(
                        "Error updating pipeline with id [" + pipeline.getId() + "]", e);
                    pipelines.put(pipeline.getId(), substitutePipeline(pipeline.getId(), parseException));
                    exceptions.add(parseException);
                }
            }
            this.pipelines = Collections.unmodifiableMap(pipelines);
            ExceptionsHelper.rethrowAndSuppress(exceptions);
        }

        private static Pipeline substitutePipeline(String id, ElasticsearchParseException e) {
            String tag = e.getHeaderKeys().contains("processor_tag") ? e.getHeader("processor_tag").get(0) : null;
            String type = e.getHeaderKeys().contains("processor_type") ? e.getHeader("processor_type").get(0) : "unknown";
            String errorMessage = "pipeline with id [" + id + "] could not be loaded, caused by [" + e.getDetailedMessage() + "]";
            Processor failureProcessor = new AbstractProcessor(tag) {
                @Override
                public void execute(IngestDocument ingestDocument) {
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

        /**
         * Stores the specified pipeline definition in the request.
         */
        public void put(ClusterService clusterService, Map<DiscoveryNode, IngestInfo> ingestInfos, PutPipelineRequest request,
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

        void validatePipeline(Map<DiscoveryNode, IngestInfo> ingestInfos, PutPipelineRequest request) throws Exception {
            if (ingestInfos.isEmpty()) {
                throw new IllegalStateException("Ingest info is empty");
            }

            Map<String, Object> pipelineConfig = XContentHelper.convertToMap(request.getSource(), false, request.getXContentType()).v2();
            Pipeline pipeline = Pipeline.create(request.getId(), pipelineConfig, processorFactories);
            List<Exception> exceptions = new ArrayList<>();
            for (Processor processor : pipeline.flattenAllProcessors()) {
                for (Map.Entry<DiscoveryNode, IngestInfo> entry : ingestInfos.entrySet()) {
                    if (entry.getValue().containsProcessor(processor.getType()) == false) {
                        String message = "Processor type [" + processor.getType() + "] is not installed on node [" + entry.getKey() + "]";
                        exceptions.add(
                            ConfigurationUtils.newConfigurationException(processor.getType(), processor.getTag(), null, message)
                        );
                    }
                }
            }
            ExceptionsHelper.rethrowAndSuppress(exceptions);
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

        /**
         * Returns the pipeline by the specified id
         */
        public Pipeline get(String id) {
            return pipelines.get(id);
        }

        public Map<String, Processor.Factory> getProcessorFactories() {
            return processorFactories;
        }
    }

    private static final class PipelineExecutionService {

        private final PipelineStore store;
        private final ThreadPool threadPool;

        private final StatsHolder totalStats = new StatsHolder();
        private volatile Map<String, StatsHolder> statsHolderPerPipeline = Collections.emptyMap();

        PipelineExecutionService(PipelineStore store, ThreadPool threadPool) {
            this.store = store;
            this.threadPool = threadPool;
        }

        void executeBulkRequest(Iterable<DocWriteRequest<?>> actionRequests,
                                       BiConsumer<IndexRequest, Exception> itemFailureHandler,
                                       Consumer<Exception> completionHandler) {
            threadPool.executor(ThreadPool.Names.WRITE).execute(new AbstractRunnable() {

                @Override
                public void onFailure(Exception e) {
                    completionHandler.accept(e);
                }

                @Override
                protected void doRun() {
                    for (DocWriteRequest<?> actionRequest : actionRequests) {
                        IndexRequest indexRequest = null;
                        if (actionRequest instanceof IndexRequest) {
                            indexRequest = (IndexRequest) actionRequest;
                        } else if (actionRequest instanceof UpdateRequest) {
                            UpdateRequest updateRequest = (UpdateRequest) actionRequest;
                            indexRequest = updateRequest.docAsUpsert() ? updateRequest.doc() : updateRequest.upsertRequest();
                        }
                        if (indexRequest == null) {
                            continue;
                        }
                        String pipeline = indexRequest.getPipeline();
                        if (NOOP_PIPELINE_NAME.equals(pipeline) == false) {
                            try {
                                innerExecute(indexRequest, getPipeline(indexRequest.getPipeline()));
                                //this shouldn't be needed here but we do it for consistency with index api
                                // which requires it to prevent double execution
                                indexRequest.setPipeline(NOOP_PIPELINE_NAME);
                            } catch (Exception e) {
                                itemFailureHandler.accept(indexRequest, e);
                            }
                        }
                    }
                    completionHandler.accept(null);
                }
            });
        }

        IngestStats stats() {
            Map<String, StatsHolder> statsHolderPerPipeline = this.statsHolderPerPipeline;

            Map<String, IngestStats.Stats> statsPerPipeline = new HashMap<>(statsHolderPerPipeline.size());
            for (Map.Entry<String, StatsHolder> entry : statsHolderPerPipeline.entrySet()) {
                statsPerPipeline.put(entry.getKey(), entry.getValue().createStats());
            }

            return new IngestStats(totalStats.createStats(), statsPerPipeline);
        }

        void updatePipelineStats(IngestMetadata ingestMetadata) {
            boolean changed = false;
            Map<String, StatsHolder> newStatsPerPipeline = new HashMap<>(statsHolderPerPipeline);
            Iterator<String> iterator = newStatsPerPipeline.keySet().iterator();
            while (iterator.hasNext()) {
                String pipeline = iterator.next();
                if (ingestMetadata.getPipelines().containsKey(pipeline) == false) {
                    iterator.remove();
                    changed = true;
                }
            }
            for (String pipeline : ingestMetadata.getPipelines().keySet()) {
                if (newStatsPerPipeline.containsKey(pipeline) == false) {
                    newStatsPerPipeline.put(pipeline, new StatsHolder());
                    changed = true;
                }
            }

            if (changed) {
                statsHolderPerPipeline = Collections.unmodifiableMap(newStatsPerPipeline);
            }
        }

        private void innerExecute(IndexRequest indexRequest, Pipeline pipeline) throws Exception {
            if (pipeline.getProcessors().isEmpty()) {
                return;
            }

            long startTimeInNanos = System.nanoTime();
            // the pipeline specific stat holder may not exist and that is fine:
            // (e.g. the pipeline may have been removed while we're ingesting a document
            Optional<StatsHolder> pipelineStats = Optional.ofNullable(statsHolderPerPipeline.get(pipeline.getId()));
            try {
                totalStats.preIngest();
                pipelineStats.ifPresent(StatsHolder::preIngest);
                String index = indexRequest.index();
                String type = indexRequest.type();
                String id = indexRequest.id();
                String routing = indexRequest.routing();
                Long version = indexRequest.version();
                VersionType versionType = indexRequest.versionType();
                Map<String, Object> sourceAsMap = indexRequest.sourceAsMap();
                IngestDocument ingestDocument = new IngestDocument(index, type, id, routing, version, versionType, sourceAsMap);
                pipeline.execute(ingestDocument);

                Map<IngestDocument.MetaData, Object> metadataMap = ingestDocument.extractMetadata();
                //it's fine to set all metadata fields all the time, as ingest document holds their starting values
                //before ingestion, which might also get modified during ingestion.
                indexRequest.index((String) metadataMap.get(IngestDocument.MetaData.INDEX));
                indexRequest.type((String) metadataMap.get(IngestDocument.MetaData.TYPE));
                indexRequest.id((String) metadataMap.get(IngestDocument.MetaData.ID));
                indexRequest.routing((String) metadataMap.get(IngestDocument.MetaData.ROUTING));
                indexRequest.version(((Number) metadataMap.get(IngestDocument.MetaData.VERSION)).longValue());
                if (metadataMap.get(IngestDocument.MetaData.VERSION_TYPE) != null) {
                    indexRequest.versionType(VersionType.fromString((String) metadataMap.get(IngestDocument.MetaData.VERSION_TYPE)));
                }
                indexRequest.source(ingestDocument.getSourceAndMetadata());
            } catch (Exception e) {
                totalStats.ingestFailed();
                pipelineStats.ifPresent(StatsHolder::ingestFailed);
                throw e;
            } finally {
                long ingestTimeInMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeInNanos);
                totalStats.postIngest(ingestTimeInMillis);
                pipelineStats.ifPresent(statsHolder -> statsHolder.postIngest(ingestTimeInMillis));
            }
        }

        private Pipeline getPipeline(String pipelineId) {
            Pipeline pipeline = store.get(pipelineId);
            if (pipeline == null) {
                throw new IllegalArgumentException("pipeline with id [" + pipelineId + "] does not exist");
            }
            return pipeline;
        }

        private static class StatsHolder {

            private final MeanMetric ingestMetric = new MeanMetric();
            private final CounterMetric ingestCurrent = new CounterMetric();
            private final CounterMetric ingestFailed = new CounterMetric();

            void preIngest() {
                ingestCurrent.inc();
            }

            void postIngest(long ingestTimeInMillis) {
                ingestCurrent.dec();
                ingestMetric.inc(ingestTimeInMillis);
            }

            void ingestFailed() {
                ingestFailed.inc();
            }

            IngestStats.Stats createStats() {
                return new IngestStats.Stats(ingestMetric.count(), ingestMetric.sum(), ingestCurrent.count(), ingestFailed.count());
            }

        }

    }
}
