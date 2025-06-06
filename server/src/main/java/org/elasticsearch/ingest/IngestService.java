/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.bulk.FailureStoreMetrics;
import org.elasticsearch.action.bulk.IndexDocFailureStoreStatus;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.env.Environment;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.node.ReportingService;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.internal.XContentParserDecorator;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.core.UpdateForV10.Owner.DATA_MANAGEMENT;

/**
 * Holder class for several ingest related services.
 */
public class IngestService implements ClusterStateApplier, ReportingService<IngestInfo> {

    public static final String NOOP_PIPELINE_NAME = "_none";

    public static final String INGEST_ORIGIN = "ingest";

    private static final Logger logger = LogManager.getLogger(IngestService.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(IngestService.class);

    public static final NodeFeature FIELD_ACCESS_PATTERN = new NodeFeature("ingest.field_access_pattern", true);

    /**
     * Checks the locally supported node features without relying on cluster state or feature service.
     * This is primarily to support the Logstash elastic_integration plugin which uses the IngestService
     * internally and thus would not have access to cluster service or feature services. NodeFeatures that
     * are accepted here should be currently and generally available in Elasticsearch.
     * @param nodeFeature The node feature to check
     * @return true if the node feature can be supported in the local library code, false if it is not supported
     */
    public static boolean locallySupportedIngestFeature(NodeFeature nodeFeature) {
        if (DataStream.LOGS_STREAM_FEATURE_FLAG) {
            // logs_stream feature flag guard
            return IngestService.FIELD_ACCESS_PATTERN.equals(nodeFeature);
        }
        // Default to unsupported if not contained here
        return false;
    }

    private final MasterServiceTaskQueue<PipelineClusterStateUpdateTask> taskQueue;
    private final ClusterService clusterService;
    private final ScriptService scriptService;
    private final Map<String, Processor.Factory> processorFactories;
    // Ideally this should be in IngestMetadata class, but we don't have the processor factories around there.
    // We know of all the processor factories when a node with all its plugin have been initialized. Also some
    // processor factories rely on other node services. Custom metadata is statically registered when classes
    // are loaded, so in the cluster state we just save the pipeline config and here we keep the actual pipelines around.
    private volatile ImmutableOpenMap<ProjectId, ImmutableOpenMap<String, PipelineHolder>> pipelines = ImmutableOpenMap.of();
    private final ThreadPool threadPool;
    private final IngestMetric totalMetrics = new IngestMetric();
    private final FailureStoreMetrics failureStoreMetrics;
    private final List<Consumer<ClusterState>> ingestClusterStateListeners = new CopyOnWriteArrayList<>();
    private volatile ClusterState state;
    private final ProjectResolver projectResolver;
    private final FeatureService featureService;

    private static BiFunction<Long, Runnable, Scheduler.ScheduledCancellable> createScheduler(ThreadPool threadPool) {
        return (delay, command) -> threadPool.schedule(command, TimeValue.timeValueMillis(delay), threadPool.generic());
    }

    public static MatcherWatchdog createGrokThreadWatchdog(Environment env, ThreadPool threadPool) {
        final Settings settings = env.settings();
        final BiFunction<Long, Runnable, Scheduler.ScheduledCancellable> scheduler = createScheduler(threadPool);
        long intervalMillis = IngestSettings.GROK_WATCHDOG_INTERVAL.get(settings).getMillis();
        long maxExecutionTimeMillis = IngestSettings.GROK_WATCHDOG_INTERVAL.get(settings).getMillis();
        return MatcherWatchdog.newInstance(
            intervalMillis,
            maxExecutionTimeMillis,
            threadPool.relativeTimeInMillisSupplier(),
            scheduler::apply
        );
    }

    /**
     * Cluster state task executor for ingest pipeline operations
     */
    static final ClusterStateTaskExecutor<PipelineClusterStateUpdateTask> PIPELINE_TASK_EXECUTOR = batchExecutionContext -> {
        var state = batchExecutionContext.initialState();
        Map<ProjectId, ProjectMetadata.Builder> builders = Maps.newHashMapWithExpectedSize(state.metadata().projects().size());
        var changed = false;
        for (final var taskContext : batchExecutionContext.taskContexts()) {
            try {
                final var task = taskContext.getTask();
                final ProjectMetadata.Builder projectBuilder = builders.computeIfAbsent(
                    task.projectId,
                    k -> ProjectMetadata.builder(state.metadata().getProject(k))
                );
                final IngestMetadata currentIngestMetadata = projectBuilder.getCustom(IngestMetadata.TYPE);
                final var allIndexMetadata = state.metadata().getProject(task.projectId).indices().values();
                try (var ignored = taskContext.captureResponseHeaders()) {
                    final var newIngestMetadata = task.execute(currentIngestMetadata, allIndexMetadata);
                    if (newIngestMetadata != currentIngestMetadata) {
                        projectBuilder.putCustom(IngestMetadata.TYPE, newIngestMetadata);
                        changed = true;
                    }
                }
                taskContext.success(() -> task.listener.onResponse(AcknowledgedResponse.TRUE));
            } catch (Exception e) {
                taskContext.onFailure(e);
            }
        }
        if (changed == false) {
            return state;
        }
        var stateBuilder = ClusterState.builder(state);
        builders.values().forEach(stateBuilder::putProjectMetadata);
        return stateBuilder.build();
    };

    /**
     * Specialized cluster state update task specifically for ingest pipeline operations.
     * These operations all receive an AcknowledgedResponse.
     */
    public abstract static class PipelineClusterStateUpdateTask implements ClusterStateTaskListener {
        final ProjectId projectId;
        final ActionListener<AcknowledgedResponse> listener;

        PipelineClusterStateUpdateTask(ProjectId projectId, ActionListener<AcknowledgedResponse> listener) {
            this.projectId = projectId;
            this.listener = listener;
        }

        public abstract IngestMetadata execute(IngestMetadata currentIngestMetadata, Collection<IndexMetadata> allIndexMetadata);

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    @SuppressWarnings("this-escape")
    public IngestService(
        ClusterService clusterService,
        ThreadPool threadPool,
        Environment env,
        ScriptService scriptService,
        AnalysisRegistry analysisRegistry,
        List<IngestPlugin> ingestPlugins,
        Client client,
        MatcherWatchdog matcherWatchdog,
        FailureStoreMetrics failureStoreMetrics,
        ProjectResolver projectResolver,
        FeatureService featureService
    ) {
        this.clusterService = clusterService;
        this.scriptService = scriptService;
        this.processorFactories = processorFactories(
            ingestPlugins,
            new Processor.Parameters(
                env,
                scriptService,
                analysisRegistry,
                threadPool.getThreadContext(),
                threadPool.relativeTimeInMillisSupplier(),
                createScheduler(threadPool),
                this,
                client,
                threadPool.generic()::execute,
                matcherWatchdog
            )
        );
        this.threadPool = threadPool;
        this.taskQueue = clusterService.createTaskQueue("ingest-pipelines", Priority.NORMAL, PIPELINE_TASK_EXECUTOR);
        this.failureStoreMetrics = failureStoreMetrics;
        this.projectResolver = projectResolver;
        this.featureService = featureService;
    }

    /**
     * This copy constructor returns a copy of the given ingestService, using all of the same internal state. The returned copy is not
     * registered to listen to any cluster state changes
     * @param ingestService
     */
    IngestService(IngestService ingestService) {
        this.clusterService = ingestService.clusterService;
        this.scriptService = ingestService.scriptService;
        this.processorFactories = ingestService.processorFactories;
        this.threadPool = ingestService.threadPool;
        this.taskQueue = ingestService.taskQueue;
        this.pipelines = ingestService.pipelines;
        this.state = ingestService.state;
        this.failureStoreMetrics = ingestService.failureStoreMetrics;
        this.projectResolver = ingestService.projectResolver;
        this.featureService = ingestService.featureService;
    }

    private static Map<String, Processor.Factory> processorFactories(List<IngestPlugin> ingestPlugins, Processor.Parameters parameters) {
        Map<String, Processor.Factory> processorFactories = new TreeMap<>();
        for (IngestPlugin ingestPlugin : ingestPlugins) {
            Map<String, Processor.Factory> newProcessors = ingestPlugin.getProcessors(parameters);
            for (Map.Entry<String, Processor.Factory> entry : newProcessors.entrySet()) {
                if (processorFactories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Ingest processor [" + entry.getKey() + "] is already registered");
                }
            }
        }
        logger.debug("registered ingest processor types: {}", processorFactories.keySet());
        return Map.copyOf(processorFactories);
    }

    /**
     * Resolves the potential pipelines (default and final) from the requests or templates associated to the index and then **mutates**
     * the {@link org.elasticsearch.action.index.IndexRequest} passed object with the pipeline information.
     * <p>
     * Also, this method marks the request as `isPipelinesResolved = true`: Due to the request could be rerouted from a coordinating node
     * to an ingest node, we have to be able to avoid double resolving the pipelines and also able to distinguish that either the pipeline
     * comes as part of the request or resolved from this method. All this is made to later be able to reject the request in case the
     * pipeline was set by a required pipeline **and** the request also has a pipeline request too.
     *
     * @param originalRequest Original write request received.
     * @param indexRequest    The {@link org.elasticsearch.action.index.IndexRequest} object to update.
     * @param projectMetadata Project metadata from the cluster state from where the pipeline information is derived.
     */
    public static void resolvePipelinesAndUpdateIndexRequest(
        final DocWriteRequest<?> originalRequest,
        final IndexRequest indexRequest,
        final ProjectMetadata projectMetadata
    ) {
        resolvePipelinesAndUpdateIndexRequest(originalRequest, indexRequest, projectMetadata, System.currentTimeMillis());
    }

    static void resolvePipelinesAndUpdateIndexRequest(
        final DocWriteRequest<?> originalRequest,
        final IndexRequest indexRequest,
        final ProjectMetadata metadata,
        final long epochMillis
    ) {
        if (indexRequest.isPipelineResolved() == false) {
            var pipelines = resolvePipelines(originalRequest, indexRequest, metadata, epochMillis);
            setPipelineOnRequest(indexRequest, pipelines);
        }
    }

    static boolean isRolloverOnWrite(ProjectMetadata projectMetadata, IndexRequest indexRequest) {
        DataStream dataStream = projectMetadata.dataStreams().get(indexRequest.index());
        if (dataStream == null) {
            return false;
        }
        return dataStream.getDataComponent().isRolloverOnWrite();
    }

    /**
     *  Resolve the default and final pipelines from the cluster state metadata or index templates.
     *
     * @param originalRequest initial request
     * @param indexRequest the index request, which could be different from the initial request if rerouted
     * @param projectMetadata cluster data metadata
     * @param epochMillis current time for index name resolution
     * @return the resolved pipelines
     */
    public static Pipelines resolvePipelines(
        final DocWriteRequest<?> originalRequest,
        final IndexRequest indexRequest,
        final ProjectMetadata projectMetadata,
        final long epochMillis
    ) {
        if (isRolloverOnWrite(projectMetadata, indexRequest)) {
            return resolvePipelinesFromIndexTemplates(indexRequest, projectMetadata) //
                .orElse(Pipelines.NO_PIPELINES_DEFINED);
        } else {
            return resolvePipelinesFromMetadata(originalRequest, indexRequest, projectMetadata, epochMillis) //
                .or(() -> resolvePipelinesFromIndexTemplates(indexRequest, projectMetadata)) //
                .orElse(Pipelines.NO_PIPELINES_DEFINED);
        }
    }

    /**
     *  Set the request pipeline on the index request if present, otherwise set the default pipeline.
     *  Always set the final pipeline.
     * @param indexRequest the index request
     * @param resolvedPipelines default and final pipelines resolved from metadata and templates
     */
    public static void setPipelineOnRequest(IndexRequest indexRequest, Pipelines resolvedPipelines) {
        // The pipeline coming as part of the request always has priority over the resolved one from metadata or templates
        String requestPipeline = indexRequest.getPipeline();
        if (requestPipeline != null) {
            indexRequest.setPipeline(requestPipeline);
        } else {
            indexRequest.setPipeline(resolvedPipelines.defaultPipeline);
        }
        indexRequest.setFinalPipeline(resolvedPipelines.finalPipeline);
        indexRequest.isPipelineResolved(true);
    }

    public ClusterService getClusterService() {
        return clusterService;
    }

    public ScriptService getScriptService() {
        return scriptService;
    }

    public ProjectResolver getProjectResolver() {
        return projectResolver;
    }

    public FeatureService getFeatureService() {
        return featureService;
    }

    /**
     * Deletes the pipeline specified by id in the request.
     */
    public void delete(ProjectId projectId, DeletePipelineRequest request, ActionListener<AcknowledgedResponse> listener) {
        taskQueue.submitTask(
            "delete-pipeline-" + request.getId(),
            new DeletePipelineClusterStateUpdateTask(projectId, listener, request),
            request.masterNodeTimeout()
        );
    }

    /**
     * Used by this class and {@link org.elasticsearch.action.ingest.ReservedPipelineAction}
     */
    public static class DeletePipelineClusterStateUpdateTask extends PipelineClusterStateUpdateTask {
        private final DeletePipelineRequest request;

        public DeletePipelineClusterStateUpdateTask(
            ProjectId projectId,
            ActionListener<AcknowledgedResponse> listener,
            DeletePipelineRequest request
        ) {
            super(projectId, listener);
            this.request = request;
        }

        @Override
        public IngestMetadata execute(IngestMetadata currentIngestMetadata, Collection<IndexMetadata> allIndexMetadata) {
            if (currentIngestMetadata == null) {
                return null;
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
                return currentIngestMetadata;
            }
            final Map<String, PipelineConfiguration> pipelinesCopy = new HashMap<>(pipelines);
            for (String key : toRemove) {
                validateNotInUse(key, allIndexMetadata);
                pipelinesCopy.remove(key);
            }
            return new IngestMetadata(pipelinesCopy);
        }
    }

    static void validateNotInUse(String pipeline, Collection<IndexMetadata> allIndexMetadata) {
        List<String> defaultPipelineIndices = new ArrayList<>();
        List<String> finalPipelineIndices = new ArrayList<>();
        for (IndexMetadata indexMetadata : allIndexMetadata) {
            String defaultPipeline = IndexSettings.DEFAULT_PIPELINE.get(indexMetadata.getSettings());
            String finalPipeline = IndexSettings.FINAL_PIPELINE.get(indexMetadata.getSettings());
            if (pipeline.equals(defaultPipeline)) {
                defaultPipelineIndices.add(indexMetadata.getIndex().getName());
            }

            if (pipeline.equals(finalPipeline)) {
                finalPipelineIndices.add(indexMetadata.getIndex().getName());
            }
        }

        if (defaultPipelineIndices.size() > 0 || finalPipelineIndices.size() > 0) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "pipeline [%s] cannot be deleted because it is %s%s%s",
                    pipeline,
                    defaultPipelineIndices.size() > 0
                        ? String.format(
                            Locale.ROOT,
                            "the default pipeline for %s index(es) including [%s]",
                            defaultPipelineIndices.size(),
                            defaultPipelineIndices.stream().sorted().limit(3).collect(Collectors.joining(","))
                        )
                        : Strings.EMPTY,
                    defaultPipelineIndices.size() > 0 && finalPipelineIndices.size() > 0 ? " and " : Strings.EMPTY,
                    finalPipelineIndices.size() > 0
                        ? String.format(
                            Locale.ROOT,
                            "the final pipeline for %s index(es) including [%s]",
                            finalPipelineIndices.size(),
                            finalPipelineIndices.stream().sorted().limit(3).collect(Collectors.joining(","))
                        )
                        : Strings.EMPTY
                )
            );
        }
    }

    /**
     * @return pipeline configuration specified by id. If multiple ids or wildcards are specified multiple pipelines
     * may be returned
     */
    // Returning PipelineConfiguration instead of Pipeline, because Pipeline and Processor interface don't
    // know how to serialize themselves.
    public static List<PipelineConfiguration> getPipelines(ProjectMetadata project, String... ids) {
        IngestMetadata ingestMetadata = project.custom(IngestMetadata.TYPE);
        return innerGetPipelines(ingestMetadata, ids);
    }

    static List<PipelineConfiguration> innerGetPipelines(IngestMetadata ingestMetadata, String... ids) {
        if (ingestMetadata == null) {
            return List.of();
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
    public void putPipeline(
        ProjectId projectId,
        PutPipelineRequest request,
        ActionListener<AcknowledgedResponse> listener,
        Consumer<ActionListener<NodesInfoResponse>> nodeInfoListener
    ) throws Exception {
        if (isNoOpPipelineUpdate(state.metadata().getProject(projectId), request)) {
            // existing pipeline matches request pipeline -- no need to update
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }

        nodeInfoListener.accept(listener.delegateFailureAndWrap((l, nodeInfos) -> {
            validatePipelineRequest(projectId, request, nodeInfos);

            taskQueue.submitTask(
                "put-pipeline-" + request.getId(),
                new PutPipelineClusterStateUpdateTask(projectId, l, request),
                request.masterNodeTimeout()
            );
        }));
    }

    public void validatePipelineRequest(ProjectId projectId, PutPipelineRequest request, NodesInfoResponse nodeInfos) throws Exception {
        final Map<String, Object> config = XContentHelper.convertToMap(request.getSource(), false, request.getXContentType()).v2();
        Map<DiscoveryNode, IngestInfo> ingestInfos = new HashMap<>();
        for (NodeInfo nodeInfo : nodeInfos.getNodes()) {
            ingestInfos.put(nodeInfo.getNode(), nodeInfo.getInfo(IngestInfo.class));
        }

        validatePipeline(ingestInfos, projectId, request.getId(), config);
    }

    public static boolean isNoOpPipelineUpdate(ProjectMetadata metadata, PutPipelineRequest request) {
        IngestMetadata currentIngestMetadata = metadata.custom(IngestMetadata.TYPE);
        if (request.getVersion() == null
            && currentIngestMetadata != null
            && currentIngestMetadata.getPipelines().containsKey(request.getId())) {
            var pipelineConfig = XContentHelper.convertToMap(request.getSource(), false, request.getXContentType()).v2();
            var currentPipeline = currentIngestMetadata.getPipelines().get(request.getId());
            if (currentPipeline.getConfig().equals(pipelineConfig)) {
                return true;
            }
        }

        return false;
    }

    /**
     * This method is to be used exclusively by the {@link PipelineProcessor}. It solely exists for the pipeline processor to be able to
     * retrieve the pipeline that it needs to execute. Processors will exclusively be executed in a context where the project id is set in
     * the thread context, which allows us to use the project resolver here.
     */
    public Pipeline getPipeline(String id) {
        return getPipeline(projectResolver.getProjectId(), id);
    }

    /**
     * Returns the pipeline by the specified id
     */
    public Pipeline getPipeline(ProjectId projectId, String id) {
        if (id == null) {
            return null;
        }

        PipelineHolder holder = pipelines.getOrDefault(projectId, ImmutableOpenMap.of()).get(id);
        if (holder != null) {
            return holder.pipeline;
        } else {
            return null;
        }
    }

    public Map<String, Processor.Factory> getProcessorFactories() {
        return processorFactories;
    }

    @Override
    public IngestInfo info() {
        Map<String, Processor.Factory> processorFactories = getProcessorFactories();
        List<ProcessorInfo> processorInfoList = new ArrayList<>(processorFactories.size());
        for (Map.Entry<String, Processor.Factory> entry : processorFactories.entrySet()) {
            processorInfoList.add(new ProcessorInfo(entry.getKey()));
        }
        return new IngestInfo(processorInfoList);
    }

    ImmutableOpenMap<ProjectId, ImmutableOpenMap<String, PipelineHolder>> pipelines() {
        return pipelines;
    }

    /**
     * Recursive method to obtain all the non-failure processors for given compoundProcessor.
     * <p>
     * 'if' and 'ignore_failure'/'on_failure' are implemented as wrappers around the actual processor (via {@link ConditionalProcessor}
     * and {@link OnFailureProcessor}, respectively), so we unwrap these processors internally in order to expose the underlying
     * 'actual' processor via the metrics. This corresponds best to the customer intent -- e.g. they used a 'set' processor that has an
     * 'on_failure', so we report metrics for the set processor, not an on_failure processor.
     *
     * @param compoundProcessor The compound processor to start walking the non-failure processors
     * @param processorMetrics The list to populate with {@link Processor} {@link IngestMetric} tuples.
     */
    private static void collectProcessorMetrics(
        CompoundProcessor compoundProcessor,
        List<Tuple<Processor, IngestMetric>> processorMetrics
    ) {
        // only surface the top level non-failure processors, on-failure processor times will be included in the top level non-failure
        for (Tuple<Processor, IngestMetric> processorWithMetric : compoundProcessor.getProcessorsWithMetrics()) {
            Processor processor = processorWithMetric.v1();
            IngestMetric metric = processorWithMetric.v2();

            // unwrap 'if' and 'ignore_failure/on_failure' wrapping, so that we expose the underlying actual processor
            boolean unwrapped;
            do {
                unwrapped = false;
                if (processor instanceof ConditionalProcessor conditional) {
                    processor = conditional.getInnerProcessor();
                    metric = conditional.getMetric(); // prefer the conditional's metric, it only covers when the conditional was true
                    unwrapped = true;
                }
                if (processor instanceof OnFailureProcessor onFailure) {
                    processor = onFailure.getInnerProcessor();
                    metric = onFailure.getInnerMetric(); // the wrapped processor records the failure count
                    unwrapped = true;
                }
            } while (unwrapped);

            if (processor instanceof CompoundProcessor cp) {
                collectProcessorMetrics(cp, processorMetrics);
            } else {
                processorMetrics.add(new Tuple<>(processor, metric));
            }
        }
    }

    /**
     * Used in this class and externally by the {@link org.elasticsearch.action.ingest.ReservedPipelineAction}
     */
    public static class PutPipelineClusterStateUpdateTask extends PipelineClusterStateUpdateTask {
        private final PutPipelineRequest request;

        PutPipelineClusterStateUpdateTask(ProjectId projectId, ActionListener<AcknowledgedResponse> listener, PutPipelineRequest request) {
            super(projectId, listener);
            this.request = request;
        }

        /**
         * Used by {@link org.elasticsearch.action.ingest.ReservedPipelineAction}
         */
        public PutPipelineClusterStateUpdateTask(ProjectId projectId, PutPipelineRequest request) {
            this(projectId, null, request);
        }

        @Override
        public IngestMetadata execute(IngestMetadata currentIngestMetadata, Collection<IndexMetadata> allIndexMetadata) {
            BytesReference pipelineSource = request.getSource();
            if (request.getVersion() != null) {
                var currentPipeline = currentIngestMetadata != null ? currentIngestMetadata.getPipelines().get(request.getId()) : null;
                if (currentPipeline == null) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "version conflict, required version [%s] for pipeline [%s] but no pipeline was found",
                            request.getVersion(),
                            request.getId()
                        )
                    );
                }

                final Integer currentVersion = currentPipeline.getVersion();
                if (Objects.equals(request.getVersion(), currentVersion) == false) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "version conflict, required version [%s] for pipeline [%s] but current version is [%s]",
                            request.getVersion(),
                            request.getId(),
                            currentVersion
                        )
                    );
                }

                var pipelineConfig = XContentHelper.convertToMap(request.getSource(), false, request.getXContentType()).v2();
                final Integer specifiedVersion = (Integer) pipelineConfig.get("version");
                if (pipelineConfig.containsKey("version") && Objects.equals(specifiedVersion, currentVersion)) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "cannot update pipeline [%s] with the same version [%s]",
                            request.getId(),
                            request.getVersion()
                        )
                    );
                }

                // if no version specified in the pipeline definition, inject a version of [request.getVersion() + 1]
                if (specifiedVersion == null) {
                    pipelineConfig.put("version", request.getVersion() == null ? 1 : request.getVersion() + 1);
                    try {
                        var builder = XContentBuilder.builder(request.getXContentType().xContent()).map(pipelineConfig);
                        pipelineSource = BytesReference.bytes(builder);
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                }
            }

            Map<String, PipelineConfiguration> pipelines;
            if (currentIngestMetadata != null) {
                pipelines = new HashMap<>(currentIngestMetadata.getPipelines());
            } else {
                pipelines = new HashMap<>();
            }

            pipelines.put(request.getId(), new PipelineConfiguration(request.getId(), pipelineSource, request.getXContentType()));
            return new IngestMetadata(pipelines);
        }
    }

    @UpdateForV10(owner = DATA_MANAGEMENT) // Change deprecation log for special characters in name to a failure
    void validatePipeline(
        Map<DiscoveryNode, IngestInfo> ingestInfos,
        ProjectId projectId,
        String pipelineId,
        Map<String, Object> pipelineConfig
    ) throws Exception {
        if (ingestInfos.isEmpty()) {
            throw new IllegalStateException("Ingest info is empty");
        }

        try {
            MetadataCreateIndexService.validateIndexOrAliasName(
                pipelineId,
                (pipelineName, error) -> new IllegalArgumentException(
                    "Pipeline name [" + pipelineName + "] will be disallowed in a future version for the following reason: " + error
                )
            );
        } catch (IllegalArgumentException e) {
            deprecationLogger.critical(DeprecationCategory.API, "pipeline_name_special_chars", e.getMessage());
        }

        Pipeline pipeline = Pipeline.create(
            pipelineId,
            pipelineConfig,
            processorFactories,
            scriptService,
            projectId,
            (n) -> featureService.clusterHasFeature(state, n)
        );
        List<Exception> exceptions = new ArrayList<>();
        for (Processor processor : pipeline.flattenAllProcessors()) {

            // run post-construction extra validation (if any, the default implementation from the Processor interface is a no-op)
            try {
                processor.extraValidation();
            } catch (Exception e) {
                exceptions.add(e);
            }

            for (Map.Entry<DiscoveryNode, IngestInfo> entry : ingestInfos.entrySet()) {
                String type = processor.getType();
                if (entry.getValue().containsProcessor(type) == false && ConditionalProcessor.TYPE.equals(type) == false) {
                    String message = "Processor type [" + processor.getType() + "] is not installed on node [" + entry.getKey() + "]";
                    exceptions.add(ConfigurationUtils.newConfigurationException(processor.getType(), processor.getTag(), null, message));
                }
            }
        }
        ExceptionsHelper.rethrowAndSuppress(exceptions);
    }

    private record IngestPipelinesExecutionResult(
        boolean success,
        boolean shouldKeep,
        Exception exception,
        String failedIndex,
        IndexDocFailureStoreStatus failureStoreStatus
    ) {

        private static final IngestPipelinesExecutionResult SUCCESSFUL_RESULT = new IngestPipelinesExecutionResult(
            true,
            true,
            null,
            null,
            IndexDocFailureStoreStatus.NOT_APPLICABLE_OR_UNKNOWN
        );
        private static final IngestPipelinesExecutionResult DISCARD_RESULT = new IngestPipelinesExecutionResult(
            true,
            false,
            null,
            null,
            IndexDocFailureStoreStatus.NOT_APPLICABLE_OR_UNKNOWN
        );
        private static IngestPipelinesExecutionResult failAndStoreFor(String index, Exception e) {
            return new IngestPipelinesExecutionResult(false, true, e, index, IndexDocFailureStoreStatus.USED);
        }

        private static IngestPipelinesExecutionResult failWithoutStoringIn(String index, Exception e) {
            return new IngestPipelinesExecutionResult(false, true, e, index, IndexDocFailureStoreStatus.NOT_ENABLED);
        }
    }

    /**
     * Executes all applicable pipelines for a collection of documents.
     * @param projectId The ID of the project we are indexing into.
     * @param numberOfActionRequests The total number of requests to process.
     * @param actionRequests The collection of requests to be processed.
     * @param onDropped A callback executed when a document is dropped by a pipeline.
     *                  Accepts the slot in the collection of requests that the document occupies.
     * @param resolveFailureStore A function executed on each ingest failure to determine if the
     *                           failure should be stored somewhere.
     * @param onStoreFailure A callback executed when a document fails ingest but the failure should
     *                       be persisted elsewhere. Accepts the slot in the collection of requests
     *                       that the document occupies, the index name that the request was targeting
     *                       at the time of failure, and the exception that the document encountered.
     * @param onFailure A callback executed when a document fails ingestion and does not need to be
     *                  persisted. Accepts the slot in the collection of requests that the document
     *                  occupies, and the exception that the document encountered.
     * @param onCompletion A callback executed once all documents have been processed. Accepts the thread
     *                     that ingestion completed on or an exception in the event that the entire operation
     *                     has failed.
     * @param executor Which executor the bulk request should be executed on.
     */
    public void executeBulkRequest(
        final ProjectId projectId,
        final int numberOfActionRequests,
        final Iterable<DocWriteRequest<?>> actionRequests,
        final IntConsumer onDropped,
        final Function<String, Boolean> resolveFailureStore,
        final TriConsumer<Integer, String, Exception> onStoreFailure,
        final TriConsumer<Integer, Exception, IndexDocFailureStoreStatus> onFailure,
        final BiConsumer<Thread, Exception> onCompletion,
        final Executor executor
    ) {
        assert numberOfActionRequests > 0 : "numberOfActionRequests must be greater than 0 but was [" + numberOfActionRequests + "]";

        // Adapt handler to ensure node features during ingest logic
        final Function<String, Boolean> adaptedResolveFailureStore = wrapResolverWithFeatureCheck(resolveFailureStore);

        executor.execute(new AbstractRunnable() {

            @Override
            public void onFailure(Exception e) {
                onCompletion.accept(null, e);
            }

            @Override
            protected void doRun() {
                final Thread originalThread = Thread.currentThread();
                try (var refs = new RefCountingRunnable(() -> onCompletion.accept(originalThread, null))) {
                    int i = 0;
                    for (DocWriteRequest<?> actionRequest : actionRequests) {
                        IndexRequest indexRequest = TransportBulkAction.getIndexWriteRequest(actionRequest);
                        if (indexRequest == null) {
                            i++;
                            continue;
                        }

                        PipelineIterator pipelines = getAndResetPipelines(projectId, indexRequest);
                        Pipeline firstPipeline = pipelines.peekFirst();
                        if (pipelines.hasNext() == false) {
                            i++;
                            continue;
                        }

                        // start the stopwatch and acquire a ref to indicate that we're working on this document
                        final long startTimeInNanos = System.nanoTime();
                        totalMetrics.preIngest();
                        if (firstPipeline != null) {
                            firstPipeline.getMetrics().preIngestBytes(indexRequest.ramBytesUsed());
                        }
                        final int slot = i;
                        final Releasable ref = refs.acquire();
                        final IngestDocument ingestDocument = newIngestDocument(indexRequest);
                        final org.elasticsearch.script.Metadata originalDocumentMetadata = ingestDocument.getMetadata().clone();
                        // the document listener gives us three-way logic: a document can fail processing (1), or it can
                        // be successfully processed. a successfully processed document can be kept (2) or dropped (3).
                        final ActionListener<IngestPipelinesExecutionResult> documentListener = ActionListener.runAfter(
                            new ActionListener<>() {
                                @Override
                                public void onResponse(IngestPipelinesExecutionResult result) {
                                    assert result != null;
                                    if (result.success) {
                                        if (result.shouldKeep == false) {
                                            onDropped.accept(slot);
                                        } else {
                                            assert firstPipeline != null;
                                            firstPipeline.getMetrics().postIngestBytes(indexRequest.ramBytesUsed());
                                        }
                                    } else {
                                        totalMetrics.ingestFailed();
                                        if (IndexDocFailureStoreStatus.NOT_ENABLED.equals(result.failureStoreStatus)) {
                                            // A failure result, but despite the target being a data stream, it does not have failure
                                            // storage enabled currently. Capture the status in the onFailure call and skip any further
                                            // processing
                                            onFailure.apply(slot, result.exception, result.failureStoreStatus);
                                        } else {
                                            // We were given a failure result in the onResponse method, so we must store the failure
                                            // Recover the original document state, track a failed ingest, and pass it along
                                            updateIndexRequestMetadata(indexRequest, originalDocumentMetadata);
                                            onStoreFailure.apply(slot, result.failedIndex, result.exception);
                                        }
                                    }
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    // The target of the request does not allow failure storage, or failed for unforeseen reason
                                    totalMetrics.ingestFailed();
                                    onFailure.apply(slot, e, IndexDocFailureStoreStatus.NOT_APPLICABLE_OR_UNKNOWN);
                                }
                            },
                            () -> {
                                // regardless of success or failure, we always stop the ingest "stopwatch" and release the ref to indicate
                                // that we're finished with this document
                                final long ingestTimeInNanos = System.nanoTime() - startTimeInNanos;
                                totalMetrics.postIngest(ingestTimeInNanos);
                                ref.close();
                            }
                        );

                        executePipelines(pipelines, indexRequest, ingestDocument, adaptedResolveFailureStore, documentListener);
                        assert actionRequest.index() != null;

                        i++;
                    }
                }
            }
        });
    }

    /**
     * Adapts failure store resolver function so that if the failure store node feature is not present on every node it reverts to the
     * old ingest behavior.
     * @param resolveFailureStore Function that surfaces if failures for an index should be redirected to failure store.
     * @return An adapted function that mutes the original if the cluster does not have the node feature universally applied.
     */
    private Function<String, Boolean> wrapResolverWithFeatureCheck(Function<String, Boolean> resolveFailureStore) {
        final boolean clusterHasFailureStoreFeature = featureService.clusterHasFeature(
            clusterService.state(),
            DataStream.DATA_STREAM_FAILURE_STORE_FEATURE
        );
        return (indexName) -> {
            if (clusterHasFailureStoreFeature) {
                return resolveFailureStore.apply(indexName);
            } else {
                // If we get a non-null result but the cluster is not yet fully updated with required node features,
                // force the result null to maintain old logic until all nodes are updated
                return null;
            }
        };
    }

    /**
     * Returns the pipelines of the request, and updates the request so that it no longer references
     * any pipelines (both the default and final pipeline are set to the noop pipeline).
     */
    private PipelineIterator getAndResetPipelines(ProjectId projectId, IndexRequest indexRequest) {
        final String pipelineId = indexRequest.getPipeline();
        indexRequest.setPipeline(NOOP_PIPELINE_NAME);
        final String finalPipelineId = indexRequest.getFinalPipeline();
        indexRequest.setFinalPipeline(NOOP_PIPELINE_NAME);
        return new PipelineIterator(projectId, pipelineId, finalPipelineId);
    }

    /**
     * A triple for tracking the non-null id of a pipeline, the pipeline itself, and whether the pipeline is a final pipeline.
     *
     * @param id the non-null id of the pipeline
     * @param pipeline a possibly-null reference to the pipeline for the given pipeline id
     * @param isFinal true if the pipeline is a final pipeline
     */
    private record PipelineSlot(String id, @Nullable Pipeline pipeline, boolean isFinal) {
        public PipelineSlot {
            Objects.requireNonNull(id);
        }
    }

    private class PipelineIterator implements Iterator<PipelineSlot> {

        private final ProjectId projectId;
        private final String defaultPipeline;
        private final String finalPipeline;
        private final Iterator<PipelineSlot> pipelineSlotIterator;

        private PipelineIterator(ProjectId projectId, String defaultPipeline, String finalPipeline) {
            this.projectId = projectId;
            this.defaultPipeline = NOOP_PIPELINE_NAME.equals(defaultPipeline) ? null : defaultPipeline;
            this.finalPipeline = NOOP_PIPELINE_NAME.equals(finalPipeline) ? null : finalPipeline;
            this.pipelineSlotIterator = iterator();
        }

        private PipelineIterator withoutDefaultPipeline() {
            return new PipelineIterator(projectId, null, finalPipeline);
        }

        private Iterator<PipelineSlot> iterator() {
            PipelineSlot defaultPipelineSlot = null, finalPipelineSlot = null;
            if (defaultPipeline != null) {
                defaultPipelineSlot = new PipelineSlot(defaultPipeline, getPipeline(projectId, defaultPipeline), false);
            }
            if (finalPipeline != null) {
                finalPipelineSlot = new PipelineSlot(finalPipeline, getPipeline(projectId, finalPipeline), true);
            }

            if (defaultPipeline != null && finalPipeline != null) {
                return List.of(defaultPipelineSlot, finalPipelineSlot).iterator();
            } else if (finalPipeline != null) {
                return List.of(finalPipelineSlot).iterator();
            } else if (defaultPipeline != null) {
                return List.of(defaultPipelineSlot).iterator();
            } else {
                return Collections.emptyIterator();
            }
        }

        @Override
        public boolean hasNext() {
            return pipelineSlotIterator.hasNext();
        }

        @Override
        public PipelineSlot next() {
            return pipelineSlotIterator.next();
        }

        private Pipeline peekFirst() {
            return getPipeline(projectId, defaultPipeline != null ? defaultPipeline : finalPipeline);
        }

        private ProjectId projectId() {
            return projectId;
        }
    }

    private void executePipelines(
        final PipelineIterator pipelines,
        final IndexRequest indexRequest,
        final IngestDocument ingestDocument,
        final Function<String, Boolean> resolveFailureStore,
        final ActionListener<IngestPipelinesExecutionResult> listener
    ) {
        assert pipelines.hasNext();
        PipelineSlot slot = pipelines.next();
        final String pipelineId = slot.id();
        final Pipeline pipeline = slot.pipeline();
        final boolean isFinalPipeline = slot.isFinal();

        // reset the reroute flag, at the start of a new pipeline execution this document hasn't been rerouted yet
        ingestDocument.resetReroute();
        final String originalIndex = indexRequest.indices()[0];
        final Consumer<Exception> exceptionHandler = (Exception e) -> {
            String errorType = ElasticsearchException.getExceptionName(ExceptionsHelper.unwrapCause(e));
            // If `failureStoreResolution` is true, we store the failure. If it's false, the target is a data stream,
            // but it doesn't have the failure store enabled. If it's null, the target wasn't a data stream.
            Boolean failureStoreResolution = resolveFailureStore.apply(originalIndex);
            if (failureStoreResolution != null && failureStoreResolution) {
                failureStoreMetrics.incrementFailureStore(originalIndex, errorType, FailureStoreMetrics.ErrorLocation.PIPELINE);
                listener.onResponse(IngestPipelinesExecutionResult.failAndStoreFor(originalIndex, e));
            } else if (failureStoreResolution != null) {
                // If this document targeted a data stream that didn't have the failure store enabled, we increment
                // the rejected counter.
                // We also increment the total counter because this request will not reach the code that increments
                // the total counter for non-rejected documents.
                failureStoreMetrics.incrementTotal(originalIndex);
                failureStoreMetrics.incrementRejected(originalIndex, errorType, FailureStoreMetrics.ErrorLocation.PIPELINE, false);
                listener.onResponse(IngestPipelinesExecutionResult.failWithoutStoringIn(originalIndex, e));
            } else {
                listener.onFailure(e);
            }
        };

        try {
            if (pipeline == null) {
                throw new IllegalArgumentException("pipeline with id [" + pipelineId + "] does not exist");
            }
            final var project = state.metadata().projects().get(pipelines.projectId());
            if (project == null) {
                throw new IllegalArgumentException("project with id [" + pipelines.projectId() + "] does not exist");
            }
            indexRequest.addPipeline(pipelineId);
            executePipeline(ingestDocument, pipeline, (keep, e) -> {
                assert keep != null;

                if (e != null) {
                    logger.debug(
                        () -> format(
                            "failed to execute pipeline [%s] for document [%s/%s]",
                            pipelineId,
                            indexRequest.index(),
                            indexRequest.id()
                        ),
                        e
                    );
                    exceptionHandler.accept(e);
                    return; // document failed!
                }

                if (keep == false) {
                    // We only increment the total counter for dropped docs here, because these docs don't reach the code
                    // that ordinarily take care of that.
                    // We reuse `resolveFailureStore` here to determine whether the index request targets a data stream,
                    // because we only want to track these metrics for data streams.
                    Boolean failureStoreResolution = resolveFailureStore.apply(originalIndex);
                    if (failureStoreResolution != null) {
                        // Get index abstraction, resolving date math if it exists
                        IndexAbstraction indexAbstraction = project.getIndicesLookup()
                            .get(IndexNameExpressionResolver.resolveDateMathExpression(originalIndex, threadPool.absoluteTimeInMillis()));
                        DataStream dataStream = DataStream.resolveDataStream(indexAbstraction, project);
                        String dataStreamName = dataStream != null ? dataStream.getName() : originalIndex;
                        failureStoreMetrics.incrementTotal(dataStreamName);
                    }
                    listener.onResponse(IngestPipelinesExecutionResult.DISCARD_RESULT);
                    return; // document dropped!
                }

                // update the index request so that we can execute additional pipelines (if any), etc
                updateIndexRequestMetadata(indexRequest, ingestDocument.getMetadata());
                try {
                    // check for self-references if necessary, (i.e. if a script processor has run), and clear the bit
                    if (ingestDocument.doNoSelfReferencesCheck()) {
                        CollectionUtils.ensureNoSelfReferences(ingestDocument.getSource(), null);
                        ingestDocument.doNoSelfReferencesCheck(false);
                    }
                } catch (IllegalArgumentException ex) {
                    // An IllegalArgumentException can be thrown when an ingest processor creates a source map that is self-referencing.
                    // In that case, we catch and wrap the exception, so we can include more details
                    exceptionHandler.accept(
                        new IngestPipelineException(
                            pipelineId,
                            new IllegalArgumentException(
                                format(
                                    "Failed to generate the source document for ingest pipeline [%s] for document [%s/%s]",
                                    pipelineId,
                                    indexRequest.index(),
                                    indexRequest.id()
                                ),
                                ex
                            )
                        )
                    );
                    return; // document failed!
                }

                PipelineIterator newPipelines = pipelines;
                final String newIndex = indexRequest.indices()[0];

                if (Objects.equals(originalIndex, newIndex) == false) {
                    // final pipelines cannot change the target index (either directly or by way of a reroute)
                    if (isFinalPipeline) {
                        logger.info("Service stack: [{}]", ingestDocument.getPipelineStack());
                        exceptionHandler.accept(
                            new IngestPipelineException(
                                pipelineId,
                                new IllegalStateException(
                                    format(
                                        "final pipeline [%s] can't change the target index (from [%s] to [%s]) for document [%s]",
                                        pipelineId,
                                        originalIndex,
                                        newIndex,
                                        indexRequest.id()
                                    )
                                )
                            )
                        );
                        return; // document failed!
                    }

                    // add the index to the document's index history, and check for cycles in the visited indices
                    boolean cycle = ingestDocument.updateIndexHistory(newIndex) == false;
                    if (cycle) {
                        List<String> indexCycle = new ArrayList<>(ingestDocument.getIndexHistory());
                        indexCycle.add(newIndex);
                        exceptionHandler.accept(
                            new IngestPipelineException(
                                pipelineId,
                                new IllegalStateException(
                                    format(
                                        "index cycle detected while processing pipeline [%s] for document [%s]: %s",
                                        pipelineId,
                                        indexRequest.id(),
                                        indexCycle
                                    )
                                )
                            )
                        );
                        return; // document failed!
                    }

                    // clear the current pipeline, then re-resolve the pipelines for this request
                    indexRequest.setPipeline(null);
                    indexRequest.isPipelineResolved(false);
                    resolvePipelinesAndUpdateIndexRequest(null, indexRequest, project);
                    newPipelines = getAndResetPipelines(pipelines.projectId(), indexRequest);

                    // for backwards compatibility, when a pipeline changes the target index for a document without using the reroute
                    // mechanism, do not invoke the default pipeline of the new target index
                    if (ingestDocument.isReroute() == false) {
                        newPipelines = newPipelines.withoutDefaultPipeline();
                    }
                }

                if (newPipelines.hasNext()) {
                    executePipelines(newPipelines, indexRequest, ingestDocument, resolveFailureStore, listener);
                } else {
                    // update the index request's source and (potentially) cache the timestamp for TSDB
                    updateIndexRequestSource(indexRequest, ingestDocument);
                    cacheRawTimestamp(indexRequest, ingestDocument);
                    listener.onResponse(IngestPipelinesExecutionResult.SUCCESSFUL_RESULT); // document succeeded!
                }
            });
        } catch (Exception e) {
            logger.debug(
                () -> format("failed to execute pipeline [%s] for document [%s/%s]", pipelineId, indexRequest.index(), indexRequest.id()),
                e
            );
            exceptionHandler.accept(e); // document failed
        }
    }

    private static void executePipeline(
        final IngestDocument ingestDocument,
        final Pipeline pipeline,
        final BiConsumer<Boolean, Exception> handler
    ) {
        // adapt our {@code BiConsumer<Boolean, Exception>} handler shape to the
        // {@code BiConsumer<IngestDocument, Exception>} handler shape used internally
        // by ingest pipelines and processors
        ingestDocument.executePipeline(pipeline, (result, e) -> {
            if (e != null) {
                handler.accept(true, e);
            } else {
                handler.accept(result != null, null);
            }
        });
    }

    public IngestStats stats() {
        IngestStats.Builder statsBuilder = new IngestStats.Builder();
        statsBuilder.addTotalMetrics(totalMetrics);
        for (ProjectId projectId : pipelines.keySet()) {
            pipelines.getOrDefault(projectId, ImmutableOpenMap.of()).forEach((id, holder) -> {
                Pipeline pipeline = holder.pipeline;
                CompoundProcessor rootProcessor = pipeline.getCompoundProcessor();
                statsBuilder.addPipelineMetrics(projectId, id, pipeline.getMetrics());
                List<Tuple<Processor, IngestMetric>> processorMetrics = new ArrayList<>();
                collectProcessorMetrics(rootProcessor, processorMetrics);
                processorMetrics.forEach(t -> {
                    Processor processor = t.v1();
                    IngestMetric processorMetric = t.v2();
                    statsBuilder.addProcessorMetrics(projectId, id, getProcessorName(processor), processor.getType(), processorMetric);
                });
            });
        }
        return statsBuilder.build();
    }

    /**
     * Adds a listener that gets invoked with the current cluster state before processor factories
     * get invoked.
     * <p>
     * This is useful for components that are used by ingest processors, so that they have the opportunity to update
     * before these components get used by the ingest processor factory.
     */
    public void addIngestClusterStateListener(Consumer<ClusterState> listener) {
        ingestClusterStateListeners.add(listener);
    }

    // package private for testing
    static String getProcessorName(Processor processor) {
        // conditionals are implemented as wrappers around the real processor, so get the real processor for the correct type for the name
        if (processor instanceof ConditionalProcessor conditionalProcessor) {
            processor = conditionalProcessor.getInnerProcessor();
        }

        String tag = processor.getTag();
        if (tag != null && tag.isEmpty()) {
            tag = null; // it simplifies the rest of the logic slightly to coalesce to null
        }

        String pipelineName = null;
        if (processor instanceof PipelineProcessor pipelineProcessor) {
            pipelineName = pipelineProcessor.getPipelineTemplate().newInstance(Map.of()).execute();
        }

        // if there's a tag, OR if it's a pipeline processor, then the processor name is a compound thing,
        // BUT if neither of those apply, then it's just the type -- so we can return the type itself without
        // allocating a new String object
        if (tag == null && pipelineName == null) {
            return processor.getType();
        } else {
            StringBuilder sb = new StringBuilder(5);
            sb.append(processor.getType());
            if (pipelineName != null) {
                sb.append(":");
                sb.append(pipelineName);
            }
            if (tag != null) {
                sb.append(":");
                sb.append(tag);
            }
            return sb.toString();
        }
    }

    /**
     * Builds a new ingest document from the passed-in index request.
     */
    private static IngestDocument newIngestDocument(final IndexRequest request) {
        return new IngestDocument(
            request.index(),
            request.id(),
            request.version(),
            request.routing(),
            request.versionType(),
            request.sourceAsMap(XContentParserDecorator.NOOP)
        );
    }

    /**
     * Updates an index request based on the metadata of an ingest document.
     */
    private static void updateIndexRequestMetadata(final IndexRequest request, final org.elasticsearch.script.Metadata metadata) {
        // it's fine to set all metadata fields all the time, as ingest document holds their starting values
        // before ingestion, which might also get modified during ingestion.
        request.index(metadata.getIndex());
        request.id(metadata.getId());
        request.routing(metadata.getRouting());
        request.version(metadata.getVersion());
        String versionType;
        if ((versionType = metadata.getVersionType()) != null) {
            request.versionType(VersionType.fromString(versionType));
        }
        Number number;
        if ((number = metadata.getIfSeqNo()) != null) {
            request.setIfSeqNo(number.longValue());
        }
        if ((number = metadata.getIfPrimaryTerm()) != null) {
            request.setIfPrimaryTerm(number.longValue());
        }
        Map<String, String> map;
        if ((map = metadata.getDynamicTemplates()) != null) {
            Map<String, String> mergedDynamicTemplates = new HashMap<>(request.getDynamicTemplates());
            mergedDynamicTemplates.putAll(map);
            request.setDynamicTemplates(mergedDynamicTemplates);
        }
    }

    /**
     * Updates an index request based on the source of an ingest document, guarding against self-references if necessary.
     */
    private static void updateIndexRequestSource(final IndexRequest request, final IngestDocument document) {
        boolean ensureNoSelfReferences = document.doNoSelfReferencesCheck();
        // we already check for self references elsewhere (and clear the bit), so this should always be false,
        // keeping the check and assert as a guard against extraordinarily surprising circumstances
        assert ensureNoSelfReferences == false;
        request.source(document.getSource(), request.getContentType(), ensureNoSelfReferences);
    }

    /**
     * Grab the @timestamp and store it on the index request so that TSDB can use it without needing to parse
     * the source for this document.
     */
    private static void cacheRawTimestamp(final IndexRequest request, final IngestDocument document) {
        if (request.getRawTimestamp() == null) {
            // cache the @timestamp from the ingest document's source map if there is one
            Object rawTimestamp = document.getSource().get(DataStream.TIMESTAMP_FIELD_NAME);
            if (rawTimestamp != null) {
                request.setRawTimestamp(rawTimestamp);
            }
        }
    }

    @Override
    public void applyClusterState(final ClusterChangedEvent event) {
        state = event.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        // Publish cluster state to components that are used by processor factories before letting
        // processor factories create new processor instances.
        // (Note that this needs to be done also in the case when there is no change to ingest metadata, because in the case
        // when only the part of the cluster state that a component is interested in, is updated.)
        ingestClusterStateListeners.forEach(consumer -> consumer.accept(state));

        for (ProjectMetadata project : state.getMetadata().projects().values()) {
            IngestMetadata newIngestMetadata = project.custom(IngestMetadata.TYPE);
            if (newIngestMetadata == null) {
                continue;
            }

            try {
                innerUpdatePipelines(project.id(), newIngestMetadata);
            } catch (ElasticsearchParseException e) {
                logger.warn("failed to update ingest pipelines", e);
            }
        }
    }

    synchronized void innerUpdatePipelines(ProjectId projectId, IngestMetadata newIngestMetadata) {
        Map<String, PipelineHolder> existingPipelines = this.pipelines.getOrDefault(projectId, ImmutableOpenMap.of());

        // Lazy initialize these variables in order to favour the most like scenario that there are no pipeline changes:
        ImmutableOpenMap.Builder<String, PipelineHolder> newPipelines = null;
        List<ElasticsearchParseException> exceptions = null;
        // Iterate over pipeline configurations in ingest metadata and constructs a new pipeline if there is no pipeline
        // or the pipeline configuration has been modified
        for (PipelineConfiguration newConfiguration : newIngestMetadata.getPipelines().values()) {
            PipelineHolder previous = existingPipelines.get(newConfiguration.getId());
            if (previous != null && previous.configuration.equals(newConfiguration)) {
                continue;
            }

            if (newPipelines == null) {
                newPipelines = ImmutableOpenMap.builder(existingPipelines);
            }
            try {
                Pipeline newPipeline = Pipeline.create(
                    newConfiguration.getId(),
                    newConfiguration.getConfig(false),
                    processorFactories,
                    scriptService,
                    projectId,
                    (nodeFeature) -> featureService.clusterHasFeature(clusterService.state(), nodeFeature)
                );
                newPipelines.put(newConfiguration.getId(), new PipelineHolder(newConfiguration, newPipeline));

                if (previous == null) {
                    continue;
                }
                Pipeline oldPipeline = previous.pipeline;
                newPipeline.getMetrics().add(oldPipeline.getMetrics());
                List<Tuple<Processor, IngestMetric>> oldPerProcessMetrics = new ArrayList<>();
                List<Tuple<Processor, IngestMetric>> newPerProcessMetrics = new ArrayList<>();
                collectProcessorMetrics(oldPipeline.getCompoundProcessor(), oldPerProcessMetrics);
                collectProcessorMetrics(newPipeline.getCompoundProcessor(), newPerProcessMetrics);
                // Best attempt to populate new processor metrics using a parallel array of the old metrics. This is not ideal since
                // the per processor metrics may get reset when the arrays don't match. However, to get to an ideal model, unique and
                // consistent id's per processor and/or semantic equals for each processor will be needed.
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
                    "Error updating pipeline with id [" + newConfiguration.getId() + "]",
                    e
                );
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
                    newPipelines = ImmutableOpenMap.builder(existingPipelines);
                }
                newPipelines.remove(entry.getKey());
            }
        }

        if (newPipelines != null) {
            // Update the pipelines:
            this.pipelines = ImmutableOpenMap.builder(this.pipelines).fPut(projectId, newPipelines.build()).build();

            // Rethrow errors that may have occurred during creating new pipeline instances:
            if (exceptions != null) {
                ExceptionsHelper.rethrowAndSuppress(exceptions);
            }
        }
    }

    /**
     * Gets all the Processors of the given type from within a Pipeline.
     *
     * @param projectId
     * @param pipelineId the pipeline to inspect
     * @param clazz the Processor class to look for
     * @return True if the pipeline contains an instance of the Processor class passed in
     */
    public <P extends Processor> List<P> getProcessorsInPipeline(ProjectId projectId, String pipelineId, Class<P> clazz) {
        Pipeline pipeline = getPipeline(projectId, pipelineId);
        if (pipeline == null) {
            throw new IllegalArgumentException("pipeline with id [" + pipelineId + "] does not exist in project [" + projectId + "]");
        }

        List<P> processors = new ArrayList<>();
        for (Processor processor : pipeline.flattenAllProcessors()) {
            if (clazz.isAssignableFrom(processor.getClass())) {
                processors.add(clazz.cast(processor));
            }

            while (processor instanceof WrappingProcessor wrappingProcessor) {
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

    public <P extends Processor> Collection<String> getPipelineWithProcessorType(
        ProjectId projectId,
        Class<P> clazz,
        Predicate<P> predicate
    ) {
        List<String> matchedPipelines = new LinkedList<>();
        for (PipelineHolder holder : pipelines.getOrDefault(projectId, ImmutableOpenMap.of()).values()) {
            String pipelineId = holder.pipeline.getId();
            List<P> processors = getProcessorsInPipeline(projectId, pipelineId, clazz);
            if (processors.isEmpty() == false && processors.stream().anyMatch(predicate)) {
                matchedPipelines.add(pipelineId);
            }
        }
        return matchedPipelines;
    }

    public synchronized void reloadPipeline(ProjectId projectId, String id) throws Exception {
        var originalPipelines = this.pipelines.getOrDefault(projectId, ImmutableOpenMap.of());
        PipelineHolder holder = originalPipelines.get(id);
        Pipeline updatedPipeline = Pipeline.create(
            id,
            holder.configuration.getConfig(false),
            processorFactories,
            scriptService,
            projectId,
            (nodeFeature) -> featureService.clusterHasFeature(state, nodeFeature)
        );
        ImmutableOpenMap<String, PipelineHolder> updatedPipelines = ImmutableOpenMap.builder(originalPipelines)
            .fPut(id, new PipelineHolder(holder.configuration, updatedPipeline))
            .build();
        this.pipelines = ImmutableOpenMap.builder(this.pipelines).fPut(projectId, updatedPipelines).build();
    }

    private static Pipeline substitutePipeline(String id, ElasticsearchParseException e) {
        String tag = e.getHeaderKeys().contains("processor_tag") ? e.getHeader("processor_tag").get(0) : null;
        String type = e.getHeaderKeys().contains("processor_type") ? e.getHeader("processor_type").get(0) : "unknown";
        String errorMessage = "pipeline with id [" + id + "] could not be loaded, caused by [" + e.getDetailedMessage() + "]";
        Processor failureProcessor = new AbstractProcessor(tag, "this is a placeholder processor") {
            @Override
            public IngestDocument execute(IngestDocument ingestDocument) {
                throw new IllegalStateException(errorMessage);
            }

            @Override
            public String getType() {
                return type;
            }
        };
        String description = "this is a place holder pipeline, because pipeline with id [" + id + "] could not be loaded";
        return new Pipeline(id, description, null, null, new CompoundProcessor(failureProcessor));
    }

    record PipelineHolder(PipelineConfiguration configuration, Pipeline pipeline) {

        public PipelineHolder {
            Objects.requireNonNull(configuration);
            Objects.requireNonNull(pipeline);
        }
    }

    private static Optional<Pipelines> resolvePipelinesFromMetadata(
        DocWriteRequest<?> originalRequest,
        IndexRequest indexRequest,
        ProjectMetadata projectMetadata,
        long epochMillis
    ) {
        IndexMetadata indexMetadata = null;
        // start to look for default or final pipelines via settings found in the cluster metadata
        if (originalRequest != null) {
            indexMetadata = projectMetadata.indices()
                .get(IndexNameExpressionResolver.resolveDateMathExpression(originalRequest.index(), epochMillis));
        }
        // check the alias for the index request (this is how normal index requests are modeled)
        if (indexMetadata == null && indexRequest.index() != null) {
            IndexAbstraction indexAbstraction = projectMetadata.getIndicesLookup().get(indexRequest.index());
            if (indexAbstraction != null && indexAbstraction.getWriteIndex() != null) {
                indexMetadata = projectMetadata.index(indexAbstraction.getWriteIndex());
            }
        }
        // check the alias for the action request (this is how upserts are modeled)
        if (indexMetadata == null && originalRequest != null && originalRequest.index() != null) {
            IndexAbstraction indexAbstraction = projectMetadata.getIndicesLookup().get(originalRequest.index());
            if (indexAbstraction != null && indexAbstraction.getWriteIndex() != null) {
                indexMetadata = projectMetadata.index(indexAbstraction.getWriteIndex());
            }
        }

        if (indexMetadata == null) {
            return Optional.empty();
        }

        final Settings settings = indexMetadata.getSettings();
        return Optional.of(new Pipelines(IndexSettings.DEFAULT_PIPELINE.get(settings), IndexSettings.FINAL_PIPELINE.get(settings)));
    }

    private static Optional<Pipelines> resolvePipelinesFromIndexTemplates(IndexRequest indexRequest, ProjectMetadata projectMetadata) {
        if (indexRequest.index() == null) {
            return Optional.empty();
        }

        // the index does not exist yet (and this is a valid request), so match index
        // templates to look for pipelines in either a matching V2 template (which takes
        // precedence), or if a V2 template does not match, any V1 templates
        String v2Template = MetadataIndexTemplateService.findV2Template(projectMetadata, indexRequest.index(), false);
        if (v2Template != null) {
            final Settings settings = MetadataIndexTemplateService.resolveSettings(projectMetadata, v2Template);
            return Optional.of(new Pipelines(IndexSettings.DEFAULT_PIPELINE.get(settings), IndexSettings.FINAL_PIPELINE.get(settings)));
        }

        String defaultPipeline = null;
        String finalPipeline = null;
        List<IndexTemplateMetadata> templates = MetadataIndexTemplateService.findV1Templates(projectMetadata, indexRequest.index(), null);
        // order of templates are the highest order first
        for (final IndexTemplateMetadata template : templates) {
            final Settings settings = template.settings();

            // note: the exists/get trickiness here is because we explicitly *don't* want the default value
            // of the settings -- a non-null value would terminate the search too soon
            if (defaultPipeline == null && IndexSettings.DEFAULT_PIPELINE.exists(settings)) {
                defaultPipeline = IndexSettings.DEFAULT_PIPELINE.get(settings);
                // we can not break in case a lower-order template has a final pipeline that we need to collect
            }
            if (finalPipeline == null && IndexSettings.FINAL_PIPELINE.exists(settings)) {
                finalPipeline = IndexSettings.FINAL_PIPELINE.get(settings);
                // we can not break in case a lower-order template has a default pipeline that we need to collect
            }
            if (defaultPipeline != null && finalPipeline != null) {
                // we can break if we have already collected a default and final pipeline
                break;
            }
        }

        // having exhausted the search, if nothing was found, then use the default noop pipeline names
        defaultPipeline = Objects.requireNonNullElse(defaultPipeline, NOOP_PIPELINE_NAME);
        finalPipeline = Objects.requireNonNullElse(finalPipeline, NOOP_PIPELINE_NAME);

        return Optional.of(new Pipelines(defaultPipeline, finalPipeline));
    }

    /**
     * Checks whether an IndexRequest has at least one pipeline defined.
     * <p>
     * This method assumes that the pipelines are beforehand resolved.
     */
    public static boolean hasPipeline(IndexRequest indexRequest) {
        assert indexRequest.isPipelineResolved();
        assert indexRequest.getPipeline() != null;
        assert indexRequest.getFinalPipeline() != null;
        return NOOP_PIPELINE_NAME.equals(indexRequest.getPipeline()) == false
            || NOOP_PIPELINE_NAME.equals(indexRequest.getFinalPipeline()) == false;
    }

    public record Pipelines(String defaultPipeline, String finalPipeline) {

        private static final Pipelines NO_PIPELINES_DEFINED = new Pipelines(NOOP_PIPELINE_NAME, NOOP_PIPELINE_NAME);

        public Pipelines {
            Objects.requireNonNull(defaultPipeline);
            Objects.requireNonNull(finalPipeline);
        }
    }
}
