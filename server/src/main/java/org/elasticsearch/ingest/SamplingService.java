/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.sampling.SamplingConfiguration;
import org.elasticsearch.action.admin.indices.sampling.SamplingMetadata;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedBatchedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.SimpleBatchedAckListenerTaskExecutor;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.scheduler.TimeValueSchedule;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.script.IngestConditionalScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class SamplingService extends AbstractLifecycleComponent implements ClusterStateListener, SchedulerEngine.Listener {
    public static final boolean RANDOM_SAMPLING_FEATURE_FLAG = new FeatureFlag("random_sampling").isEnabled();
    public static final Setting<TimeValue> TTL_POLL_INTERVAL_SETTING = Setting.timeSetting(
        "random_sampling.ttl_poll_interval",
        TimeValue.timeValueMinutes(30),
        TimeValue.timeValueSeconds(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private static final Logger logger = LogManager.getLogger(SamplingService.class);
    private static final String TTL_JOB_ID = "sampling_ttl";
    private final ScriptService scriptService;
    private final ClusterService clusterService;
    private final LongSupplier statsTimeSupplier = System::nanoTime;
    private final MasterServiceTaskQueue<UpdateSamplingConfigurationTask> updateSamplingConfigurationTaskQueue;
    private final MasterServiceTaskQueue<DeleteSampleConfigurationTask> deleteSamplingConfigurationTaskQueue;

    private static final Setting<Integer> MAX_CONFIGURATIONS_SETTING = Setting.intSetting(
        "sampling.max_configurations",
        100,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    private final SetOnce<SchedulerEngine> scheduler = new SetOnce<>();
    private SchedulerEngine.Job scheduledJob;
    private volatile TimeValue pollInterval;
    private final Settings settings;
    private final Clock clock = Clock.systemUTC();
    /*
     * This Map contains the samples that exist on this node. They are not persisted to disk. They are stored as SoftReferences so that
     * sampling does not contribute to a node running out of memory. The idea is that access to samples is desirable, but not critical. We
     * make a best effort to keep them around, but do not worry about the complexity or cost of making them durable.
     */
    private final Map<ProjectIndex, SoftReference<SampleInfo>> samples = new ConcurrentHashMap<>();

    /*
     * This creates a new SamplingService, and configures various listeners on it.
     */
    public static SamplingService create(ScriptService scriptService, ClusterService clusterService, Settings settings) {
        SamplingService samplingService = new SamplingService(scriptService, clusterService, settings);
        samplingService.configureListeners();
        return samplingService;
    }

    private SamplingService(ScriptService scriptService, ClusterService clusterService, Settings settings) {
        this.scriptService = scriptService;
        this.clusterService = clusterService;
        this.updateSamplingConfigurationTaskQueue = clusterService.createTaskQueue(
            "update-sampling-configuration",
            Priority.NORMAL,
            new UpdateSamplingConfigurationExecutor()
        );
        this.deleteSamplingConfigurationTaskQueue = clusterService.createTaskQueue(
            "delete-sampling-configuration",
            Priority.NORMAL,
            new DeleteSampleConfigurationExecutor()
        );
        this.settings = settings;
        this.pollInterval = TTL_POLL_INTERVAL_SETTING.get(settings);
    }

    private void configureListeners() {
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(TTL_POLL_INTERVAL_SETTING, (v) -> {
            pollInterval = v;
            if (clusterService.state().nodes().isLocalNodeElectedMaster()) {
                maybeScheduleJob();
            }
        });
        this.addLifecycleListener(new LifecycleListener() {
            @Override
            public void afterStop() {
                cancelJob();
            }
        });
    }

    /**
     * Potentially samples the given indexRequest, depending on the existing sampling configuration.
     * @param projectMetadata Used to get the sampling configuration
     * @param indexRequest The raw request to potentially sample
     */
    public void maybeSample(ProjectMetadata projectMetadata, IndexRequest indexRequest) {
        maybeSample(projectMetadata, indexRequest.index(), indexRequest, () -> {
            /*
             * The conditional scripts used by random sampling work off of IngestDocuments, in the same way conditionals do in pipelines. In
             * this case, we did not have an IngestDocument (which happens when there are no pipelines). So we construct one with the same
             * fields as this IndexRequest for use in conditionals. It is created in this lambda to avoid the expensive sourceAsMap call
             * if the condition is never executed.
             */
            Map<String, Object> sourceAsMap;
            try {
                sourceAsMap = indexRequest.sourceAsMap();
            } catch (XContentParseException e) {
                sourceAsMap = Map.of();
                logger.trace("Invalid index request source, attempting to sample anyway");
            }
            return new IngestDocument(
                indexRequest.index(),
                indexRequest.id(),
                indexRequest.version(),
                indexRequest.routing(),
                indexRequest.versionType(),
                sourceAsMap
            );
        });
    }

    /**
     * Potentially samples the given indexRequest, depending on the existing sampling configuration. The request will be sampled against
     * the sampling configurations of all indices it has been rerouted to (if it has been rerouted).
     * @param projectMetadata Used to get the sampling configuration
     * @param indexRequest The raw request to potentially sample
     * @param ingestDocument The IngestDocument used for evaluating any conditionals that are part of the sample configuration
     */
    public void maybeSample(ProjectMetadata projectMetadata, IndexRequest indexRequest, IngestDocument ingestDocument) {
        // The index history gives us the initially-requested index, as well as any indices it has been rerouted through
        for (String index : ingestDocument.getIndexHistory()) {
            maybeSample(projectMetadata, index, indexRequest, () -> ingestDocument);
        }
    }

    private void maybeSample(
        ProjectMetadata projectMetadata,
        String indexName,
        IndexRequest indexRequest,
        Supplier<IngestDocument> ingestDocumentSupplier
    ) {
        if (RANDOM_SAMPLING_FEATURE_FLAG == false) {
            return;
        }
        long startTime = statsTimeSupplier.getAsLong();
        SoftReference<SampleInfo> sampleInfoReference = samples.compute(new ProjectIndex(projectMetadata.id(), indexName), (k, v) -> {
            if (v == null || v.get() == null) {
                SamplingConfiguration samplingConfig = getSamplingConfiguration(projectMetadata, indexName);
                if (samplingConfig == null) {
                    /*
                     * Calls to getSamplingConfiguration() are relatively expensive. So we store the NONE object here to indicate that there
                     * was no sampling configuration. This way we don't have to do the lookup every single time for every index that has no
                     * sampling configuration. If a sampling configuration is added for this index, this NONE sample will be removed by
                     * the cluster state change listener.
                     */
                    return new SoftReference<>(SampleInfo.NONE);
                }
                return new SoftReference<>(new SampleInfo(samplingConfig.maxSamples()));
            }
            return v;
        });
        SampleInfo sampleInfo = sampleInfoReference.get();
        if (sampleInfo == null || sampleInfo == SampleInfo.NONE) {
            return;
        }
        SampleStats stats = sampleInfo.stats;
        stats.potentialSamples.increment();
        try {
            if (sampleInfo.isFull) {
                stats.samplesRejectedForMaxSamplesExceeded.increment();
                return;
            }
            SamplingConfiguration samplingConfig = getSamplingConfiguration(projectMetadata, indexName);
            if (samplingConfig == null) {
                return; // it was not null above, but has since become null because the index was deleted asynchronously
            }
            if (sampleInfo.getSizeInBytes() + indexRequest.source().length() > samplingConfig.maxSize().getBytes()) {
                stats.samplesRejectedForSize.increment();
                return;
            }
            if (Math.random() >= samplingConfig.rate()) {
                stats.samplesRejectedForRate.increment();
                return;
            }
            String condition = samplingConfig.condition();
            if (condition != null) {
                if (sampleInfo.script == null || sampleInfo.factory == null) {
                    // We don't want to pay for synchronization because worst case, we compile the script twice
                    long compileScriptStartTime = statsTimeSupplier.getAsLong();
                    try {
                        if (sampleInfo.compilationFailed) {
                            // we don't want to waste time -- if the script failed to compile once it will just fail again
                            stats.samplesRejectedForException.increment();
                            return;
                        } else {
                            Script script = getScript(condition);
                            sampleInfo.setScript(script, scriptService.compile(script, IngestConditionalScript.CONTEXT));
                        }
                    } catch (Exception e) {
                        sampleInfo.compilationFailed = true;
                        throw e;
                    } finally {
                        stats.timeCompilingConditionInNanos.add((statsTimeSupplier.getAsLong() - compileScriptStartTime));
                    }
                }
            }
            if (condition != null
                && evaluateCondition(ingestDocumentSupplier, sampleInfo.script, sampleInfo.factory, sampleInfo.stats) == false) {
                stats.samplesRejectedForCondition.increment();
                return;
            }
            RawDocument sample = getRawDocumentForIndexRequest(indexName, indexRequest);
            if (sampleInfo.offer(sample)) {
                stats.samples.increment();
                logger.trace("Sampling " + indexRequest);
            } else {
                stats.samplesRejectedForMaxSamplesExceeded.increment();
            }
        } catch (Exception e) {
            stats.samplesRejectedForException.increment();
            /*
             * We potentially overwrite a previous exception here. But the thinking is that the user will pretty rapidly iterate on
             * exceptions as they come up, and this avoids the overhead and complexity of keeping track of multiple exceptions.
             */
            stats.lastException = e;
            logger.debug("Error performing sampling for " + indexName, e);
        } finally {
            stats.timeSamplingInNanos.add((statsTimeSupplier.getAsLong() - startTime));
        }
    }

    /**
     * Retrieves the sampling configuration for the specified index from the given project metadata.
     *
     * @param projectMetadata The project metadata containing sampling information.
     * @param indexName The name of the index or data stream for which to retrieve the sampling configuration.
     * @return The {@link SamplingConfiguration} for the specified index, or {@code null} if none exists.
     */
    public SamplingConfiguration getSamplingConfiguration(ProjectMetadata projectMetadata, String indexName) {
        SamplingMetadata samplingMetadata = projectMetadata.custom(SamplingMetadata.TYPE);
        if (samplingMetadata == null) {
            return null;
        }
        return samplingMetadata.getIndexToSamplingConfigMap().get(indexName);
    }

    /**
     * Gets the sample for the given projectId and index on this node only. The sample is not persistent.
     * @param projectId The project that this sample is for
     * @param index The index that the sample is for
     * @return The raw documents in the sample on this node, or an empty list if there are none
     */
    public List<RawDocument> getLocalSample(ProjectId projectId, String index) {
        SoftReference<SampleInfo> sampleInfoReference = samples.get(new ProjectIndex(projectId, index));
        SampleInfo sampleInfo = sampleInfoReference == null ? null : sampleInfoReference.get();
        return sampleInfo == null ? List.of() : Arrays.stream(sampleInfo.getRawDocuments()).filter(Objects::nonNull).toList();
    }

    /**
     * Gets the sample stats for the given projectId and index on this node only. The stats are not persistent. They are reset when the
     * node restarts for example.
     * @param projectId The project that this sample is for
     * @param index The index that the sample is for
     * @return Current stats on this node for this sample
     */
    public SampleStats getLocalSampleStats(ProjectId projectId, String index) {
        SoftReference<SampleInfo> sampleInfoReference = samples.get(new ProjectIndex(projectId, index));
        if (sampleInfoReference == null) {
            return new SampleStats();
        }
        SampleInfo sampleInfo = sampleInfoReference.get();
        return sampleInfo == null ? new SampleStats() : sampleInfo.stats;
    }

    /*
     * Throws an IndexNotFoundException if the first index in the IndicesRequest is not a data stream or a single index that exists
     */
    public static void throwIndexNotFoundExceptionIfNotDataStreamOrIndex(
        IndexNameExpressionResolver indexNameExpressionResolver,
        ProjectResolver projectResolver,
        ClusterState state,
        IndicesRequest request
    ) {
        assert request.indices().length == 1 : "Expected IndicesRequest to have a single index but found " + request.indices().length;
        assert request.includeDataStreams() : "Expected IndicesRequest to include data streams but it did not";
        boolean isDataStream = projectResolver.getProjectMetadata(state).dataStreams().containsKey(request.indices()[0]);
        if (isDataStream == false) {
            indexNameExpressionResolver.concreteIndexNames(state, request);
        }
    }

    public boolean atLeastOneSampleConfigured(ProjectMetadata projectMetadata) {
        if (RANDOM_SAMPLING_FEATURE_FLAG) {
            SamplingMetadata samplingMetadata = projectMetadata.custom(SamplingMetadata.TYPE);
            return samplingMetadata != null && samplingMetadata.getIndexToSamplingConfigMap().isEmpty() == false;
        } else {
            return false;
        }
    }

    public void updateSampleConfiguration(
        ProjectId projectId,
        String index,
        SamplingConfiguration samplingConfiguration,
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        ActionListener<AcknowledgedResponse> listener
    ) {
        // Early validation: check if adding a new configuration would exceed the limit
        ClusterState clusterState = clusterService.state();
        boolean maxConfigLimitBreached = checkMaxConfigLimitBreached(projectId, index, clusterState);
        if (maxConfigLimitBreached) {
            Integer maxConfigurations = MAX_CONFIGURATIONS_SETTING.get(clusterState.getMetadata().settings());
            listener.onFailure(
                new IllegalStateException(
                    "Cannot add sampling configuration for index ["
                        + index
                        + "]. Maximum number of sampling configurations ("
                        + maxConfigurations
                        + ") already reached."
                )
            );
            return;
        }

        updateSamplingConfigurationTaskQueue.submitTask(
            "Updating Sampling Configuration",
            new UpdateSamplingConfigurationTask(projectId, index, samplingConfiguration, ackTimeout, listener),
            masterNodeTimeout
        );
    }

    public void deleteSampleConfiguration(
        ProjectId projectId,
        String index,
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        ActionListener<AcknowledgedResponse> listener
    ) {
        deleteSamplingConfigurationTaskQueue.submitTask(
            "deleting sampling configuration for index " + index,
            new DeleteSampleConfigurationTask(projectId, index, ackTimeout, listener),
            masterNodeTimeout
        );
    }

    /*
     * This version is meant to be used by background processes, not user requests.
     */
    private void deleteSampleConfiguration(ProjectId projectId, String index) {
        deleteSampleConfiguration(projectId, index, TimeValue.MAX_VALUE, TimeValue.MAX_VALUE, ActionListener.wrap(response -> {
            if (response.isAcknowledged()) {
                logger.debug("Deleted sampling configuration for {}", index);
            } else {
                logger.warn("Deletion of sampling configuration for {} not acknowledged", index);
            }
        }, e -> logger.warn("Failed to delete sample configuration for " + index, e)));
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (RANDOM_SAMPLING_FEATURE_FLAG == false) {
            return;
        }
        final boolean isMaster = event.localNodeMaster();
        final boolean wasMaster = event.previousState().nodes().isLocalNodeElectedMaster();
        if (wasMaster != isMaster) {
            if (isMaster) {
                // we weren't the master, and now we are
                maybeScheduleJob();
            } else {
                // we were the master, and now we aren't
                cancelJob();
            }
        }
        if (isMaster == false && samples.isEmpty()) {
            /*
             * The remaining code potentially removes entries from samples, and delete configurations if this is the master. So if this is
             * not the master and has no sampling configurations, we can just bail out here.
             */
            return;
        }
        // We want to remove any samples if their sampling configuration has been deleted or modified.
        if (event.metadataChanged()) {
            /*
             * First, we collect the union of all project ids in the current state and the previous one. We include the project ids from the
             * previous state in case an entire project has been deleted -- in that case we would want to delete all of its samples.
             */
            Set<ProjectId> allProjectIds = Sets.union(
                event.state().metadata().projects().keySet(),
                event.previousState().metadata().projects().keySet()
            );
            for (ProjectId projectId : allProjectIds) {
                maybeRemoveStaleSamples(event, projectId);
                // Now delete configurations for any indices that have been deleted:
                if (isMaster) {
                    maybeDeleteSamplingConfigurations(event, projectId);
                }
            }
        }
    }

    /*
     * This method removes any samples from the samples Map that have had their sampling configuration removed or changed in this event.
     */
    private void maybeRemoveStaleSamples(ClusterChangedEvent event, ProjectId projectId) {
        if (samples.isEmpty() == false && event.customMetadataChanged(projectId, SamplingMetadata.TYPE)) {
            Map<String, SamplingConfiguration> oldSampleConfigsMap = Optional.ofNullable(
                event.previousState().metadata().projects().get(projectId)
            )
                .map(p -> (SamplingMetadata) p.custom(SamplingMetadata.TYPE))
                .map(SamplingMetadata::getIndexToSamplingConfigMap)
                .orElse(Map.of());
            Map<String, SamplingConfiguration> newSampleConfigsMap = Optional.ofNullable(event.state().metadata().projects().get(projectId))
                .map(p -> (SamplingMetadata) p.custom(SamplingMetadata.TYPE))
                .map(SamplingMetadata::getIndexToSamplingConfigMap)
                .orElse(Map.of());
            Set<String> indicesWithRemovedConfigs = new HashSet<>(oldSampleConfigsMap.keySet());
            indicesWithRemovedConfigs.removeAll(newSampleConfigsMap.keySet());
            /*
             * These index names no longer have sampling configurations associated with them. So we remove their samples. We are OK
             * with the fact that we have a race condition here -- it is possible that in maybeSample() the configuration still
             * exists but before the sample is read from samples it is deleted by this method and gets recreated. In the worst case
             * we'll have a small amount of memory being used until the sampling configuration is recreated or the TTL checker
             * reclaims it. The advantage is that we can avoid locking here, which could slow down ingestion.
             */
            for (String indexName : indicesWithRemovedConfigs) {
                logger.debug("Removing sample info for {} because its configuration has been removed", indexName);
                samples.remove(new ProjectIndex(projectId, indexName));
            }
            /*
             * Now we check if any of the sampling configurations have changed. If they have, we remove the existing sample. Same as
             * above, we have a race condition here that we can live with.
             */
            for (Map.Entry<String, SamplingConfiguration> entry : newSampleConfigsMap.entrySet()) {
                String indexName = entry.getKey();
                if (oldSampleConfigsMap.containsKey(indexName) && entry.getValue().equals(oldSampleConfigsMap.get(indexName)) == false) {
                    logger.debug("Removing sample info for {} because its configuration has changed", indexName);
                    samples.remove(new ProjectIndex(projectId, indexName));
                } else if (oldSampleConfigsMap.containsKey(indexName) == false
                    && samples.containsKey(new ProjectIndex(projectId, indexName))) {
                        // There had previously been a NONE sample here. There is a real config now, so delete the NONE sample
                        logger.debug("Removing sample info for {} because its configuration has been created", indexName);
                        samples.remove(new ProjectIndex(projectId, indexName));
                    }
            }
        }
    }

    /*
     * This method deletes the sampling configuration for any index that has been deleted in this event.
     */
    private void maybeDeleteSamplingConfigurations(ClusterChangedEvent event, ProjectId projectId) {
        ProjectMetadata currentProject = event.state().metadata().projects().get(projectId);
        ProjectMetadata previousProject = event.previousState().metadata().projects().get(projectId);
        if (currentProject == null || previousProject == null) {
            return;
        }
        if (currentProject.indices() != previousProject.indices()) {
            for (IndexMetadata index : previousProject.indices().values()) {
                IndexMetadata current = currentProject.index(index.getIndex());
                if (current == null) {
                    String indexName = index.getIndex().getName();
                    SamplingConfiguration samplingConfiguration = getSamplingConfiguration(
                        event.state().projectState(projectId).metadata(),
                        indexName
                    );
                    if (samplingConfiguration != null) {
                        logger.debug("Deleting sample configuration for {} because the index has been deleted", indexName);
                        deleteSampleConfiguration(projectId, indexName);
                    }
                }
            }
        }
        if (currentProject.dataStreams() != previousProject.dataStreams()) {
            for (DataStream dataStream : previousProject.dataStreams().values()) {
                DataStream current = currentProject.dataStreams().get(dataStream.getName());
                if (current == null) {
                    String dataStreamName = dataStream.getName();
                    SamplingConfiguration samplingConfiguration = getSamplingConfiguration(
                        event.state().projectState(projectId).metadata(),
                        dataStreamName
                    );
                    if (samplingConfiguration != null) {
                        logger.debug("Deleting sample configuration for {} because the data stream has been deleted", dataStreamName);
                        deleteSampleConfiguration(projectId, dataStreamName);
                    }
                }
            }
        }
    }

    private void maybeScheduleJob() {
        if (isClusterServiceStoppedOrClosed()) {
            // don't create scheduler if the node is shutting down
            return;
        }
        if (scheduler.get() == null) {
            scheduler.set(new SchedulerEngine(settings, clock));
            scheduler.get().register(this);
        }
        scheduledJob = new SchedulerEngine.Job(TTL_JOB_ID, new TimeValueSchedule(pollInterval));
        scheduler.get().add(scheduledJob);
    }

    private void cancelJob() {
        if (scheduler.get() != null) {
            scheduler.get().remove(TTL_JOB_ID);
            scheduledJob = null;
        }
    }

    private boolean isClusterServiceStoppedOrClosed() {
        final Lifecycle.State state = clusterService.lifecycleState();
        return state == Lifecycle.State.STOPPED || state == Lifecycle.State.CLOSED;
    }

    private boolean evaluateCondition(
        Supplier<IngestDocument> ingestDocumentSupplier,
        Script script,
        IngestConditionalScript.Factory factory,
        SampleStats stats
    ) {
        long conditionStartTime = statsTimeSupplier.getAsLong();
        boolean passedCondition = factory.newInstance(script.getParams(), ingestDocumentSupplier.get().getUnmodifiableSourceAndMetadata())
            .execute();
        stats.timeEvaluatingConditionInNanos.add((statsTimeSupplier.getAsLong() - conditionStartTime));
        return passedCondition;
    }

    private static Script getScript(String conditional) throws IOException {
        logger.debug("Parsing script for conditional " + conditional);
        try (
            XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent).map(Map.of("source", conditional));
            XContentParser parser = XContentHelper.createParserNotCompressed(
                LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG,
                BytesReference.bytes(builder),
                XContentType.JSON
            )
        ) {
            return Script.parse(parser);
        }
    }

    // Checks whether the maximum number of sampling configurations has been reached for the given project.
    // If the limit is breached, it notifies the listener with an IllegalStateException and returns true.
    private static boolean checkMaxConfigLimitBreached(ProjectId projectId, String index, ClusterState currentState) {
        Metadata currentMetadata = currentState.metadata();
        ProjectMetadata projectMetadata = currentMetadata.getProject(projectId);

        if (projectMetadata != null) {
            SamplingMetadata samplingMetadata = projectMetadata.custom(SamplingMetadata.TYPE);
            Map<String, SamplingConfiguration> existingConfigs = samplingMetadata != null
                ? samplingMetadata.getIndexToSamplingConfigMap()
                : Map.of();

            boolean isUpdate = existingConfigs.containsKey(index);
            Integer maxConfigurations = MAX_CONFIGURATIONS_SETTING.get(currentMetadata.settings());

            // Only check limit for new configurations, not updates
            if (isUpdate == false && existingConfigs.size() >= maxConfigurations) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        logger.debug("job triggered: {}, {}, {}", event.jobName(), event.scheduledTime(), event.triggeredTime());
        checkTTLs();
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
        logger.debug("Sampling service is stopping.");
    }

    @Override
    protected void doClose() throws IOException {
        logger.debug("Sampling service is closing.");
        SchedulerEngine engine = scheduler.get();
        if (engine != null) {
            engine.stop();
        }
    }

    private void checkTTLs() {
        long now = clock.instant().toEpochMilli();
        for (ProjectMetadata projectMetadata : clusterService.state().metadata().projects().values()) {
            SamplingMetadata samplingMetadata = projectMetadata.custom(SamplingMetadata.TYPE);
            if (samplingMetadata != null) {
                for (Map.Entry<String, SamplingConfiguration> entry : samplingMetadata.getIndexToSamplingConfigMap().entrySet()) {
                    SamplingConfiguration samplingConfiguration = entry.getValue();
                    if (samplingConfiguration.creationTime() + samplingConfiguration.timeToLive().millis() < now) {
                        String indexName = entry.getKey();
                        logger.debug(
                            "Deleting configuration for {} created at {} because it is older than {} now at {}",
                            indexName,
                            ZonedDateTime.ofInstant(Instant.ofEpochMilli(samplingConfiguration.creationTime()), ZoneOffset.UTC),
                            samplingConfiguration.timeToLive(),
                            ZonedDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneOffset.UTC)
                        );
                        deleteSampleConfiguration(projectMetadata.id(), indexName);
                    }
                }
            }
        }
    }

    /*
     * This represents a raw document as the user sent it to us in an IndexRequest. It only holds onto the information needed for the
     * sampling API, rather than holding all of the fields a user might send in an IndexRequest.
     */
    public record RawDocument(String indexName, byte[] source, XContentType contentType) implements Writeable {

        public RawDocument(StreamInput in) throws IOException {
            this(in.readString(), in.readByteArray(), in.readEnum(XContentType.class));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(indexName);
            out.writeByteArray(source);
            XContentHelper.writeTo(out, contentType);
        }

        public long getSizeInBytes() {
            return indexName.length() + source.length;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RawDocument rawDocument = (RawDocument) o;
            return Objects.equals(indexName, rawDocument.indexName)
                && Arrays.equals(source, rawDocument.source)
                && contentType == rawDocument.contentType;
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(indexName, contentType);
            result = 31 * result + Arrays.hashCode(source);
            return result;
        }
    }

    /*
     * This creates a RawDocument from the indexRequest. The source bytes of the indexRequest are copied into the RawDocument. So the
     * RawDocument might be a relatively expensive object memory-wise. Since the bytes are copied, subsequent changes to the indexRequest
     * are not reflected in the RawDocument
     */
    private RawDocument getRawDocumentForIndexRequest(String indexName, IndexRequest indexRequest) {
        BytesReference sourceReference = indexRequest.source();
        assert sourceReference != null : "Cannot sample an IndexRequest with no source";
        byte[] source = sourceReference.array();
        final byte[] sourceCopy = new byte[sourceReference.length()];
        System.arraycopy(source, sourceReference.arrayOffset(), sourceCopy, 0, sourceReference.length());
        return new RawDocument(indexName, sourceCopy, indexRequest.getContentType());
    }

    public static final class SampleStats implements Writeable, ToXContent {
        // These are all non-private for the sake of unit testing
        final LongAdder samples = new LongAdder();
        final LongAdder potentialSamples = new LongAdder();
        final LongAdder samplesRejectedForMaxSamplesExceeded = new LongAdder();
        final LongAdder samplesRejectedForCondition = new LongAdder();
        final LongAdder samplesRejectedForRate = new LongAdder();
        final LongAdder samplesRejectedForException = new LongAdder();
        final LongAdder samplesRejectedForSize = new LongAdder();
        final LongAdder timeSamplingInNanos = new LongAdder();
        final LongAdder timeEvaluatingConditionInNanos = new LongAdder();
        final LongAdder timeCompilingConditionInNanos = new LongAdder();
        Exception lastException = null;

        public SampleStats() {}

        public SampleStats(SampleStats other) {
            addAllFields(other, this);
        }

        /*
         * This constructor is only meant for constructing arbitrary SampleStats for testing
         */
        public SampleStats(
            long samples,
            long potentialSamples,
            long samplesRejectedForMaxSamplesExceeded,
            long samplesRejectedForCondition,
            long samplesRejectedForRate,
            long samplesRejectedForException,
            long samplesRejectedForSize,
            TimeValue timeSampling,
            TimeValue timeEvaluatingCondition,
            TimeValue timeCompilingCondition,
            Exception lastException
        ) {
            this.samples.add(samples);
            this.potentialSamples.add(potentialSamples);
            this.samplesRejectedForMaxSamplesExceeded.add(samplesRejectedForMaxSamplesExceeded);
            this.samplesRejectedForCondition.add(samplesRejectedForCondition);
            this.samplesRejectedForRate.add(samplesRejectedForRate);
            this.samplesRejectedForException.add(samplesRejectedForException);
            this.samplesRejectedForSize.add(samplesRejectedForSize);
            this.timeSamplingInNanos.add(timeSampling.nanos());
            this.timeEvaluatingConditionInNanos.add(timeEvaluatingCondition.nanos());
            this.timeCompilingConditionInNanos.add(timeCompilingCondition.nanos());
            this.lastException = lastException;
        }

        public SampleStats(StreamInput in) throws IOException {
            potentialSamples.add(in.readLong());
            samplesRejectedForMaxSamplesExceeded.add(in.readLong());
            samplesRejectedForCondition.add(in.readLong());
            samplesRejectedForRate.add(in.readLong());
            samplesRejectedForException.add(in.readLong());
            samplesRejectedForSize.add(in.readLong());
            samples.add(in.readLong());
            timeSamplingInNanos.add(in.readLong());
            timeEvaluatingConditionInNanos.add(in.readLong());
            timeCompilingConditionInNanos.add(in.readLong());
            if (in.readBoolean()) {
                lastException = in.readException();
            } else {
                lastException = null;
            }
        }

        public long getSamples() {
            return samples.longValue();
        }

        public long getPotentialSamples() {
            return potentialSamples.longValue();
        }

        public long getSamplesRejectedForMaxSamplesExceeded() {
            return samplesRejectedForMaxSamplesExceeded.longValue();
        }

        public long getSamplesRejectedForCondition() {
            return samplesRejectedForCondition.longValue();
        }

        public long getSamplesRejectedForRate() {
            return samplesRejectedForRate.longValue();
        }

        public long getSamplesRejectedForException() {
            return samplesRejectedForException.longValue();
        }

        public long getSamplesRejectedForSize() {
            return samplesRejectedForSize.longValue();
        }

        public TimeValue getTimeSampling() {
            return TimeValue.timeValueNanos(timeSamplingInNanos.longValue());
        }

        public TimeValue getTimeEvaluatingCondition() {
            return TimeValue.timeValueNanos(timeEvaluatingConditionInNanos.longValue());
        }

        public TimeValue getTimeCompilingCondition() {
            return TimeValue.timeValueNanos(timeCompilingConditionInNanos.longValue());
        }

        public Exception getLastException() {
            return lastException;
        }

        @Override
        public String toString() {
            return "potential_samples: "
                + potentialSamples
                + ", samples_rejected_for_max_samples_exceeded: "
                + samplesRejectedForMaxSamplesExceeded
                + ", samples_rejected_for_condition: "
                + samplesRejectedForCondition
                + ", samples_rejected_for_rate: "
                + samplesRejectedForRate
                + ", samples_rejected_for_exception: "
                + samplesRejectedForException
                + ", samples_accepted: "
                + samples
                + ", time_sampling: "
                + TimeValue.timeValueNanos(timeSamplingInNanos.longValue())
                + ", time_evaluating_condition: "
                + TimeValue.timeValueNanos(timeEvaluatingConditionInNanos.longValue())
                + ", time_compiling_condition: "
                + TimeValue.timeValueNanos(timeCompilingConditionInNanos.longValue());
        }

        public SampleStats combine(SampleStats other) {
            SampleStats result = new SampleStats(this);
            addAllFields(other, result);
            return result;
        }

        private static void addAllFields(SampleStats source, SampleStats dest) {
            dest.potentialSamples.add(source.potentialSamples.longValue());
            dest.samplesRejectedForMaxSamplesExceeded.add(source.samplesRejectedForMaxSamplesExceeded.longValue());
            dest.samplesRejectedForCondition.add(source.samplesRejectedForCondition.longValue());
            dest.samplesRejectedForRate.add(source.samplesRejectedForRate.longValue());
            dest.samplesRejectedForException.add(source.samplesRejectedForException.longValue());
            dest.samplesRejectedForSize.add(source.samplesRejectedForSize.longValue());
            dest.samples.add(source.samples.longValue());
            dest.timeSamplingInNanos.add(source.timeSamplingInNanos.longValue());
            dest.timeEvaluatingConditionInNanos.add(source.timeEvaluatingConditionInNanos.longValue());
            dest.timeCompilingConditionInNanos.add(source.timeCompilingConditionInNanos.longValue());
            if (dest.lastException == null) {
                dest.lastException = source.lastException;
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field("potential_samples", potentialSamples.longValue());
            builder.field("samples_rejected_for_max_samples_exceeded", samplesRejectedForMaxSamplesExceeded.longValue());
            builder.field("samples_rejected_for_condition", samplesRejectedForCondition.longValue());
            builder.field("samples_rejected_for_rate", samplesRejectedForRate.longValue());
            builder.field("samples_rejected_for_exception", samplesRejectedForException.longValue());
            builder.field("samples_rejected_for_size", samplesRejectedForSize.longValue());
            builder.field("samples_accepted", samples.longValue());
            builder.humanReadableField("time_sampling_millis", "time_sampling", TimeValue.timeValueNanos(timeSamplingInNanos.longValue()));
            builder.humanReadableField(
                "time_evaluating_condition_millis",
                "time_evaluating_condition",
                TimeValue.timeValueNanos(timeEvaluatingConditionInNanos.longValue())
            );
            builder.humanReadableField(
                "time_compiling_condition_millis",
                "time_compiling_condition",
                TimeValue.timeValueNanos(timeCompilingConditionInNanos.longValue())
            );
            if (lastException != null) {
                Throwable unwrapped = ExceptionsHelper.unwrapCause(lastException);
                builder.startObject("last_exception");
                builder.field("type", ElasticsearchException.getExceptionName(unwrapped));
                builder.field("message", unwrapped.getMessage());
                builder.field("stack_trace", ExceptionsHelper.limitedStackTrace(unwrapped, 5));
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(potentialSamples.longValue());
            out.writeLong(samplesRejectedForMaxSamplesExceeded.longValue());
            out.writeLong(samplesRejectedForCondition.longValue());
            out.writeLong(samplesRejectedForRate.longValue());
            out.writeLong(samplesRejectedForException.longValue());
            out.writeLong(samplesRejectedForSize.longValue());
            out.writeLong(samples.longValue());
            out.writeLong(timeSamplingInNanos.longValue());
            out.writeLong(timeEvaluatingConditionInNanos.longValue());
            out.writeLong(timeCompilingConditionInNanos.longValue());
            if (lastException == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeException(lastException);
            }
        }

        /*
         * equals and hashCode are implemented for the sake of testing serialization. Since this class is mutable, these ought to never be
         * used outside of testing.
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SampleStats that = (SampleStats) o;
            if (samples.longValue() != that.samples.longValue()) {
                return false;
            }
            if (potentialSamples.longValue() != that.potentialSamples.longValue()) {
                return false;
            }
            if (samplesRejectedForMaxSamplesExceeded.longValue() != that.samplesRejectedForMaxSamplesExceeded.longValue()) {
                return false;
            }
            if (samplesRejectedForCondition.longValue() != that.samplesRejectedForCondition.longValue()) {
                return false;
            }
            if (samplesRejectedForRate.longValue() != that.samplesRejectedForRate.longValue()) {
                return false;
            }
            if (samplesRejectedForException.longValue() != that.samplesRejectedForException.longValue()) {
                return false;
            }
            if (samplesRejectedForSize.longValue() != that.samplesRejectedForSize.longValue()) {
                return false;
            }
            if (timeSamplingInNanos.longValue() != that.timeSamplingInNanos.longValue()) {
                return false;
            }
            if (timeEvaluatingConditionInNanos.longValue() != that.timeEvaluatingConditionInNanos.longValue()) {
                return false;
            }
            if (timeCompilingConditionInNanos.longValue() != that.timeCompilingConditionInNanos.longValue()) {
                return false;
            }
            return exceptionsAreEqual(lastException, that.lastException);
        }

        /*
         * This is used because most Exceptions do not have an equals or hashCode, and cause trouble when testing for equality in
         * serialization unit tests. This method returns true if the exceptions are the same class and have the same message. This is good
         * enough for serialization unit tests.
         */
        private boolean exceptionsAreEqual(Exception e1, Exception e2) {
            if (e1 == null && e2 == null) {
                return true;
            }
            if (e1 == null || e2 == null) {
                return false;
            }
            return e1.getClass().equals(e2.getClass()) && e1.getMessage().equals(e2.getMessage());
        }

        /*
         * equals and hashCode are implemented for the sake of testing serialization. Since this class is mutable, these ought to never be
         * used outside of testing.
         */
        @Override
        public int hashCode() {
            return Objects.hash(
                samples.longValue(),
                potentialSamples.longValue(),
                samplesRejectedForMaxSamplesExceeded.longValue(),
                samplesRejectedForCondition.longValue(),
                samplesRejectedForRate.longValue(),
                samplesRejectedForException.longValue(),
                samplesRejectedForSize.longValue(),
                timeSamplingInNanos.longValue(),
                timeEvaluatingConditionInNanos.longValue(),
                timeCompilingConditionInNanos.longValue()
            ) + hashException(lastException);
        }

        private int hashException(Exception e) {
            if (e == null) {
                return 0;
            } else {
                return Objects.hash(e.getClass(), e.getMessage());
            }
        }

        /*
         * If the sample stats report more raw documents than the maximum size allowed for this sample, then this method creates a new
         * cloned copy of the stats, but with the reported samples lowered to maxSize, and the reported rejected documents increased by the
         * same amount. This avoids the confusing situation of the stats reporting more samples than the user has configured. This can
         * happen in a multi-node cluster when each node has collected fewer than maxSize raw documents but the total across all nodes is
         * greater than maxSize.
         */
        public SampleStats adjustForMaxSize(int maxSize) {
            long actualSamples = samples.longValue();
            if (actualSamples > maxSize) {
                SampleStats adjusted = new SampleStats().combine(this);
                adjusted.samples.add(maxSize - actualSamples);
                adjusted.samplesRejectedForMaxSamplesExceeded.add(actualSamples - maxSize);
                return adjusted;
            } else {
                return this;
            }
        }
    }

    /*
     * This is used internally to store information about a sample in the samples Map.
     */
    private static final class SampleInfo {
        public static final SampleInfo NONE = new SampleInfo(0);
        private final RawDocument[] rawDocuments;
        /*
         * This stores the maximum index in rawDocuments that has data currently. This is incremented speculatively before writing data to
         * the array, so it is possible that this index is rawDocuments.length or greater.
         */
        private final AtomicInteger rawDocumentsIndex = new AtomicInteger(-1);
        /*
         * This caches the size of all raw documents in the rawDocuments array up to and including the data at the index on the left side
         * of the tuple. The size in bytes is the right side of the tuple.
         */
        private volatile Tuple<Integer, Long> sizeInBytesAtIndex = Tuple.tuple(-1, 0L);
        private final SampleStats stats;
        private volatile Script script;
        private volatile IngestConditionalScript.Factory factory;
        private volatile boolean compilationFailed = false;
        private volatile boolean isFull = false;

        SampleInfo(int maxSamples) {
            this.rawDocuments = new RawDocument[maxSamples];
            this.stats = new SampleStats();
        }

        /*
         * This returns the array of raw documents. It's size will be the maximum number of raw documents allowed in this sample. Some (or
         * all) elements could be null.
         */
        public RawDocument[] getRawDocuments() {
            return rawDocuments;
        }

        /*
         * This gets an approximate size in bytes for this sample. It only takes the size of the raw documents into account, since that is
         * the only part of the sample that is not a fixed size. This method favors speed over 100% correctness -- it is possible during
         * heavy concurrent ingestion that it under-reports the current size.
         */
        public long getSizeInBytes() {
            /*
             * This method could get called very frequently during ingestion. Looping through every RawDocument every time would get
             * expensive. Since the data in the rawDocuments array is immutable once it has been written, we store the index and value of
             * the computed size if all raw documents up to that index are non-null (i.e. no documents were still in flight as we were
             * counting). That way we don't have to re-compute the size for documents we've already looked at.
             */
            Tuple<Integer, Long> knownIndexAndSize = sizeInBytesAtIndex;
            int knownSizeIndex = knownIndexAndSize.v1();
            long knownSize = knownIndexAndSize.v2();
            // It is possible that rawDocumentsIndex is beyond the end of rawDocuments
            int currentRawDocumentsIndex = Math.min(rawDocumentsIndex.get(), rawDocuments.length - 1);
            if (currentRawDocumentsIndex == knownSizeIndex) {
                return knownSize;
            }
            long size = knownSize;
            boolean anyNulls = false;
            for (int i = knownSizeIndex + 1; i <= currentRawDocumentsIndex; i++) {
                RawDocument rawDocument = rawDocuments[i];
                if (rawDocument == null) {
                    /*
                     * Some documents were in flight and haven't been stored in the array yet, so we'll move past this. The size will be a
                     * little low on this method call. So we're going to set this flag so that we don't store this value for future use.
                     */
                    anyNulls = true;
                } else {
                    size += rawDocuments[i].getSizeInBytes();
                }
            }
            /*
             * The most important thing is for this method to be fast. It is OK if we store the same value twice, or even if we store a
             * slightly out-of-date copy, as long as we don't do any locking. The correct size will be calculated next time.
             */
            if (anyNulls == false) {
                sizeInBytesAtIndex = Tuple.tuple(currentRawDocumentsIndex, size);
            }
            return size;
        }

        /*
         * Adds the rawDocument to the sample if there is capacity. Returns true if it adds it, or false if it does not.
         */
        public boolean offer(RawDocument rawDocument) {
            int index = rawDocumentsIndex.incrementAndGet();
            if (index < rawDocuments.length) {
                rawDocuments[index] = rawDocument;
                if (index == rawDocuments.length - 1) {
                    isFull = true;
                }
                return true;
            }
            return false;
        }

        void setScript(Script script, IngestConditionalScript.Factory factory) {
            this.script = script;
            this.factory = factory;
        }
    }

    static class UpdateSamplingConfigurationTask extends AckedBatchedClusterStateUpdateTask {
        private final ProjectId projectId;
        private final String indexName;
        private final SamplingConfiguration samplingConfiguration;

        UpdateSamplingConfigurationTask(
            ProjectId projectId,
            String indexName,
            SamplingConfiguration samplingConfiguration,
            TimeValue ackTimeout,
            ActionListener<AcknowledgedResponse> listener
        ) {
            super(ackTimeout, listener);
            this.projectId = projectId;
            this.indexName = indexName;
            this.samplingConfiguration = samplingConfiguration;
        }
    }

    static class UpdateSamplingConfigurationExecutor extends SimpleBatchedAckListenerTaskExecutor<UpdateSamplingConfigurationTask> {
        private static final Logger logger = LogManager.getLogger(UpdateSamplingConfigurationExecutor.class);

        UpdateSamplingConfigurationExecutor() {}

        @Override
        public Tuple<ClusterState, ClusterStateAckListener> executeTask(
            UpdateSamplingConfigurationTask updateSamplingConfigurationTask,
            ClusterState clusterState
        ) {
            logger.debug(
                "Updating sampling configuration for index [{}] with rate [{}],"
                    + " maxSamples [{}], maxSize [{}], timeToLive [{}], condition [{}], creationTime [{}]",
                updateSamplingConfigurationTask.indexName,
                updateSamplingConfigurationTask.samplingConfiguration.rate(),
                updateSamplingConfigurationTask.samplingConfiguration.maxSamples(),
                updateSamplingConfigurationTask.samplingConfiguration.maxSize(),
                updateSamplingConfigurationTask.samplingConfiguration.timeToLive(),
                updateSamplingConfigurationTask.samplingConfiguration.condition(),
                updateSamplingConfigurationTask.samplingConfiguration.creationTime()
            );

            // Get sampling metadata
            Metadata metadata = clusterState.getMetadata();
            ProjectMetadata projectMetadata = metadata.getProject(updateSamplingConfigurationTask.projectId);
            SamplingMetadata samplingMetadata = projectMetadata.custom(SamplingMetadata.TYPE);

            boolean isNewConfiguration = samplingMetadata == null; // for logging
            int existingConfigCount = isNewConfiguration ? 0 : samplingMetadata.getIndexToSamplingConfigMap().size();
            logger.trace(
                "Current sampling metadata state: {} (number of existing configurations: {})",
                isNewConfiguration ? "null" : "exists",
                existingConfigCount
            );

            // Update with new sampling configuration if it exists or create new sampling metadata with the configuration
            Map<String, SamplingConfiguration> updatedConfigMap = new HashMap<>();
            if (samplingMetadata != null) {
                updatedConfigMap.putAll(samplingMetadata.getIndexToSamplingConfigMap());
            }
            boolean isUpdate = updatedConfigMap.containsKey(updateSamplingConfigurationTask.indexName);

            Integer maxConfigurations = MAX_CONFIGURATIONS_SETTING.get(metadata.settings());
            // check if adding a new configuration would exceed the limit
            boolean maxConfigLimitBreached = checkMaxConfigLimitBreached(
                updateSamplingConfigurationTask.projectId,
                updateSamplingConfigurationTask.indexName,
                clusterState
            );
            if (maxConfigLimitBreached) {
                throw new IllegalStateException(
                    "Cannot add sampling configuration for index ["
                        + updateSamplingConfigurationTask.indexName
                        + "]. Maximum number of sampling configurations ("
                        + maxConfigurations
                        + ") already reached."
                );
            }
            updatedConfigMap.put(updateSamplingConfigurationTask.indexName, updateSamplingConfigurationTask.samplingConfiguration);

            logger.trace(
                "{} sampling configuration for index [{}], total configurations after update: {}",
                isUpdate ? "Updated" : "Added",
                updateSamplingConfigurationTask.indexName,
                updatedConfigMap.size()
            );

            SamplingMetadata newSamplingMetadata = new SamplingMetadata(updatedConfigMap);

            // Update cluster state
            ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectMetadata);
            projectMetadataBuilder.putCustom(SamplingMetadata.TYPE, newSamplingMetadata);

            // Return tuple with updated cluster state and the original listener
            ClusterState updatedClusterState = ClusterState.builder(clusterState).putProjectMetadata(projectMetadataBuilder).build();

            logger.debug(
                "Successfully {} sampling configuration for index [{}]",
                isUpdate ? "updated" : "created",
                updateSamplingConfigurationTask.indexName
            );
            return new Tuple<>(updatedClusterState, updateSamplingConfigurationTask);
        }
    }

    static final class DeleteSampleConfigurationTask extends AckedBatchedClusterStateUpdateTask {
        private final ProjectId projectId;
        private final String indexName;

        DeleteSampleConfigurationTask(
            ProjectId projectId,
            String indexName,
            TimeValue ackTimeout,
            ActionListener<AcknowledgedResponse> listener
        ) {
            super(ackTimeout, listener);
            this.projectId = projectId;
            this.indexName = indexName;
        }
    }

    static final class DeleteSampleConfigurationExecutor extends SimpleBatchedAckListenerTaskExecutor<DeleteSampleConfigurationTask> {
        private static final Logger logger = LogManager.getLogger(DeleteSampleConfigurationExecutor.class);

        DeleteSampleConfigurationExecutor() {}

        @Override
        public Tuple<ClusterState, ClusterStateAckListener> executeTask(
            DeleteSampleConfigurationTask deleteSampleConfigurationTask,
            ClusterState clusterState
        ) {
            // Get sampling metadata
            Metadata metadata = clusterState.getMetadata();
            ProjectMetadata projectMetadata = metadata.getProject(deleteSampleConfigurationTask.projectId);
            SamplingMetadata samplingMetadata = projectMetadata.custom(SamplingMetadata.TYPE);
            Map<String, SamplingConfiguration> oldConfigMap = validateConfigExists(
                deleteSampleConfigurationTask.indexName,
                samplingMetadata
            );
            logger.debug("Deleting sampling configuration for index [{}]", deleteSampleConfigurationTask.indexName);

            // Delete the sampling configuration if it exists
            Map<String, SamplingConfiguration> updatedConfigMap = new HashMap<>(oldConfigMap);
            updatedConfigMap.remove(deleteSampleConfigurationTask.indexName);
            SamplingMetadata newSamplingMetadata = new SamplingMetadata(updatedConfigMap);

            // Update cluster state
            ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectMetadata);
            projectMetadataBuilder.putCustom(SamplingMetadata.TYPE, newSamplingMetadata);

            // Return tuple with updated cluster state and the original listener
            ClusterState updatedClusterState = ClusterState.builder(clusterState).putProjectMetadata(projectMetadataBuilder).build();

            logger.debug(
                "Successfully deleted sampling configuration for index [{}], total configurations after deletion: [{}]",
                deleteSampleConfigurationTask.indexName,
                updatedConfigMap.size()
            );
            return new Tuple<>(updatedClusterState, deleteSampleConfigurationTask);
        }

        // Validates that the configuration exists, returns the index to config map if it does.
        private static Map<String, SamplingConfiguration> validateConfigExists(String indexName, SamplingMetadata samplingMetadata) {
            final String exceptionMessage = "provided index [" + indexName + "] has no sampling configuration";
            if (samplingMetadata == null) {
                throw new ResourceNotFoundException(exceptionMessage);
            }
            Map<String, SamplingConfiguration> configMap = samplingMetadata.getIndexToSamplingConfigMap();
            if (configMap == null || configMap.containsKey(indexName) == false) {
                throw new ResourceNotFoundException(exceptionMessage);
            }
            return configMap;
        }
    }

    /*
     * This is meant to be used internally as the key of the map of samples
     */
    private record ProjectIndex(ProjectId projectId, String indexName) {};

}
