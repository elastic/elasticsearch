/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.iplocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.iplocation.api.IpLocationConsumer;
import org.elasticsearch.iplocation.api.IpLocationService;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.ingest.iplocation.GeoIpProcessor.GEOIP_TYPE;
import static org.elasticsearch.ingest.iplocation.GeoIpProcessor.IP_LOCATION_TYPE;

public class IngestIpLocationPlugin extends Plugin implements IngestPlugin, ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(IngestIpLocationPlugin.class);

    private IpLocationService ipLocationService;
    private IngestService ingestService;

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters params) {
        this.ipLocationService = params.ipLocationService;
        this.ingestService = params.ingestService;
        params.ingestService.getClusterService().addListener(this);

        ipLocationService.addDatabaseAvailabilityListener((projectId, databaseFile) -> {
            ProjectId pid = ProjectId.fromId(projectId);
            var ids = ingestService.getPipelineWithProcessorType(
                pid,
                DatabaseUnavailableProcessor.class,
                p -> databaseFile.equals(p.getDatabaseName())
            );
            for (var id : ids) {
                try {
                    ingestService.reloadPipeline(pid, id);
                } catch (Exception e) {
                    logger.warn(() -> format("failed to reload pipeline [%s] after database [%s] became available", id, databaseFile), e);
                }
            }
        });

        return Map.of(
            GEOIP_TYPE,
            new GeoIpProcessor.Factory(GEOIP_TYPE, ipLocationService),
            IP_LOCATION_TYPE,
            new GeoIpProcessor.Factory(IP_LOCATION_TYPE, ipLocationService)
        );
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        // Only the master node needs to update download consumer metadata — all nodes see the
        // same ingest/index metadata, so having every node fire a transport action is redundant.
        if (event.localNodeMaster() == false) {
            return;
        }

        // On every master election, reconcile each project so an upgraded master can recover the
        // IpLocationDownloadConsumers custom (introduced in 9.5) even if the previous master (older
        // version) didn't write it. Idempotent for 9.5 -> 9.5 handovers since both requestDownloads
        // and cancelDownloadRequest short-circuit when the consumer is already in the desired state.
        boolean masterChanged = event.previousState().nodes().isLocalNodeElectedMaster() == false;

        if (event.metadataChanged() == false && masterChanged == false) {
            return;
        }

        for (var projectMetadata : event.state().metadata().projects().values()) {
            ProjectId projectId = projectMetadata.id();

            boolean hasIngestChanges = event.customMetadataChanged(projectId, IngestMetadata.TYPE);
            boolean hasIndicesChanges = false;
            boolean projectExisted = event.previousState().metadata().hasProject(projectId);
            if (projectExisted) {
                hasIndicesChanges = event.previousState()
                    .metadata()
                    .getProject(projectId)
                    .indices()
                    .equals(projectMetadata.indices()) == false;
            }

            if (hasIngestChanges == false && hasIndicesChanges == false && masterChanged == false) {
                continue;
            }

            // A geoip processor with download_database_on_pipeline_creation:true wants its databases as soon as the pipeline
            // exists; one with download_database_on_pipeline_creation:false only wants them once an index references the
            // pipeline.
            Set<String> eagerPipelines = pipelinesWithGeoIpProcessor(projectMetadata, true);
            Set<String> lazyPipelines = pipelinesWithGeoIpProcessor(projectMetadata, false);
            if (shouldRequestDownloads(projectMetadata, eagerPipelines, lazyPipelines)) {
                ipLocationService.requestDownloads(projectId.id(), IpLocationConsumer.INGEST);
            } else if (lazyPipelines.isEmpty()) {
                // Drop any pending downloads for this project only if there are no pipelines that need them (we know the eager
                // pipelines are empty if we ever get here).
                ipLocationService.cancelDownloadRequest(projectId.id(), IpLocationConsumer.INGEST);
            }
        }
    }

    /**
     * Whether IP-location downloads should be requested for the project: an eager
     * ({@code download_database_on_pipeline_creation:true}) geoip pipeline exists, or an index references one of the
     * lazy pipelines.
     */
    static boolean shouldRequestDownloads(ProjectMetadata projectMetadata, Set<String> eagerPipelines, Set<String> lazyPipelines) {
        return eagerPipelines.isEmpty() == false || anyIndexReferencesPipeline(projectMetadata, lazyPipelines);
    }

    /**
     * Returns whether any index in the project references one of the given pipelines as its default or final pipeline.
     */
    private static boolean anyIndexReferencesPipeline(ProjectMetadata projectMetadata, Set<String> pipelineIds) {
        if (pipelineIds.isEmpty()) {
            return false;
        }
        for (IndexMetadata indexMetadata : projectMetadata.indices().values()) {
            String defaultPipeline = IndexSettings.DEFAULT_PIPELINE.get(indexMetadata.getSettings());
            String finalPipeline = IndexSettings.FINAL_PIPELINE.get(indexMetadata.getSettings());
            if (pipelineIds.contains(defaultPipeline) || pipelineIds.contains(finalPipeline)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Retrieve the set of pipeline ids that have at least one geoip processor.
     * @param projectMetadata project metadata
     * @param downloadDatabaseOnPipelineCreation Filter the list to include only pipeline with the download_database_on_pipeline_creation
     *                                           matching the param.
     * @return A set of pipeline ids matching criteria.
     */
    @SuppressWarnings("unchecked")
    static Set<String> pipelinesWithGeoIpProcessor(ProjectMetadata projectMetadata, boolean downloadDatabaseOnPipelineCreation) {
        List<PipelineConfiguration> configurations = IngestService.getPipelines(projectMetadata);
        Map<String, PipelineConfiguration> pipelineConfigById = HashMap.newHashMap(configurations.size());
        for (PipelineConfiguration configuration : configurations) {
            pipelineConfigById.put(configuration.getId(), configuration);
        }
        // this map is used to keep track of pipelines that have already been checked
        Map<String, Boolean> pipelineHasGeoProcessorById = HashMap.newHashMap(configurations.size());
        Set<String> ids = new HashSet<>();
        // note: this loop is unrolled rather than streaming-style because it's hot enough to show up in a flamegraph
        for (PipelineConfiguration configuration : configurations) {
            List<Map<String, Object>> processors = (List<Map<String, Object>>) configuration.getConfig().get(Pipeline.PROCESSORS_KEY);
            String pipelineName = configuration.getId();
            if (pipelineHasGeoProcessorById.containsKey(pipelineName) == false) {
                if (hasAtLeastOneGeoipProcessor(
                    processors,
                    downloadDatabaseOnPipelineCreation,
                    pipelineConfigById,
                    pipelineHasGeoProcessorById
                )) {
                    ids.add(pipelineName);
                }
            }
        }
        return Collections.unmodifiableSet(ids);
    }

    /**
     * Check if a list of processor contains at least a geoip processor.
     * @param processors List of processors.
     * @param downloadDatabaseOnPipelineCreation Should the download_database_on_pipeline_creation of the geoip processor be true or false.
     * @param pipelineConfigById A Map of pipeline id to PipelineConfiguration
     * @param pipelineHasGeoProcessorById A Map of pipeline id to Boolean, indicating whether the pipeline references a geoip processor
     *                                    (true), does not reference a geoip processor (false), or we are currently trying to figure that
     *                                    out (null).
     * @return true if a geoip processor is found in the processor list.
     */
    private static boolean hasAtLeastOneGeoipProcessor(
        List<Map<String, Object>> processors,
        boolean downloadDatabaseOnPipelineCreation,
        Map<String, PipelineConfiguration> pipelineConfigById,
        Map<String, Boolean> pipelineHasGeoProcessorById
    ) {
        if (processors != null) {
            // note: this loop is unrolled rather than streaming-style because it's hot enough to show up in a flamegraph
            for (Map<String, Object> processor : processors) {
                if (hasAtLeastOneGeoipProcessor(
                    processor,
                    downloadDatabaseOnPipelineCreation,
                    pipelineConfigById,
                    pipelineHasGeoProcessorById
                )) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Check if a processor config is a geoip processor or contains at least a geoip processor.
     * @param processor Processor config.
     * @param downloadDatabaseOnPipelineCreation Should the download_database_on_pipeline_creation of the geoip processor be true or false.
     * @param pipelineConfigById A Map of pipeline id to PipelineConfiguration
     * @param pipelineHasGeoProcessorById A Map of pipeline id to Boolean, indicating whether the pipeline references a geoip processor
     *                                    (true), does not reference a geoip processor (false), or we are currently trying to figure that
     *                                    out (null).
     * @return true if a geoip processor is found in the processor list.
     */
    @SuppressWarnings("unchecked")
    private static boolean hasAtLeastOneGeoipProcessor(
        Map<String, Object> processor,
        boolean downloadDatabaseOnPipelineCreation,
        Map<String, PipelineConfiguration> pipelineConfigById,
        Map<String, Boolean> pipelineHasGeoProcessorById
    ) {
        if (processor == null) {
            return false;
        }

        Map<String, Object> processorConfig = (Map<String, Object>) processor.get(GEOIP_TYPE);
        if (processorConfig != null) {
            return GeoIpProcessor.Factory.downloadDatabaseOnPipelineCreation(processorConfig) == downloadDatabaseOnPipelineCreation;
        }

        processorConfig = (Map<String, Object>) processor.get(IP_LOCATION_TYPE);
        if (processorConfig != null) {
            return GeoIpProcessor.Factory.downloadDatabaseOnPipelineCreation(processorConfig) == downloadDatabaseOnPipelineCreation;
        }

        return isProcessorWithOnFailureGeoIpProcessor(
            processor,
            downloadDatabaseOnPipelineCreation,
            pipelineConfigById,
            pipelineHasGeoProcessorById
        )
            || isForeachProcessorWithGeoipProcessor(
                processor,
                downloadDatabaseOnPipelineCreation,
                pipelineConfigById,
                pipelineHasGeoProcessorById
            )
            || isPipelineProcessorWithGeoIpProcessor(
                processor,
                downloadDatabaseOnPipelineCreation,
                pipelineConfigById,
                pipelineHasGeoProcessorById
            );
    }

    /**
     * Check if a processor config has an on_failure clause containing at least a geoip processor.
     * @param processor Processor config.
     * @param downloadDatabaseOnPipelineCreation Should the download_database_on_pipeline_creation of the geoip processor be true or false.
     * @param pipelineConfigById A Map of pipeline id to PipelineConfiguration
     * @param pipelineHasGeoProcessorById A Map of pipeline id to Boolean, indicating whether the pipeline references a geoip processor
     *                                    (true), does not reference a geoip processor (false), or we are currently trying to figure that
     *                                    out (null).
     * @return true if a geoip processor is found in the processor list.
     */
    @SuppressWarnings("unchecked")
    private static boolean isProcessorWithOnFailureGeoIpProcessor(
        Map<String, Object> processor,
        boolean downloadDatabaseOnPipelineCreation,
        Map<String, PipelineConfiguration> pipelineConfigById,
        Map<String, Boolean> pipelineHasGeoProcessorById
    ) {
        // note: this loop is unrolled rather than streaming-style because it's hot enough to show up in a flamegraph
        for (Object value : processor.values()) {
            if (value instanceof Map
                && hasAtLeastOneGeoipProcessor(
                    ((Map<String, List<Map<String, Object>>>) value).get("on_failure"),
                    downloadDatabaseOnPipelineCreation,
                    pipelineConfigById,
                    pipelineHasGeoProcessorById
                )) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if a processor is a foreach processor containing at least a geoip processor.
     * @param processor Processor config.
     * @param downloadDatabaseOnPipelineCreation Should the download_database_on_pipeline_creation of the geoip processor be true or false.
     * @param pipelineConfigById A Map of pipeline id to PipelineConfiguration
     * @param pipelineHasGeoProcessorById A Map of pipeline id to Boolean, indicating whether the pipeline references a geoip processor
     *                                    (true), does not reference a geoip processor (false), or we are currently trying to figure that
     *                                    out (null).
     * @return true if a geoip processor is found in the processor list.
     */
    @SuppressWarnings("unchecked")
    private static boolean isForeachProcessorWithGeoipProcessor(
        Map<String, Object> processor,
        boolean downloadDatabaseOnPipelineCreation,
        Map<String, PipelineConfiguration> pipelineConfigById,
        Map<String, Boolean> pipelineHasGeoProcessorById
    ) {
        Map<String, Object> processorConfig = (Map<String, Object>) processor.get("foreach");
        return processorConfig != null
            && hasAtLeastOneGeoipProcessor(
                (Map<String, Object>) processorConfig.get("processor"),
                downloadDatabaseOnPipelineCreation,
                pipelineConfigById,
                pipelineHasGeoProcessorById
            );
    }

    /**
     * Check if a processor is a pipeline processor containing at least a geoip processor. This method also updates
     * pipelineHasGeoProcessorById with a result for any pipelines it looks at.
     * @param processor Processor config.
     * @param downloadDatabaseOnPipelineCreation Should the download_database_on_pipeline_creation of the geoip processor be true or false.
     * @param pipelineConfigById A Map of pipeline id to PipelineConfiguration
     * @param pipelineHasGeoProcessorById A Map of pipeline id to Boolean, indicating whether the pipeline references a geoip processor
     *                                    (true), does not reference a geoip processor (false), or we are currently trying to figure that
     *                                    out (null).
     * @return true if a geoip processor is found in the processors of this processor if this processor is a pipeline processor.
     */
    @SuppressWarnings("unchecked")
    private static boolean isPipelineProcessorWithGeoIpProcessor(
        Map<String, Object> processor,
        boolean downloadDatabaseOnPipelineCreation,
        Map<String, PipelineConfiguration> pipelineConfigById,
        Map<String, Boolean> pipelineHasGeoProcessorById
    ) {
        Map<String, Object> processorConfig = (Map<String, Object>) processor.get("pipeline");
        if (processorConfig != null) {
            String pipelineName = (String) processorConfig.get("name");
            if (pipelineName != null) {
                if (pipelineHasGeoProcessorById.containsKey(pipelineName)) {
                    if (pipelineHasGeoProcessorById.get(pipelineName) == null) {
                        /*
                         * If the value is null here, it indicates that this method has been called recursively with the same pipeline name.
                         * This will cause a runtime error when the pipeline is executed, but we're avoiding changing existing behavior at
                         * server startup time. Instead, we just bail out as quickly as possible. It is possible that this could lead to a
                         * geo database not being downloaded for the pipeline, but it doesn't really matter since the pipeline was going to
                         * fail anyway.
                         */
                        pipelineHasGeoProcessorById.put(pipelineName, false);
                    }
                } else {
                    List<Map<String, Object>> childProcessors = null;
                    PipelineConfiguration config = pipelineConfigById.get(pipelineName);
                    if (config != null) {
                        childProcessors = (List<Map<String, Object>>) config.getConfig().get(Pipeline.PROCESSORS_KEY);
                    }
                    // We initialize this to null so that we know it's in progress and can use it to avoid stack overflow errors:
                    pipelineHasGeoProcessorById.put(pipelineName, null);
                    pipelineHasGeoProcessorById.put(
                        pipelineName,
                        hasAtLeastOneGeoipProcessor(
                            childProcessors,
                            downloadDatabaseOnPipelineCreation,
                            pipelineConfigById,
                            pipelineHasGeoProcessorById
                        )
                    );
                }
                return pipelineHasGeoProcessorById.get(pipelineName);
            }
        }
        return false;
    }
}
