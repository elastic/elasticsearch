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
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.iplocation.api.IpLocationService;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.ingest.iplocation.GeoIpProcessor.GEOIP_TYPE;
import static org.elasticsearch.ingest.iplocation.GeoIpProcessor.IP_LOCATION_TYPE;

public class IngestIpLocationPlugin extends Plugin implements IngestPlugin, ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(IngestIpLocationPlugin.class);

    private IpLocationService ipLocationService;
    private IngestService ingestService;
    private final Map<ProjectId, Boolean> downloadRequestedByProject = new ConcurrentHashMap<>();

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
                    logger.debug(() -> format("failed to reload pipeline [%s] after database [%s] became available", id, databaseFile), e);
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
        if (event.metadataChanged() == false) {
            return;
        }

        var currentProjects = event.state().metadata().projects();

        for (var projectMetadata : currentProjects.values()) {
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

            if (hasIngestChanges == false && hasIndicesChanges == false) {
                continue;
            }

            boolean previouslyRequested = downloadRequestedByProject.getOrDefault(projectId, false);
            boolean nowNeeded = hasAtLeastOneGeoipProcessor(projectMetadata);

            if (nowNeeded && previouslyRequested == false) {
                downloadRequestedByProject.put(projectId, true);
                ipLocationService.requestDownloads(projectId.id());
            } else if (nowNeeded == false && previouslyRequested) {
                downloadRequestedByProject.put(projectId, false);
                ipLocationService.cancelDownloadRequest(projectId.id());
            }
        }

        // Cancel downloads for projects that have been removed from cluster state
        var iterator = downloadRequestedByProject.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            if (entry.getValue() && currentProjects.containsKey(entry.getKey()) == false) {
                ipLocationService.cancelDownloadRequest(entry.getKey().id());
                iterator.remove();
            }
        }
    }

    boolean isDownloadRequestedForProject(ProjectId projectId) {
        return downloadRequestedByProject.getOrDefault(projectId, false);
    }

    static boolean hasAtLeastOneGeoipProcessor(ProjectMetadata projectMetadata) {
        if (pipelinesWithGeoIpProcessor(projectMetadata, true).isEmpty() == false) {
            return true;
        }

        Set<String> checkReferencedPipelines = pipelinesWithGeoIpProcessor(projectMetadata, false);
        if (checkReferencedPipelines.isEmpty()) {
            return false;
        }

        for (IndexMetadata indexMetadata : projectMetadata.indices().values()) {
            String defaultPipeline = IndexSettings.DEFAULT_PIPELINE.get(indexMetadata.getSettings());
            String finalPipeline = IndexSettings.FINAL_PIPELINE.get(indexMetadata.getSettings());
            if (checkReferencedPipelines.contains(defaultPipeline) || checkReferencedPipelines.contains(finalPipeline)) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private static Set<String> pipelinesWithGeoIpProcessor(ProjectMetadata projectMetadata, boolean downloadDatabaseOnPipelineCreation) {
        List<PipelineConfiguration> configurations = IngestService.getPipelines(projectMetadata);
        Map<String, PipelineConfiguration> pipelineConfigById = HashMap.newHashMap(configurations.size());
        for (PipelineConfiguration configuration : configurations) {
            pipelineConfigById.put(configuration.getId(), configuration);
        }
        Map<String, Boolean> pipelineHasGeoProcessorById = HashMap.newHashMap(configurations.size());
        Set<String> ids = new HashSet<>();
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

    private static boolean hasAtLeastOneGeoipProcessor(
        List<Map<String, Object>> processors,
        boolean downloadDatabaseOnPipelineCreation,
        Map<String, PipelineConfiguration> pipelineConfigById,
        Map<String, Boolean> pipelineHasGeoProcessorById
    ) {
        if (processors != null) {
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

        {
            Map<String, Object> processorConfig = (Map<String, Object>) processor.get(GEOIP_TYPE);
            if (processorConfig != null) {
                return GeoIpProcessor.Factory.downloadDatabaseOnPipelineCreation(processorConfig) == downloadDatabaseOnPipelineCreation;
            }
        }

        {
            Map<String, Object> processorConfig = (Map<String, Object>) processor.get(IP_LOCATION_TYPE);
            if (processorConfig != null) {
                return GeoIpProcessor.Factory.downloadDatabaseOnPipelineCreation(processorConfig) == downloadDatabaseOnPipelineCreation;
            }
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

    @SuppressWarnings("unchecked")
    private static boolean isProcessorWithOnFailureGeoIpProcessor(
        Map<String, Object> processor,
        boolean downloadDatabaseOnPipelineCreation,
        Map<String, PipelineConfiguration> pipelineConfigById,
        Map<String, Boolean> pipelineHasGeoProcessorById
    ) {
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
                        pipelineHasGeoProcessorById.put(pipelineName, false);
                    }
                } else {
                    List<Map<String, Object>> childProcessors = null;
                    PipelineConfiguration config = pipelineConfigById.get(pipelineName);
                    if (config != null) {
                        childProcessors = (List<Map<String, Object>>) config.getConfig().get(Pipeline.PROCESSORS_KEY);
                    }
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
