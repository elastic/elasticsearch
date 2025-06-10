/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.ingest.geoip.GeoIpDownloader.DATABASES_INDEX;
import static org.elasticsearch.ingest.geoip.GeoIpDownloader.GEOIP_DOWNLOADER;
import static org.elasticsearch.ingest.geoip.GeoIpProcessor.Factory.downloadDatabaseOnPipelineCreation;
import static org.elasticsearch.ingest.geoip.GeoIpProcessor.GEOIP_TYPE;
import static org.elasticsearch.ingest.geoip.GeoIpProcessor.IP_LOCATION_TYPE;

/**
 * Persistent task executor that is responsible for starting {@link GeoIpDownloader} after task is allocated by master node.
 * Also bootstraps GeoIP download task on clean cluster and handles changes to the 'ingest.geoip.downloader.enabled' setting
 */
public final class GeoIpDownloaderTaskExecutor extends PersistentTasksExecutor<GeoIpTaskParams> implements ClusterStateListener {

    private static final boolean ENABLED_DEFAULT = "false".equals(
        System.getProperty("ingest.geoip.downloader.enabled.default", "true")
    ) == false;
    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "ingest.geoip.downloader.enabled",
        ENABLED_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope,
        Setting.Property.ProjectScope
    );
    public static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting(
        "ingest.geoip.downloader.poll.interval",
        TimeValue.timeValueDays(3),
        TimeValue.timeValueDays(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> EAGER_DOWNLOAD_SETTING = Setting.boolSetting(
        "ingest.geoip.downloader.eager.download",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(GeoIpDownloader.class);

    private final Client client;
    private final HttpClient httpClient;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Settings settings;
    private final PersistentTasksService persistentTasksService;
    @FixForMultiProject(description = "These settings need to be project-scoped")
    private volatile TimeValue pollInterval;
    private volatile boolean eagerDownload;

    private final ConcurrentHashMap<ProjectId, Boolean> atLeastOneGeoipProcessorByProject = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ProjectId, AtomicBoolean> taskIsBootstrappedByProject = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ProjectId, GeoIpDownloader> tasks = new ConcurrentHashMap<>();
    private final ProjectResolver projectResolver;

    GeoIpDownloaderTaskExecutor(
        Client client,
        HttpClient httpClient,
        ClusterService clusterService,
        ThreadPool threadPool,
        ProjectResolver projectResolver
    ) {
        super(GEOIP_DOWNLOADER, threadPool.generic());
        this.client = new OriginSettingClient(client, IngestService.INGEST_ORIGIN);
        this.httpClient = httpClient;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.settings = clusterService.getSettings();
        this.persistentTasksService = new PersistentTasksService(clusterService, threadPool, client);
        this.pollInterval = POLL_INTERVAL_SETTING.get(settings);
        this.eagerDownload = EAGER_DOWNLOAD_SETTING.get(settings);
        this.projectResolver = projectResolver;
    }

    /**
     * This method completes the initialization of the GeoIpDownloaderTaskExecutor by registering several listeners.
     */
    public void init() {
        clusterService.addListener(this);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(ENABLED_SETTING, this::setEnabled);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(EAGER_DOWNLOAD_SETTING, this::setEagerDownload);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(POLL_INTERVAL_SETTING, this::setPollInterval);
    }

    @FixForMultiProject(description = "Should execute in the context of the current project after settings are project-aware")
    private void setEnabled(boolean enabled) {
        if (clusterService.state().nodes().isLocalNodeElectedMaster() == false) {
            // we should only start/stop task from single node, master is the best as it will go through it anyway
            return;
        }
        if (enabled) {
            startTask(() -> {});
        } else {
            stopTask(() -> {});
        }
    }

    @FixForMultiProject(description = "Should execute in the context of the current project after settings are project-aware")
    private void setEagerDownload(Boolean eagerDownload) {
        assert projectResolver.getProjectId() != null : "projectId must be set before setting eager download";
        if (Objects.equals(this.eagerDownload, eagerDownload) == false) {
            this.eagerDownload = eagerDownload;
            GeoIpDownloader currentDownloader = getTask(projectResolver.getProjectId());
            if (currentDownloader != null && Objects.equals(eagerDownload, Boolean.TRUE)) {
                currentDownloader.requestReschedule();
            }
        }
    }

    @FixForMultiProject(description = "Should execute in the context of the current project after settings are project-aware")
    private void setPollInterval(TimeValue pollInterval) {
        assert projectResolver.getProjectId() != null : "projectId must be set before setting poll interval";
        if (Objects.equals(this.pollInterval, pollInterval) == false) {
            this.pollInterval = pollInterval;
            GeoIpDownloader currentDownloader = getTask(projectResolver.getProjectId());
            if (currentDownloader != null) {
                currentDownloader.requestReschedule();
            }
        }
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, GeoIpTaskParams params, PersistentTaskState state) {
        GeoIpDownloader downloader = (GeoIpDownloader) task;
        GeoIpTaskState geoIpTaskState = (state == null) ? GeoIpTaskState.EMPTY : (GeoIpTaskState) state;
        downloader.setState(geoIpTaskState);
        tasks.put(projectResolver.getProjectId(), downloader);
        if (ENABLED_SETTING.get(clusterService.state().metadata().settings(), settings)) {
            downloader.runDownloader();
        }
    }

    @Override
    protected GeoIpDownloader createTask(
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        PersistentTasksCustomMetadata.PersistentTask<GeoIpTaskParams> taskInProgress,
        Map<String, String> headers
    ) {
        ProjectId projectId = projectResolver.getProjectId();
        return new GeoIpDownloader(
            client,
            httpClient,
            clusterService,
            threadPool,
            settings,
            id,
            type,
            action,
            getDescription(taskInProgress),
            parentTaskId,
            headers,
            () -> pollInterval,
            () -> eagerDownload,
            () -> atLeastOneGeoipProcessorByProject.getOrDefault(projectId, false),
            projectId,
            projectResolver
        );
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait for state recovered
            return;
        }

        DiscoveryNode masterNode = event.state().nodes().getMasterNode();
        if (masterNode == null) {
            // no master yet
            return;
        }

        if (event.metadataChanged() == false) {
            return;
        }

        for (var projectMetadataEntry : event.state().metadata().projects().entrySet()) {
            ProjectId projectId = projectMetadataEntry.getKey();
            ProjectMetadata projectMetadata = projectMetadataEntry.getValue();

            projectResolver.executeOnProject(projectId, () -> {
                // bootstrap task once iff it is not already bootstrapped
                AtomicBoolean taskIsBootstrapped = taskIsBootstrappedByProject.computeIfAbsent(projectId, k -> new AtomicBoolean(false));
                if (taskIsBootstrapped.getAndSet(true) == false) {
                    this.taskIsBootstrappedByProject.computeIfAbsent(
                        projectId,
                        k -> new AtomicBoolean(hasAtLeastOneGeoipProcessor(projectMetadata))
                    );
                    if (ENABLED_SETTING.get(event.state().getMetadata().settings(), settings)) {
                        logger.debug("Bootstrapping geoip downloader task for project [{}]", projectId);
                        startTask(() -> taskIsBootstrapped.set(false));
                    } else {
                        logger.debug("Stopping geoip downloader task for project [{}]", projectId);
                        stopTask(() -> taskIsBootstrapped.set(false));
                    }
                }

                boolean hasIngestPipelineChanges = event.changedCustomProjectMetadataSet(projectId).contains(IngestMetadata.TYPE);
                boolean hasIndicesChanges = false;
                boolean projectExisted = event.previousState().metadata().hasProject(projectId);
                if (projectExisted) {
                    hasIndicesChanges = event.previousState()
                        .metadata()
                        .getProject(projectId)
                        .indices()
                        .equals(projectMetadata.indices()) == false;
                }

                if (hasIngestPipelineChanges || hasIndicesChanges) {
                    var atLeastOneGeoipProcessor = atLeastOneGeoipProcessorByProject.computeIfAbsent(projectId, k -> false);
                    boolean newAtLeastOneGeoipProcessor = hasAtLeastOneGeoipProcessor(projectMetadata);

                    if (newAtLeastOneGeoipProcessor && atLeastOneGeoipProcessor == false) {
                        logger.trace("Scheduling runDownloader for project [{}] because a geoip processor has been added", projectId);
                        GeoIpDownloader currentDownloader = getTask(projectId);
                        if (currentDownloader != null) {
                            currentDownloader.requestReschedule();
                        }
                    }
                    atLeastOneGeoipProcessorByProject.put(projectId, newAtLeastOneGeoipProcessor);
                }
            });
        }
    }

    static boolean hasAtLeastOneGeoipProcessor(ProjectMetadata projectMetadata) {
        if (pipelinesWithGeoIpProcessor(projectMetadata, true).isEmpty() == false) {
            return true;
        }

        final Set<String> checkReferencedPipelines = pipelinesWithGeoIpProcessor(projectMetadata, false);
        if (checkReferencedPipelines.isEmpty()) {
            return false;
        }

        return projectMetadata.indices().values().stream().anyMatch(indexMetadata -> {
            String defaultPipeline = IndexSettings.DEFAULT_PIPELINE.get(indexMetadata.getSettings());
            String finalPipeline = IndexSettings.FINAL_PIPELINE.get(indexMetadata.getSettings());
            return checkReferencedPipelines.contains(defaultPipeline) || checkReferencedPipelines.contains(finalPipeline);
        });
    }

    /**
     * Retrieve the set of pipeline ids that have at least one geoip processor.
     * @param projectMetadata project metadata
     * @param downloadDatabaseOnPipelineCreation Filter the list to include only pipeline with the download_database_on_pipeline_creation
     *                                           matching the param.
     * @return A set of pipeline ids matching criteria.
     */
    @SuppressWarnings("unchecked")
    private static Set<String> pipelinesWithGeoIpProcessor(ProjectMetadata projectMetadata, boolean downloadDatabaseOnPipelineCreation) {
        List<PipelineConfiguration> configurations = IngestService.getPipelines(projectMetadata);
        Set<String> ids = new HashSet<>();
        // note: this loop is unrolled rather than streaming-style because it's hot enough to show up in a flamegraph
        for (PipelineConfiguration configuration : configurations) {
            List<Map<String, Object>> processors = (List<Map<String, Object>>) configuration.getConfig().get(Pipeline.PROCESSORS_KEY);
            if (hasAtLeastOneGeoipProcessor(processors, downloadDatabaseOnPipelineCreation)) {
                ids.add(configuration.getId());
            }
        }
        return Collections.unmodifiableSet(ids);
    }

    /**
     * Check if a list of processor contains at least a geoip processor.
     * @param processors List of processors.
     * @param downloadDatabaseOnPipelineCreation Should the download_database_on_pipeline_creation of the geoip processor be true or false.
     * @return true if a geoip processor is found in the processor list.
     */
    private static boolean hasAtLeastOneGeoipProcessor(List<Map<String, Object>> processors, boolean downloadDatabaseOnPipelineCreation) {
        if (processors != null) {
            // note: this loop is unrolled rather than streaming-style because it's hot enough to show up in a flamegraph
            for (Map<String, Object> processor : processors) {
                if (hasAtLeastOneGeoipProcessor(processor, downloadDatabaseOnPipelineCreation)) {
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
     * @return true if a geoip processor is found in the processor list.
     */
    @SuppressWarnings("unchecked")
    private static boolean hasAtLeastOneGeoipProcessor(Map<String, Object> processor, boolean downloadDatabaseOnPipelineCreation) {
        if (processor == null) {
            return false;
        }

        {
            final Map<String, Object> processorConfig = (Map<String, Object>) processor.get(GEOIP_TYPE);
            if (processorConfig != null) {
                return downloadDatabaseOnPipelineCreation(processorConfig) == downloadDatabaseOnPipelineCreation;
            }
        }

        {
            final Map<String, Object> processorConfig = (Map<String, Object>) processor.get(IP_LOCATION_TYPE);
            if (processorConfig != null) {
                return downloadDatabaseOnPipelineCreation(processorConfig) == downloadDatabaseOnPipelineCreation;
            }
        }

        return isProcessorWithOnFailureGeoIpProcessor(processor, downloadDatabaseOnPipelineCreation)
            || isForeachProcessorWithGeoipProcessor(processor, downloadDatabaseOnPipelineCreation);
    }

    /**
     * Check if a processor config has an on_failure clause containing at least a geoip processor.
     * @param processor Processor config.
     * @param downloadDatabaseOnPipelineCreation Should the download_database_on_pipeline_creation of the geoip processor be true or false.
     * @return true if a geoip processor is found in the processor list.
     */
    @SuppressWarnings("unchecked")
    private static boolean isProcessorWithOnFailureGeoIpProcessor(
        Map<String, Object> processor,
        boolean downloadDatabaseOnPipelineCreation
    ) {
        // note: this loop is unrolled rather than streaming-style because it's hot enough to show up in a flamegraph
        for (Object value : processor.values()) {
            if (value instanceof Map
                && hasAtLeastOneGeoipProcessor(
                    ((Map<String, List<Map<String, Object>>>) value).get("on_failure"),
                    downloadDatabaseOnPipelineCreation
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
     * @return true if a geoip processor is found in the processor list.
     */
    @SuppressWarnings("unchecked")
    private static boolean isForeachProcessorWithGeoipProcessor(Map<String, Object> processor, boolean downloadDatabaseOnPipelineCreation) {
        final Map<String, Object> processorConfig = (Map<String, Object>) processor.get("foreach");
        return processorConfig != null
            && hasAtLeastOneGeoipProcessor((Map<String, Object>) processorConfig.get("processor"), downloadDatabaseOnPipelineCreation);
    }

    // starts GeoIP downloader task for a single project
    private void startTask(Runnable onFailure) {
        ProjectId projectId = projectResolver.getProjectId();
        assert projectId != null : "projectId must be set before starting geoIp download task";
        persistentTasksService.sendStartRequest(
            getTaskId(projectId, projectResolver.supportsMultipleProjects()),
            GEOIP_DOWNLOADER,
            new GeoIpTaskParams(),
            MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
            ActionListener.wrap(r -> logger.debug("Started geoip downloader task"), e -> {
                Throwable t = e instanceof RemoteTransportException ? ExceptionsHelper.unwrapCause(e) : e;
                if (t instanceof ResourceAlreadyExistsException == false) {
                    logger.error("failed to create geoip downloader task", e);
                    onFailure.run();
                }
            })
        );
    }

    // stops GeoIP downloader task for a single project
    private void stopTask(Runnable onFailure) {
        assert projectResolver.getProjectId() != null : "projectId must be set before stopping geoIp download task";
        ProjectId projectId = projectResolver.getProjectId();
        ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> listener = ActionListener.wrap(
            r -> logger.debug("Stopped geoip downloader task"),
            e -> {
                Throwable t = e instanceof RemoteTransportException ? ExceptionsHelper.unwrapCause(e) : e;
                if (t instanceof ResourceNotFoundException == false) {
                    logger.error("failed to remove geoip downloader task", e);
                    onFailure.run();
                }
            }
        );
        persistentTasksService.sendRemoveRequest(
            getTaskId(projectId, projectResolver.supportsMultipleProjects()),
            MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
            ActionListener.runAfter(listener, () -> {
                IndexAbstraction databasesAbstraction = clusterService.state()
                    .projectState(projectId)
                    .metadata()
                    .getIndicesLookup()
                    .get(DATABASES_INDEX);
                if (databasesAbstraction != null) {
                    // regardless of whether DATABASES_INDEX is an alias, resolve it to a concrete index
                    Index databasesIndex = databasesAbstraction.getWriteIndex();
                    client.admin().indices().prepareDelete(databasesIndex.getName()).execute(ActionListener.wrap(rr -> {
                        // remove task reference in the map so it can be garbage collected
                        tasks.remove(projectId);
                        taskIsBootstrappedByProject.remove(projectId);
                        atLeastOneGeoipProcessorByProject.remove(projectId);
                    }, e -> {
                        Throwable t = e instanceof RemoteTransportException ? ExceptionsHelper.unwrapCause(e) : e;
                        if (t instanceof ResourceNotFoundException == false) {
                            logger.warn("failed to remove " + databasesIndex, e);
                        }
                    }));
                }
            })
        );
    }

    public GeoIpDownloader getTask(ProjectId projectId) {
        return tasks.get(projectId);
    }

    public static String getTaskId(ProjectId projectId, boolean supportsMultipleProjects) {
        return supportsMultipleProjects ? projectId + "/" + GEOIP_DOWNLOADER : GEOIP_DOWNLOADER;
    }
}
