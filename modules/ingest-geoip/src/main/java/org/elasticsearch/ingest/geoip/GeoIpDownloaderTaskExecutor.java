/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.ingest.geoip.GeoIpDownloader.DATABASES_INDEX;
import static org.elasticsearch.ingest.geoip.GeoIpDownloader.GEOIP_DOWNLOADER;
import static org.elasticsearch.ingest.geoip.GeoIpProcessor.Factory.downloadDatabaseOnPipelineCreation;

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
        Setting.Property.NodeScope
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
    private final AtomicReference<GeoIpDownloader> currentTask = new AtomicReference<>();
    private volatile TimeValue pollInterval;
    private volatile boolean eagerDownload;
    private volatile boolean atLeastOneGeoipProcessor;
    private final AtomicBoolean taskIsBootstrapped = new AtomicBoolean(false);

    GeoIpDownloaderTaskExecutor(Client client, HttpClient httpClient, ClusterService clusterService, ThreadPool threadPool) {
        super(GEOIP_DOWNLOADER, ThreadPool.Names.GENERIC);
        this.client = new OriginSettingClient(client, IngestService.INGEST_ORIGIN);
        this.httpClient = httpClient;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.settings = clusterService.getSettings();
        persistentTasksService = new PersistentTasksService(clusterService, threadPool, client);
        this.pollInterval = POLL_INTERVAL_SETTING.get(settings);
        this.eagerDownload = EAGER_DOWNLOAD_SETTING.get(settings);
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

    private void setEagerDownload(Boolean eagerDownload) {
        if (Objects.equals(this.eagerDownload, eagerDownload) == false) {
            this.eagerDownload = eagerDownload;
            GeoIpDownloader currentDownloader = getCurrentTask();
            if (currentDownloader != null && Objects.equals(eagerDownload, Boolean.TRUE)) {
                currentDownloader.requestReschedule();
            }
        }
    }

    private void setPollInterval(TimeValue pollInterval) {
        if (Objects.equals(this.pollInterval, pollInterval) == false) {
            this.pollInterval = pollInterval;
            GeoIpDownloader currentDownloader = getCurrentTask();
            if (currentDownloader != null) {
                currentDownloader.requestReschedule();
            }
        }
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, GeoIpTaskParams params, PersistentTaskState state) {
        GeoIpDownloader downloader = (GeoIpDownloader) task;
        GeoIpTaskState geoIpTaskState = state == null ? GeoIpTaskState.EMPTY : (GeoIpTaskState) state;
        downloader.setState(geoIpTaskState);
        currentTask.set(downloader);
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
            () -> atLeastOneGeoipProcessor
        );
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait for state recovered
            return;
        }

        DiscoveryNode masterNode = event.state().nodes().getMasterNode();
        if (masterNode == null || masterNode.getVersion().before(Version.V_7_14_0)) {
            // wait for master to be upgraded so it understands geoip task
            return;
        }

        if (taskIsBootstrapped.getAndSet(true) == false) {
            this.atLeastOneGeoipProcessor = hasAtLeastOneGeoipProcessor(event.state());
            if (ENABLED_SETTING.get(event.state().getMetadata().settings(), settings)) {
                startTask(() -> taskIsBootstrapped.set(false));
            } else {
                stopTask(() -> taskIsBootstrapped.set(false));
            }
        }

        if (event.metadataChanged() == false) {
            return;
        }

        boolean hasIndicesChanges = event.previousState().metadata().indices().equals(event.state().metadata().indices()) == false;
        boolean hasIngestPipelineChanges = event.changedCustomMetadataSet().contains(IngestMetadata.TYPE);

        if (hasIngestPipelineChanges || hasIndicesChanges) {
            boolean newAtLeastOneGeoipProcessor = hasAtLeastOneGeoipProcessor(event.state());
            if (newAtLeastOneGeoipProcessor && atLeastOneGeoipProcessor == false) {
                atLeastOneGeoipProcessor = true;
                logger.trace("Scheduling runDownloader because a geoip processor has been added");
                GeoIpDownloader currentDownloader = getCurrentTask();
                if (currentDownloader != null) {
                    currentDownloader.requestReschedule();
                }
            } else {
                atLeastOneGeoipProcessor = newAtLeastOneGeoipProcessor;
            }
        }
    }

    static boolean hasAtLeastOneGeoipProcessor(ClusterState clusterState) {
        if (pipelineConfigurationsWithGeoIpProcessor(clusterState, true).isEmpty() == false) {
            return true;
        }

        List<String> checkReferencedPipelines = pipelineConfigurationsWithGeoIpProcessor(clusterState, false).stream()
            .map(PipelineConfiguration::getId)
            .collect(Collectors.toList());

        if (checkReferencedPipelines.isEmpty()) {
            return false;
        }

        return clusterState.getMetadata().indices().values().stream().anyMatch(indexMetadata -> {
            String defaultPipeline = IndexSettings.DEFAULT_PIPELINE.get(indexMetadata.getSettings());
            String finalPipeline = IndexSettings.FINAL_PIPELINE.get(indexMetadata.getSettings());
            return checkReferencedPipelines.contains(defaultPipeline) || checkReferencedPipelines.contains(finalPipeline);
        });
    }

    @SuppressWarnings("unchecked")
    private static List<PipelineConfiguration> pipelineConfigurationsWithGeoIpProcessor(
        ClusterState state,
        boolean downloadDatabaseOnPipelineCreation
    ) {
        List<PipelineConfiguration> pipelineDefinitions = IngestService.getPipelines(state);
        return pipelineDefinitions.stream().filter(pipelineConfig -> {
            List<Map<String, Object>> processors = (List<Map<String, Object>>) pipelineConfig.getConfigAsMap().get(Pipeline.PROCESSORS_KEY);
            return hasAtLeastOneGeoipProcessor(processors, downloadDatabaseOnPipelineCreation);
        }).collect(Collectors.toList());
    }

    private static boolean hasAtLeastOneGeoipProcessor(List<Map<String, Object>> processors, boolean downloadDatabaseOnPipelineCreation) {
        return processors != null && processors.stream().anyMatch(p -> hasAtLeastOneGeoipProcessor(p, downloadDatabaseOnPipelineCreation));
    }

    @SuppressWarnings("unchecked")
    private static boolean hasAtLeastOneGeoipProcessor(Map<String, Object> processor, boolean downloadDatabaseOnPipelineCreation) {
        if (processor == null) {
            return false;
        }

        if (processor.containsKey(GeoIpProcessor.TYPE)) {
            Map<String, Object> processorConfig = (Map<String, Object>) processor.get(GeoIpProcessor.TYPE);
            return downloadDatabaseOnPipelineCreation(processorConfig) == downloadDatabaseOnPipelineCreation;
        }

        return isProcessorWithOnFailureGeoIpProcessor(processor, downloadDatabaseOnPipelineCreation)
            || isForeachProcessorWithGeoipProcessor(processor, downloadDatabaseOnPipelineCreation);
    }

    @SuppressWarnings("unchecked")
    private static boolean isProcessorWithOnFailureGeoIpProcessor(
        Map<String, Object> processor,
        boolean downloadDatabaseOnPipelineCreation
    ) {
        return processor != null
            && processor.values()
                .stream()
                .anyMatch(
                    value -> value instanceof Map
                        && hasAtLeastOneGeoipProcessor(
                            ((Map<String, List<Map<String, Object>>>) value).get("on_failure"),
                            downloadDatabaseOnPipelineCreation
                        )
                );
    }

    @SuppressWarnings("unchecked")
    private static boolean isForeachProcessorWithGeoipProcessor(Map<String, Object> processor, boolean downloadDatabaseOnPipelineCreation) {
        return processor.containsKey("foreach")
            && hasAtLeastOneGeoipProcessor(
                ((Map<String, Map<String, Object>>) processor.get("foreach")).get("processor"),
                downloadDatabaseOnPipelineCreation
            );
    }

    private void startTask(Runnable onFailure) {
        persistentTasksService.sendStartRequest(
            GEOIP_DOWNLOADER,
            GEOIP_DOWNLOADER,
            new GeoIpTaskParams(),
            ActionListener.wrap(r -> logger.debug("Started geoip downloader task"), e -> {
                Throwable t = e instanceof RemoteTransportException ? e.getCause() : e;
                if (t instanceof ResourceAlreadyExistsException == false) {
                    logger.error("failed to create geoip downloader task", e);
                    onFailure.run();
                }
            })
        );
    }

    private void stopTask(Runnable onFailure) {
        ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> listener = ActionListener.wrap(
            r -> logger.debug("Stopped geoip downloader task"),
            e -> {
                Throwable t = e instanceof RemoteTransportException ? e.getCause() : e;
                if (t instanceof ResourceNotFoundException == false) {
                    logger.error("failed to remove geoip downloader task", e);
                    onFailure.run();
                }
            }
        );
        persistentTasksService.sendRemoveRequest(GEOIP_DOWNLOADER, ActionListener.runAfter(listener, () -> {
            IndexAbstraction databasesAbstraction = clusterService.state().metadata().getIndicesLookup().get(DATABASES_INDEX);
            if (databasesAbstraction != null) {
                // regardless of whether DATABASES_INDEX is an alias, resolve it to a concrete index
                Index databasesIndex = databasesAbstraction.getWriteIndex();
                client.admin().indices().prepareDelete(databasesIndex.getName()).execute(ActionListener.wrap(rr -> {}, e -> {
                    Throwable t = e instanceof RemoteTransportException ? e.getCause() : e;
                    if (t instanceof ResourceNotFoundException == false) {
                        logger.warn("failed to remove " + databasesIndex, e);
                    }
                }));
            }
        }));
    }

    public GeoIpDownloader getCurrentTask() {
        return currentTask.get();
    }
}
