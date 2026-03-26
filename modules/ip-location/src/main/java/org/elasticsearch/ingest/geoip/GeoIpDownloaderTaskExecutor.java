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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.ingest.geoip.GeoIpDownloader.DATABASES_INDEX;
import static org.elasticsearch.ingest.geoip.GeoIpDownloader.GEOIP_DOWNLOADER;

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
    private final DatabaseNodeService databaseNodeService;
    @FixForMultiProject(description = "These settings need to be project-scoped")
    private volatile TimeValue pollInterval;
    private volatile boolean eagerDownload;

    private final ConcurrentHashMap<ProjectId, AtomicBoolean> taskIsBootstrappedByProject = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ProjectId, GeoIpDownloader> tasks = new ConcurrentHashMap<>();
    private final ProjectResolver projectResolver;

    GeoIpDownloaderTaskExecutor(
        Client client,
        HttpClient httpClient,
        ClusterService clusterService,
        ThreadPool threadPool,
        DatabaseNodeService databaseNodeService
    ) {
        super(GEOIP_DOWNLOADER, threadPool.generic());
        this.client = new OriginSettingClient(client, IngestService.INGEST_ORIGIN);
        this.httpClient = httpClient;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.settings = clusterService.getSettings();
        this.persistentTasksService = new PersistentTasksService(clusterService, threadPool, client);
        this.databaseNodeService = databaseNodeService;
        this.pollInterval = POLL_INTERVAL_SETTING.get(settings);
        this.eagerDownload = EAGER_DOWNLOAD_SETTING.get(settings);
        this.projectResolver = client.projectResolver();
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
            startTask(ProjectId.DEFAULT, () -> {});
        } else {
            stopTask(ProjectId.DEFAULT, () -> {});
        }
    }

    @FixForMultiProject(description = "Should execute in the context of the current project after settings are project-aware")
    private void setEagerDownload(Boolean eagerDownload) {
        if (Objects.equals(this.eagerDownload, eagerDownload) == false) {
            this.eagerDownload = eagerDownload;
            GeoIpDownloader currentDownloader = getTask(ProjectId.DEFAULT);
            if (currentDownloader != null && Objects.equals(eagerDownload, Boolean.TRUE)) {
                currentDownloader.requestRunOnDemand();
            }
        }
    }

    @FixForMultiProject(description = "Should execute in the context of the current project after settings are project-aware")
    private void setPollInterval(TimeValue pollInterval) {
        if (Objects.equals(this.pollInterval, pollInterval) == false) {
            this.pollInterval = pollInterval;
            GeoIpDownloader currentDownloader = getTask(ProjectId.DEFAULT);
            if (currentDownloader != null) {
                currentDownloader.restartPeriodicRun();
            }
        }
    }

    @Override
    public boolean automaticReassignmentOnShutdown() {
        return false;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, GeoIpTaskParams params, PersistentTaskState state) {
        GeoIpDownloader downloader = (GeoIpDownloader) task;
        GeoIpTaskState geoIpTaskState = (state == null) ? GeoIpTaskState.EMPTY : (GeoIpTaskState) state;
        downloader.setState(geoIpTaskState);
        tasks.put(projectResolver.getProjectId(), downloader);
        if (ENABLED_SETTING.get(clusterService.state().metadata().settings(), settings)) {
            downloader.restartPeriodicRun();
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
            client.projectClient(projectId),
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
            () -> databaseNodeService.isDownloadRequested(projectId),
            projectId
        );
    }

    @FixForMultiProject(description = "Make sure removed project tasks are cancelled: https://elasticco.atlassian.net/browse/ES-12054")
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

        for (var projectMetadata : event.state().metadata().projects().values()) {
            ProjectId projectId = projectMetadata.id();

            // bootstrap task once iff it is not already bootstrapped
            AtomicBoolean taskIsBootstrapped = taskIsBootstrappedByProject.computeIfAbsent(projectId, k -> new AtomicBoolean(false));
            if (taskIsBootstrapped.getAndSet(true) == false) {
                if (ENABLED_SETTING.get(event.state().getMetadata().settings(), settings)) {
                    logger.debug("Bootstrapping geoip downloader task for project [{}]", projectId);
                    startTask(projectId, () -> taskIsBootstrapped.set(false));
                } else {
                    logger.debug("Stopping geoip downloader task for project [{}]", projectId);
                    stopTask(projectId, () -> taskIsBootstrapped.set(false));
                }
            }
        }
    }

    // starts GeoIP downloader task for a single project
    private void startTask(ProjectId projectId, Runnable onFailure) {
        persistentTasksService.sendProjectStartRequest(
            projectId,
            getTaskId(projectId, projectResolver.supportsMultipleProjects()),
            GEOIP_DOWNLOADER,
            new GeoIpTaskParams(),
            MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
            ActionListener.wrap(r -> logger.debug("Started geoip downloader task"), e -> {
                Throwable t = e instanceof RemoteTransportException ? ExceptionsHelper.unwrapCause(e) : e;
                if (t instanceof ResourceAlreadyExistsException == false) {
                    logger.warn("failed to create geoip downloader task", e);
                    onFailure.run();
                }
            })
        );
    }

    // stops GeoIP downloader task for a single project
    private void stopTask(ProjectId projectId, Runnable onFailure) {
        ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> listener = ActionListener.wrap(
            r -> logger.debug("Stopped geoip downloader task"),
            e -> {
                Throwable t = e instanceof RemoteTransportException ? ExceptionsHelper.unwrapCause(e) : e;
                if (t instanceof ResourceNotFoundException == false) {
                    logger.warn("failed to remove geoip downloader task", e);
                    onFailure.run();
                }
            }
        );
        persistentTasksService.sendProjectRemoveRequest(
            projectId,
            getTaskId(projectId, projectResolver.supportsMultipleProjects()),
            MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
            ActionListener.runAfter(listener, () -> {
                IndexAbstraction databasesAbstraction = clusterService.state()
                    .metadata()
                    .getProject(projectId)
                    .getIndicesLookup()
                    .get(DATABASES_INDEX);
                if (databasesAbstraction != null) {
                    // regardless of whether DATABASES_INDEX is an alias, resolve it to a concrete index
                    Index databasesIndex = databasesAbstraction.getWriteIndex();
                    client.projectClient(projectId)
                        .admin()
                        .indices()
                        .prepareDelete(databasesIndex.getName())
                        .execute(ActionListener.wrap(rr -> {
                            tasks.remove(projectId);
                            taskIsBootstrappedByProject.remove(projectId);
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
