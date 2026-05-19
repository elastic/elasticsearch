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
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.ingest.geoip.GeoIpDownloader.DATABASES_INDEX;
import static org.elasticsearch.ingest.geoip.GeoIpDownloader.GEOIP_DOWNLOADER;

/**
 * Persistent task executor that is responsible for starting {@link GeoIpDownloader} after task is allocated by master node.
 *
 * Task lifecycle (start/stop) is managed externally by the {@link
 * org.elasticsearch.persistent.PersistentTaskLifecycleManager}, which reconciles the
 * task presence in cluster state based on {@link #ENABLED_SETTING}.
 *
 * This executor additionally tracks task instances per project. The {@code .geoip_databases} index is
 * cleaned up by {@link #deleteGeoIpDatabasesIndex} when the lifecycle manager removes the task from cluster state.
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
        // Until ProjectScopedSettings#get is implemented, this is just a placeholder
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

    private static final Logger logger = LogManager.getLogger(GeoIpDownloaderTaskExecutor.class);

    private final Client client;
    private final HttpClient httpClient;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Settings settings;
    @FixForMultiProject(description = "These settings need to be project-scoped")
    private volatile TimeValue pollInterval;
    private volatile boolean eagerDownload;
    private final ConcurrentHashMap<ProjectId, GeoIpDownloader> tasks = new ConcurrentHashMap<>();
    private final ProjectResolver projectResolver;

    GeoIpDownloaderTaskExecutor(Client client, HttpClient httpClient, ClusterService clusterService, ThreadPool threadPool) {
        super(GEOIP_DOWNLOADER, threadPool.generic());
        this.client = new OriginSettingClient(client, IngestGeoIpPlugin.ORIGIN);
        this.httpClient = httpClient;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.settings = clusterService.getSettings();
        this.pollInterval = POLL_INTERVAL_SETTING.get(settings);
        this.eagerDownload = EAGER_DOWNLOAD_SETTING.get(settings);
        this.projectResolver = client.projectResolver();
    }

    /**
     * Completes the initialization of the GeoIpDownloaderTaskExecutor by registering several listeners.
     */
    public void init() {
        clusterService.addListener(this);
        setUpSettingsConsumers(clusterService);
    }

    @FixForMultiProject(description = "Should execute in the context of the current project after settings are project-aware")
    private void setUpSettingsConsumers(ClusterService clusterService) {
        clusterService.getClusterSettings().addSettingsUpdateConsumer(EAGER_DOWNLOAD_SETTING, this::setEagerDownload);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(POLL_INTERVAL_SETTING, this::setPollInterval);
    }

    /**
     * Returns the task ID used for the geoip downloader task within a given project.
     */
    public String getTaskIdForProject(ProjectId projectId) {
        return getTaskId(projectId, projectResolver.supportsMultipleProjects());
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
            projectId
        );
    }

    /**
     * On each cluster state update:
     * <ol>
     *   <li>Cleans up stale task entries when projects are removed or tasks
     *       are removed from cluster state.</li>
     *   <li>When a project transitions from "no registered IP location consumers" (custom absent or empty)
     *       to having at least one registered consumer in {@link IpLocationDownloadConsumers}, triggers an
     *       immediate download run so the newly-registered consumer doesn't have to wait until the next
     *       periodic poll. Subsequent changes within the non-empty state (additional consumers added, or
     *       one of multiple consumers removed) do not trigger an on-demand run: the downloader is already
     *       polling, and only "any consumer present?" — not which or how many — affects what it fetches.</li>
     * </ol>
     *
     * The persistent task is started and stopped by {@link org.elasticsearch.persistent.PersistentTaskLifecycleManager}.
     * After the task is removed from cluster state (successfully or already absent), that manager invokes
     * {@link #deleteGeoIpDatabasesIndex} to drop the {@code .geoip_databases} index when appropriate.
     */
    @FixForMultiProject(description = "Make sure removed project tasks are cancelled: https://elasticco.atlassian.net/browse/ES-12054")
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().nodes().getMasterNode() == null || event.state().clusterRecovered() == false) {
            return;
        }
        if (event.metadataChanged() == false) {
            return;
        }

        final var projects = event.state().metadata().projects();
        final var previousProjects = event.previousState().metadata().projects();

        tasks.keySet()
            .removeIf(
                p -> projects.containsKey(p) == false
                    || PersistentTasksCustomMetadata.getTaskWithId(projects.get(p), getTaskIdForProject(p)) == null
            );

        for (var taskEntry : tasks.entrySet()) {
            // looking for a state transition from either non-existing project or IpLocationDownloadConsumers, or empty
            // IpLocationDownloadConsumers, to existing and non-empty IpLocationDownloadConsumers
            ProjectId projectId = taskEntry.getKey();
            IpLocationDownloadConsumers current = projects.get(projectId)
                .custom(IpLocationDownloadConsumers.TYPE, IpLocationDownloadConsumers.EMPTY);
            if (current.hasConsumers() == false) {
                continue;
            }
            ProjectMetadata previousProject = previousProjects.get(projectId);
            IpLocationDownloadConsumers previous = previousProject == null
                ? IpLocationDownloadConsumers.EMPTY
                : previousProject.custom(IpLocationDownloadConsumers.TYPE, IpLocationDownloadConsumers.EMPTY);
            if (previous.hasConsumers()) {
                continue;
            }
            taskEntry.getValue().requestRunOnDemand();
        }
    }

    /**
     * Deletes the {@code .geoip_databases} index for the given project after the geoip downloader task has
     * been removed. Called from the {@code onRemove} callback of
     * {@link org.elasticsearch.persistent.PersistentTaskLifecycleManager}.
     * <p>
     * Safe to run concurrently with no in-flight bulks: {@link AbstractGeoIpDownloader} defers
     * {@code markAsCompleted()} until any in-flight {@code runDownloader()} has returned, so by the time
     * the persistent-tasks framework removes the task entry from cluster state and fires this callback,
     * the executing node has stopped writing to {@code .geoip_databases}. The DELETE therefore cannot
     * race with an auto-create from a straggler bulk.
     */
    void deleteGeoIpDatabasesIndex(ProjectId projectId) {
        ProjectMetadata project = clusterService.state().metadata().projects().get(projectId);
        if (project == null) {
            return;
        }
        IndexAbstraction databasesAbstraction = project.getIndicesLookup().get(DATABASES_INDEX);
        if (databasesAbstraction == null) {
            return;
        }
        Index databasesIndex = databasesAbstraction.getWriteIndex();
        client.projectClient(projectId)
            .admin()
            .indices()
            .prepareDelete(databasesIndex.getName())
            .execute(ActionListener.wrap(rr -> {}, e -> {
                Throwable t = ExceptionsHelper.unwrapCause(e);
                if (t instanceof ResourceNotFoundException == false) {
                    logger.warn("failed to remove " + databasesIndex, e);
                }
            }));
    }

    public GeoIpDownloader getTask(ProjectId projectId) {
        return tasks.get(projectId);
    }

    /// TODO (ES-14525): no need to include project id in the task id, we should just use the same name for all tasks
    public static String getTaskId(ProjectId projectId, boolean supportsMultipleProjects) {
        return supportsMultipleProjects ? projectId + "/" + GEOIP_DOWNLOADER : GEOIP_DOWNLOADER;
    }
}
