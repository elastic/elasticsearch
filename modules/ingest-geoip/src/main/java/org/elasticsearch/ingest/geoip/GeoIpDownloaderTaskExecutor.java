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
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
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
import java.util.concurrent.atomic.AtomicReference;

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

    GeoIpDownloaderTaskExecutor(Client client, HttpClient httpClient, ClusterService clusterService, ThreadPool threadPool) {
        super(GEOIP_DOWNLOADER, ThreadPool.Names.GENERIC);
        this.client = new OriginSettingClient(client, IngestService.INGEST_ORIGIN);
        this.httpClient = httpClient;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.settings = clusterService.getSettings();
        persistentTasksService = new PersistentTasksService(clusterService, threadPool, client);
        clusterService.addListener(this);

        clusterService.getClusterSettings().addSettingsUpdateConsumer(ENABLED_SETTING, this::setEnabled);
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

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, GeoIpTaskParams params, PersistentTaskState state) {
        GeoIpDownloader downloader = (GeoIpDownloader) task;
        currentTask.set(downloader);
        GeoIpTaskState geoIpTaskState = state == null ? GeoIpTaskState.EMPTY : (GeoIpTaskState) state;
        downloader.setState(geoIpTaskState);
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
            headers
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

        clusterService.removeListener(this);
        if (ENABLED_SETTING.get(event.state().getMetadata().settings(), settings)) {
            startTask(() -> clusterService.addListener(this));
        } else {
            stopTask(() -> clusterService.addListener(this));
        }
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
