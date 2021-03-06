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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

import static org.elasticsearch.ingest.geoip.GeoIpDownloader.GEOIP_DOWNLOADER;
import static org.elasticsearch.ingest.geoip.GeoIpDownloader.GEOIP_V2_FEATURE_FLAG_ENABLED;

/**
 * Persistent task executor that is responsible for starting {@link GeoIpDownloader} after task is allocated by master node.
 * Also bootstraps GeoIP download task on clean cluster and handles changes to the 'geoip.downloader.enabled' setting
 */
final class GeoIpDownloaderTaskExecutor extends PersistentTasksExecutor<GeoIpTaskParams> implements ClusterStateListener {

    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting("geoip.downloader.enabled", GEOIP_V2_FEATURE_FLAG_ENABLED,
        Setting.Property.Dynamic, Setting.Property.NodeScope);

    private static final Logger logger = LogManager.getLogger(GeoIpDownloader.class);

    private final Client client;
    private final HttpClient httpClient;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Settings settings;
    private final PersistentTasksService persistentTasksService;

    GeoIpDownloaderTaskExecutor(Client client, HttpClient httpClient, ClusterService clusterService, ThreadPool threadPool,
                                Settings settings) {
        super(GEOIP_DOWNLOADER, ThreadPool.Names.GENERIC);
        this.client = client;
        this.httpClient = httpClient;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.settings = settings;
        persistentTasksService = new PersistentTasksService(clusterService, threadPool, client);
        if (ENABLED_SETTING.get(settings)) {
            clusterService.addListener(this);
        }
        clusterService.getClusterSettings().addSettingsUpdateConsumer(ENABLED_SETTING, this::setEnabled);
    }

    private void setEnabled(boolean enabled) {
        if (clusterService.state().nodes().isLocalNodeElectedMaster() == false) {
            // we should only start/stop task from single node, master is the best as it will go through it anyway
            return;
        }
        if (enabled) {
            startTask(() -> {
            });
        } else {
            persistentTasksService.sendRemoveRequest(GEOIP_DOWNLOADER, ActionListener.wrap(r -> {
            }, e -> logger.error("failed to remove geoip task", e)));
        }
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, GeoIpTaskParams params, PersistentTaskState state) {
        GeoIpDownloader downloader = (GeoIpDownloader) task;
        GeoIpTaskState geoIpTaskState = state == null ? GeoIpTaskState.EMPTY : (GeoIpTaskState) state;
        downloader.setState(geoIpTaskState);
        downloader.runDownloader();
    }

    @Override
    protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
                                                 PersistentTasksCustomMetadata.PersistentTask<GeoIpTaskParams> taskInProgress,
                                                 Map<String, String> headers) {
        return new GeoIpDownloader(client, httpClient, clusterService, threadPool, settings, id, type, action,
            getDescription(taskInProgress), parentTaskId, headers);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        //bootstrap downloader after first cluster start
        clusterService.removeListener(this);
        if (event.localNodeMaster() && ENABLED_SETTING.get(event.state().getMetadata().settings())) {
            startTask(() -> clusterService.addListener(this));
        }
    }

    private void startTask(Runnable onFailure) {
        persistentTasksService.sendStartRequest(GEOIP_DOWNLOADER, GEOIP_DOWNLOADER, new GeoIpTaskParams(), ActionListener.wrap(r -> {
        }, e -> {
            if (e instanceof ResourceAlreadyExistsException == false) {
                logger.error("failed to create geoip downloader task", e);
                onFailure.run();
            }
        }));
    }
}
