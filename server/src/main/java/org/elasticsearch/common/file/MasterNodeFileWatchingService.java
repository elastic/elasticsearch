/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.file;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;

public abstract class MasterNodeFileWatchingService extends AbstractFileWatchingService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(MasterNodeFileWatchingService.class);

    private final ClusterService clusterService;
    private volatile boolean active = false;

    protected MasterNodeFileWatchingService(ClusterService clusterService, Path watchedFile) {
        super(watchedFile);
        this.clusterService = clusterService;
    }

    @Override
    protected void doStart() {
        // We start the file watcher when we know we are master from a cluster state change notification.
        // We need the additional active flag, since cluster state can change after we've shutdown the service
        // causing the watcher to start again.
        this.active = Files.exists(watchedFileDir().getParent());
        if (active == false) {
            // we don't have a config directory, we can't possibly launch the file settings service
            return;
        }
        if (DiscoveryNode.isMasterNode(clusterService.getSettings())) {
            clusterService.addListener(this);
        }
    }

    @Override
    protected void doStop() {
        this.active = false;
        super.doStop();
    }

    @Override
    public final void clusterChanged(ClusterChangedEvent event) {
        ClusterState clusterState = event.state();
        if (clusterState.nodes().isLocalNodeElectedMaster()) {
            synchronized (this) {
                if (watching() || active == false) {
                    refreshExistingFileStateIfNeeded(clusterState);
                    return;
                }
                startWatcher();
            }
        } else if (event.previousState().nodes().isLocalNodeElectedMaster()) {
            stopWatcher();
        }
    }

    /**
     * 'Touches' the settings file so the file watcher will re-processes it.
     * <p>
     * The file processing is asynchronous, the cluster state or the file must be already updated such that
     * the version information in the file is newer than what's already saved as processed in the
     * cluster state.
     *
     * For snapshot restores we first must restore the snapshot and then force a refresh, since the cluster state
     * metadata version must be reset to 0 and saved in the cluster state.
     */
    private void refreshExistingFileStateIfNeeded(ClusterState clusterState) {
        if (watching()) {
            if (shouldRefreshFileState(clusterState) && Files.exists(watchedFile())) {
                try {
                    Files.setLastModifiedTime(watchedFile(), FileTime.from(Instant.now()));
                } catch (IOException e) {
                    logger.warn("encountered I/O error trying to update file settings timestamp", e);
                }
            }
        }
    }

    /**
     * There may be an indication in cluster state that the file we are watching
     * should be re-processed: for example, after cluster state has been restored
     * from a snapshot. By default, we do nothing, but this method should be overridden
     * if different behavior is desired.
     * @param clusterState State of the cluster
     * @return false, by default
     */
    protected boolean shouldRefreshFileState(ClusterState clusterState) {
        return false;
    }
}
