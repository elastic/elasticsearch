/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.file;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.gateway.GatewayService;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.stream.Stream;

public abstract class MasterNodeFileWatchingService extends AbstractFileWatchingService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(MasterNodeFileWatchingService.class);

    private final ClusterService clusterService;
    private volatile boolean active = false;

    protected MasterNodeFileWatchingService(ClusterService clusterService, Path settingsDir) {
        super(settingsDir);
        this.clusterService = clusterService;
    }

    @Override
    protected void doStart() {
        // We start the file watcher when we know we are master from a cluster state change notification.
        // We need the additional active flag, since cluster state can change after we've shutdown the service
        // causing the watcher to start again.
        this.active = filesExists(watchedFileDir().getParent());
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
        if (clusterState.nodes().isLocalNodeElectedMaster()
            && clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false) {
            synchronized (this) {
                if (active == false) {
                    return;
                }
                if (watching()) {
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
     * 'Touches' the settings files so the file watcher will re-processes them.
     * <p>
     * The file processing is asynchronous, the cluster state or the file must be already updated such that
     * the version information in the file is newer than what's already saved as processed in the
     * cluster state.
     *
     * For snapshot restores we first must restore the snapshot and then force a refresh, since the cluster state
     * metadata version must be reset to 0 and saved in the cluster state.
     */
    @FixForMultiProject // do we want to re-process everything all at once?
    private void refreshExistingFileStateIfNeeded(ClusterState clusterState) {
        if (shouldRefreshFileState(clusterState)) {
            try (Stream<Path> files = filesList(watchedFileDir())) {
                FileTime time = FileTime.from(Instant.now());
                for (var it = files.iterator(); it.hasNext();) {
                    filesSetLastModifiedTime(it.next(), time);
                }
            } catch (IOException e) {
                logger.warn("encountered I/O error trying to update file settings timestamp", e);
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
