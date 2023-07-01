/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reservedstate.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.file.AbstractFileWatchingService;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.xcontent.XContentType.JSON;

/**
 * File based settings applier service which watches an 'operator` directory inside the config directory.
 * <p>
 * The service expects that the operator directory will contain a single JSON file with all the settings that
 * need to be applied to the cluster state. The name of the file is fixed to be settings.json. The operator
 * directory name can be configured by setting the 'path.config.operator_directory' in the node properties.
 * <p>
 * The {@link FileSettingsService} is active always, but enabled only on the current master node. We register
 * the service as a listener to cluster state changes, so that we can enable the file watcher thread when this
 * node becomes a master node.
 */
public class FileSettingsService extends AbstractFileWatchingService {

    private static final Logger logger = LogManager.getLogger(FileSettingsService.class);

    public static final String SETTINGS_FILE_NAME = "settings.json";
    public static final String NAMESPACE = "file_settings";
    public static final String OPERATOR_DIRECTORY = "operator";
    private final ReservedClusterStateService stateService;

    /**
     * Constructs the {@link FileSettingsService}
     *
     * @param clusterService so we can register ourselves as a cluster state change listener
     * @param stateService an instance of the immutable cluster state controller, so we can perform the cluster state changes
     * @param environment we need the environment to pull the location of the config and operator directories
     */
    public FileSettingsService(ClusterService clusterService, ReservedClusterStateService stateService, Environment environment) {
        super(clusterService, environment.configFile().toAbsolutePath().resolve(OPERATOR_DIRECTORY).resolve(SETTINGS_FILE_NAME));
        this.stateService = stateService;
    }

    /**
     * Used by snapshot restore service {@link org.elasticsearch.snapshots.RestoreService} to prepare the reserved
     * state of the snapshot for the current cluster.
     * <p>
     * If the current cluster where we are restoring the snapshot into has any operator file based settings, we'll
     * reset the reserved state version to 0.
     * <p>
     * If there's no file based settings file in this cluster, we'll remove all state reservations for
     * file based settings from the cluster state.
     * @param clusterState the cluster state before snapshot restore
     * @param mdBuilder the current metadata builder for the new cluster state
     */
    public void handleSnapshotRestore(ClusterState clusterState, Metadata.Builder mdBuilder) {
        assert clusterState.nodes().isLocalNodeElectedMaster();

        ReservedStateMetadata fileSettingsMetadata = clusterState.metadata().reservedStateMetadata().get(NAMESPACE);

        // When we restore from a snapshot we remove the reserved cluster state for file settings,
        // since we don't know the current operator configuration, e.g. file settings could be disabled
        // on the target cluster. If file settings exist and the cluster state has lost it's reserved
        // state for the "file_settings" namespace, we touch our file settings file to cause it to re-process the file.
        if (watching() && Files.exists(watchedFile())) {
            if (fileSettingsMetadata != null) {
                ReservedStateMetadata withResetVersion = new ReservedStateMetadata.Builder(fileSettingsMetadata).version(0L).build();
                mdBuilder.put(withResetVersion);
            }
        } else if (fileSettingsMetadata != null) {
            mdBuilder.removeReservedState(fileSettingsMetadata);
        }
    }

    /**
     * If the file settings metadata version is set to zero, then we have restored from
     * a snapshot and must reprocess the file.
     * @param clusterState State of the cluster
     * @return true if file settings metadata version is exactly 0, false otherwise.
     */
    @Override
    protected boolean shouldRefreshFileState(ClusterState clusterState) {
        // We check if the version was reset to 0, and force an update if a file exists. This can happen in situations
        // like snapshot restores.
        ReservedStateMetadata fileSettingsMetadata = clusterState.metadata().reservedStateMetadata().get(NAMESPACE);
        return fileSettingsMetadata != null && fileSettingsMetadata.version() == 0L;
    }

    /**
     * Read settings and pass them to {@link ReservedClusterStateService} for application
     *
     * @throws IOException if there is an error reading the file itself
     * @throws ExecutionException if there is an issue while applying the changes from the file
     * @throws InterruptedException if the file processing is interrupted by another thread.
     */
    @Override
    protected void processFileChanges() throws ExecutionException, InterruptedException, IOException {
        PlainActionFuture<Void> completion = PlainActionFuture.newFuture();
        logger.info("processing path [{}] for [{}]", watchedFile(), NAMESPACE);
        try (
            var fis = Files.newInputStream(watchedFile());
            var bis = new BufferedInputStream(fis);
            var parser = JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bis)
        ) {
            stateService.process(NAMESPACE, parser, (e) -> completeProcessing(e, completion));
        }
        completion.get();
    }

    private void completeProcessing(Exception e, PlainActionFuture<Void> completion) {
        if (e != null) {
            completion.onFailure(e);
        } else {
            completion.onResponse(null);
        }
    }
}
