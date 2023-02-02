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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.xcontent.XContentType.JSON;

/**
 * File based settings applier service which watches an 'operator` directory inside the config directory.
 * <p>
 * // TODO[wrb]: update javadoc
 * The service expects that the operator directory will contain a single JSON file with all the settings that
 * need to be applied to the cluster state. The name of the file is fixed to be settings.json. The operator
 * directory name can be configured by setting the 'path.config.operator_directory' in the node properties.
 * <p>
 * The {@link FileSettingsService} is active always, but enabled only on the current master node. We register
 * the service as a listener to cluster state changes, so that we can enable the file watcher thread when this
 * node becomes a master node.
 */
// this will become file watch service, and it can take file-action objects?
public class FileSettingsService extends AbstractLifecycleComponent implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(FileSettingsService.class);

    // TODO[wrb]: settings file may be moved for initialization logic
    public static final String SETTINGS_FILE_NAME = "settings.json";
    public static final String NAMESPACE = "file_settings";

    private final ClusterService clusterService;
    private final ReservedClusterStateService stateService;

    // TODO[wrb]: NEXT- move thread to FileWatchService
    private Thread watcherThread;

    public static final String OPERATOR_DIRECTORY = "operator";
    private final List<FileSettingsChangedListener> eventListeners;

    private final FileWatchService fileWatchService;

    /**
     * Constructs the {@link FileSettingsService}
     *
     * @param clusterService so we can register ourselves as a cluster state change listener
     * @param stateService an instance of the immutable cluster state controller, so we can perform the cluster state changes
     * @param environment we need the environment to pull the location of the config and operator directories
     */
    public FileSettingsService(ClusterService clusterService, ReservedClusterStateService stateService, Environment environment) {
        this.clusterService = clusterService;
        this.stateService = stateService;
        this.eventListeners = new CopyOnWriteArrayList<>();

        // we have listeners and state service, now we should create file watcher service
        // and register the operator settings

        // TODO[wrb] this state should go in the file watch service
        Path operatorSettings = environment.configFile().toAbsolutePath().resolve(OPERATOR_DIRECTORY);
        fileWatchService = new FileWatchService(operatorSettings, SETTINGS_FILE_NAME);
    }

    // TODO[wrb]: update for testing
    public Path operatorSettingsDir() {
        return fileWatchService.operatorSettingsDir;
    }

    // TODO[wrb]: move to fileWatchService
    public Path operatorSettingsFile() {
        return fileWatchService.operatorSettingsDir.resolve(fileWatchService.settingsFileName);
    }

    // TODO[wrb]: rework usages to get rid of this
    public List<Path> operatorSettingsFiles() {
        return List.of(operatorSettingsFile());
    }

    // TODO[wrb]: inline/remove?
    boolean watchedFileChanged(Path path) throws IOException {
        return this.fileWatchService().watchedFileChanged(path);
    }

    // visible for testing
    FileWatchService fileWatchService() {
        return fileWatchService;
    }

    @Override
    protected void doStart() {
        this.fileWatchService.doStart();
        if (this.fileWatchService.isActive() == false) {
            // we don't have a config directory, we can't possibly launch the file settings service
            return;
        }
        if (DiscoveryNode.isMasterNode(clusterService.getSettings())) {
            clusterService.addListener(this);
        }
    }

    @Override
    protected void doStop() {
        this.fileWatchService.setActive(false);
        logger.debug("Stopping file settings service");
        stopWatcher();
    }

    @Override
    protected void doClose() {}

    private boolean currentNodeMaster(ClusterState clusterState) {
        return clusterState.nodes().getLocalNodeId().equals(clusterState.nodes().getMasterNodeId());
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ClusterState clusterState = event.state();
        startIfMaster(clusterState);
    }

    private void startIfMaster(ClusterState clusterState) {
        if (currentNodeMaster(clusterState)) {
            startWatcher(clusterState);
        } else {
            stopWatcher();
        }
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
        assert currentNodeMaster(clusterState);

        ReservedStateMetadata fileSettingsMetadata = clusterState.metadata().reservedStateMetadata().get(NAMESPACE);

        // When we restore from a snapshot we remove the reserved cluster state for file settings,
        // since we don't know the current operator configuration, e.g. file settings could be disabled
        // on the target cluster. If file settings exist and the cluster state has lost it's reserved
        // state for the "file_settings" namespace, we touch our file settings file to cause it to re-process the file.
        if (watching() && operatorSettingsFiles().stream().anyMatch(Files::exists)) {
            if (fileSettingsMetadata != null) {
                ReservedStateMetadata withResetVersion = new ReservedStateMetadata.Builder(fileSettingsMetadata).version(0L).build();
                mdBuilder.put(withResetVersion);
            }
        } else if (fileSettingsMetadata != null) {
            mdBuilder.removeReservedState(fileSettingsMetadata);
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
            ReservedStateMetadata fileSettingsMetadata = clusterState.metadata().reservedStateMetadata().get(NAMESPACE);
            // We check if the version was reset to 0, and force an update if a file exists. This can happen in situations
            // like snapshot restores.
            // TODO[wrb]: this should call the file watch service for manipulating the files
            if (fileSettingsMetadata != null
                && fileSettingsMetadata.version() == 0L
                && operatorSettingsFiles().stream().anyMatch(Files::exists)) {
                operatorSettingsFiles().forEach(file -> {
                    try {
                        Files.setLastModifiedTime(file, FileTime.from(Instant.now()));
                    } catch (IOException e) {
                        logger.warn("encountered I/O error trying to update file settings timestamp", e);
                    }
                });
            }
        }
    }

    // TODO[wrb]: thread management should be moved to watch service
    public boolean watching() {
        return fileWatchService.watching();
    }

    // need to start all the watchers here
    synchronized void startWatcher(ClusterState clusterState) {
        if (watching() || this.fileWatchService.isActive() == false) {
            refreshExistingFileStateIfNeeded(clusterState);

            return;
        }

        logger.info("starting file settings watcher ...");

        fileWatchService.startWatcher(() -> {
            try {
                processSettingsAndNotifyListeners();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, () -> {
            for (var listener : eventListeners) {
                listener.settingsChanged();
            }
        });
    }

    // package private for testing
    void processSettingsAndNotifyListeners() throws InterruptedException {
        try {
            for (Path settingsFile : operatorSettingsFiles()) {
                processFileSettings(settingsFile).get();
            }
            for (var listener : eventListeners) {
                listener.settingsChanged();
            }
        } catch (ExecutionException e) {
            logger.error("Error processing operator settings json file", e.getCause());
        }
    }

    // need to stop all the watchers
    void stopWatcher() {
        fileWatchService.stopWatcher();
    }

    PlainActionFuture<Void> processFileSettings(Path path) {
        PlainActionFuture<Void> completion = PlainActionFuture.newFuture();
        logger.info("processing path [{}] for [{}]", path, NAMESPACE);
        try (
            var fis = Files.newInputStream(path);
            var bis = new BufferedInputStream(fis);
            var parser = JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bis)
        ) {
            stateService.process(NAMESPACE, parser, (e) -> completeProcessing(e, completion));
        } catch (Exception e) {
            completion.onFailure(e);
        }

        return completion;
    }

    private void completeProcessing(Exception e, PlainActionFuture<Void> completion) {
        if (e != null) {
            completion.onFailure(e);
        } else {
            completion.onResponse(null);
        }
    }

    /**
     * Holds information about the last known state of the file we watched. We use this
     * class to determine if a file has been changed.
     */
    record FileUpdateState(long timestamp, String path, Object fileKey) {}

    public void addFileSettingsChangedListener(FileSettingsChangedListener listener) {
        eventListeners.add(listener);
    }
}
