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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

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
public class FileSettingsService extends AbstractLifecycleComponent implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(FileSettingsService.class);

    private static final String SETTINGS_FILE_NAME = "settings.json";
    public static final String NAMESPACE = "file_settings";

    private final ClusterService clusterService;
    private final ReservedClusterStateService stateService;
    private final Path operatorSettingsDir;

    private WatchService watchService; // null;
    private CountDownLatch watcherThreadLatch;

    private volatile FileUpdateState fileUpdateState = null;
    private volatile WatchKey settingsDirWatchKey = null;
    private volatile WatchKey configDirWatchKey = null;

    private volatile boolean active = false;
    private volatile boolean initialState = true;

    public static final String OPERATOR_DIRECTORY = "operator";

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
        this.operatorSettingsDir = environment.configFile().toAbsolutePath().resolve(OPERATOR_DIRECTORY);
    }

    // package private for testing
    Path operatorSettingsDir() {
        return operatorSettingsDir;
    }

    // package private for testing
    Path operatorSettingsFile() {
        return operatorSettingsDir().resolve(SETTINGS_FILE_NAME);
    }

    // platform independent way to tell if a file changed
    // we compare the file modified timestamp, the absolute path (symlinks), and file id on the system
    boolean watchedFileChanged(Path path) throws IOException {
        if (Files.exists(path) == false) {
            return false;
        }

        FileUpdateState previousUpdateState = fileUpdateState;

        BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
        fileUpdateState = new FileUpdateState(attr.lastModifiedTime().toMillis(), path.toRealPath().toString(), attr.fileKey());

        return (previousUpdateState == null || previousUpdateState.equals(fileUpdateState) == false);
    }

    @Override
    protected void doStart() {
        // We start the file watcher when we know we are master from a cluster state change notification.
        // We need the additional active flag, since cluster state can change after we've shutdown the service
        // causing the watcher to start again.
        this.active = Files.exists(operatorSettingsDir().getParent());
        startIfMaster(clusterService.state());
        clusterService.addListener(this);
    }

    @Override
    protected void doStop() {
        this.active = false;
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
            startWatcher(clusterState, initialState);
        } else {
            stopWatcher();
        }
        initialState = false;
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
        if (watching() && Files.exists(operatorSettingsFile())) {
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
            if (fileSettingsMetadata != null && fileSettingsMetadata.version() == 0L && Files.exists(operatorSettingsFile())) {
                try {
                    Files.setLastModifiedTime(operatorSettingsFile(), FileTime.from(Instant.now()));
                } catch (IOException e) {
                    logger.warn("encountered I/O error trying to update file settings timestamp", e);
                }
            }
        }
    }

    // package private for testing
    boolean watching() {
        return this.watchService != null;
    }

    private void cleanupWatchKeys() {
        if (settingsDirWatchKey != null) {
            settingsDirWatchKey.cancel();
            settingsDirWatchKey = null;
        }
        if (configDirWatchKey != null) {
            configDirWatchKey.cancel();
            configDirWatchKey = null;
        }
    }

    synchronized void startWatcher(ClusterState clusterState, boolean onStartup) {
        if (watching() || active == false) {
            refreshExistingFileStateIfNeeded(clusterState);

            return;
        }

        logger.info("starting file settings watcher ...");

        Path settingsDir = operatorSettingsDir();

        /*
         * We essentially watch for two things:
         *  - the creation of the operator directory (if it doesn't exist), symlink changes to the operator directory
         *  - any changes to files inside the operator directory if it exists, filtering for settings.json
         */
        try {
            this.watchService = operatorSettingsDir().getParent().getFileSystem().newWatchService();
            if (Files.exists(settingsDir)) {
                Path settingsFilePath = operatorSettingsFile();
                if (Files.exists(settingsFilePath)) {
                    logger.debug("found initial operator settings file [{}], applying...", settingsFilePath);
                    // we make a distinction here for startup, so that if we had operator settings before the node started
                    // we would fail startup.
                    processFileSettings(settingsFilePath, (e) -> {
                        if (onStartup) {
                            throw new FileSettingsStartupException("Error applying operator settings", e);
                        } else {
                            logger.error("Error processing operator settings json file", e);
                        }
                    }).await();
                }
                settingsDirWatchKey = enableSettingsWatcher(settingsDirWatchKey, settingsDir);
            } else {
                logger.debug("operator settings directory [{}] not found, will watch for its creation...", settingsDir);
            }
            // We watch the config directory always, even if initially we had an operator directory
            // it can be deleted and created later. The config directory never goes away, we only
            // register it once for watching.
            configDirWatchKey = enableSettingsWatcher(configDirWatchKey, operatorSettingsDir().getParent());
        } catch (Exception e) {
            if (watchService != null) {
                try {
                    cleanupWatchKeys();
                    this.watchService.close();
                } catch (Exception ignore) {} finally {
                    this.watchService = null;
                }
            }

            throw new IllegalStateException("unable to launch a new watch service", e);
        }

        this.watcherThreadLatch = new CountDownLatch(1);

        new Thread(() -> {
            try {
                logger.info("file settings service up and running [tid={}]", Thread.currentThread().getId());

                WatchKey key;
                while ((key = watchService.take()) != null) {
                    /**
                     * Reading and interpreting watch service events can vary from platform to platform. E.g:
                     * MacOS symlink delete and set (rm -rf operator && ln -s <path to>/file_settings/ operator):
                     *     ENTRY_MODIFY:operator
                     *     ENTRY_CREATE:settings.json
                     *     ENTRY_MODIFY:settings.json
                     * Linux in Docker symlink delete and set (rm -rf operator && ln -s <path to>/file_settings/ operator):
                     *     ENTRY_CREATE:operator
                     * Windows
                     *     ENTRY_CREATE:operator
                     *     ENTRY_MODIFY:operator
                     * After we get an indication that something has changed, we check the timestamp, file id,
                     * real path of our desired file.
                     */
                    if (Files.exists(settingsDir)) {
                        try {
                            Path path = operatorSettingsFile();

                            if (logger.isDebugEnabled()) {
                                key.pollEvents().stream().forEach(e -> logger.debug("{}:{}", e.kind().toString(), e.context().toString()));
                            }

                            key.pollEvents();
                            key.reset();

                            // We re-register the settings directory watch key, because we don't know
                            // if the file name maps to the same native file system file id. Symlinks
                            // are one potential cause of inconsistency here, since their handling by
                            // the WatchService is platform dependent.
                            settingsDirWatchKey = enableSettingsWatcher(settingsDirWatchKey, settingsDir);

                            if (watchedFileChanged(path)) {
                                processFileSettings(path, (e) -> logger.error("Error processing operator settings json file", e)).await();
                            }
                        } catch (IOException e) {
                            logger.warn("encountered I/O error while watching file settings", e);
                        }
                    } else {
                        key.pollEvents();
                        key.reset();
                    }
                }
            } catch (ClosedWatchServiceException | InterruptedException expected) {
                logger.info("shutting down watcher thread");
            } catch (Exception e) {
                logger.error("shutting down watcher thread with exception", e);
            } finally {
                watcherThreadLatch.countDown();
            }
        }, "elasticsearch[file-settings-watcher]").start();
    }

    synchronized void stopWatcher() {
        logger.debug("stopping watcher ...");
        if (watching()) {
            try {
                cleanupWatchKeys();
                fileUpdateState = null;
                watchService.close();
                if (watcherThreadLatch != null) {
                    watcherThreadLatch.await();
                }
            } catch (IOException e) {
                logger.warn("encountered exception while closing watch service", e);
            } catch (InterruptedException interruptedException) {
                logger.info("interrupted while closing the watch service", interruptedException);
            } finally {
                watchService = null;
                logger.info("watcher service stopped");
            }
        } else {
            logger.debug("file settings service already stopped");
        }
    }

    private WatchKey enableSettingsWatcher(WatchKey previousKey, Path settingsDir) throws IOException {
        if (previousKey != null) {
            previousKey.cancel();
        }
        return settingsDir.register(
            watchService,
            StandardWatchEventKinds.ENTRY_MODIFY,
            StandardWatchEventKinds.ENTRY_CREATE,
            StandardWatchEventKinds.ENTRY_DELETE
        );
    }

    CountDownLatch processFileSettings(Path path, Consumer<Exception> errorHandler) throws IOException {
        CountDownLatch waitForCompletion = new CountDownLatch(1);
        logger.info("processing path [{}] for [{}]", path, NAMESPACE);
        try (
            var fis = Files.newInputStream(path);
            var bis = new BufferedInputStream(fis);
            var parser = JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bis)
        ) {
            stateService.process(NAMESPACE, parser, (e) -> {
                try {
                    if (e != null) {
                        errorHandler.accept(e);
                    }
                } finally {
                    waitForCompletion.countDown();
                }
            });
        }

        return waitForCompletion;
    }

    /**
     * Holds information about the last known state of the file we watched. We use this
     * class to determine if a file has been changed.
     */
    record FileUpdateState(long timestamp, String path, Object fileKey) {}

    /**
     * Error subclass that is thrown when we encounter a fatal error while applying
     * the operator cluster state at Elasticsearch boot time.
     */
    public static class FileSettingsStartupException extends RuntimeException {
        public FileSettingsStartupException(String message, Throwable t) {
            super(message, t);
        }
    }
}
