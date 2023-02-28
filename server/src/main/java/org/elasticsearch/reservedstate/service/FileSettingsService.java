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
import org.elasticsearch.common.Randomness;
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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
public class FileSettingsService extends AbstractLifecycleComponent implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(FileSettingsService.class);

    public static final String SETTINGS_FILE_NAME = "settings.json";
    public static final String NAMESPACE = "file_settings";
    private static final int REGISTER_RETRY_COUNT = 5;

    private final ClusterService clusterService;
    private final ReservedClusterStateService stateService;
    private final Path operatorSettingsDir;

    private WatchService watchService; // null;
    private Thread watcherThread;
    private FileUpdateState fileUpdateState;
    private WatchKey settingsDirWatchKey;
    private WatchKey configDirWatchKey;

    private volatile boolean active = false;

    public static final String OPERATOR_DIRECTORY = "operator";

    private final List<FileSettingsChangedListener> eventListeners;

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
        this.eventListeners = new CopyOnWriteArrayList<>();
    }

    public Path operatorSettingsDir() {
        return operatorSettingsDir;
    }

    public Path operatorSettingsFile() {
        return operatorSettingsDir.resolve(SETTINGS_FILE_NAME);
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

    public boolean watching() {
        return watcherThread != null;
    }

    synchronized void startWatcher(ClusterState clusterState) {
        if (watching() || active == false) {
            refreshExistingFileStateIfNeeded(clusterState);

            return;
        }

        logger.info("starting file settings watcher ...");

        /*
         * We essentially watch for two things:
         *  - the creation of the operator directory (if it doesn't exist), symlink changes to the operator directory
         *  - any changes to files inside the operator directory if it exists, filtering for settings.json
         */
        try {
            Path settingsDirPath = operatorSettingsDir();
            this.watchService = settingsDirPath.getParent().getFileSystem().newWatchService();
            if (Files.exists(settingsDirPath)) {
                settingsDirWatchKey = enableSettingsWatcher(settingsDirWatchKey, settingsDirPath);
            } else {
                logger.debug("operator settings directory [{}] not found, will watch for its creation...", settingsDirPath);
            }
            // We watch the config directory always, even if initially we had an operator directory
            // it can be deleted and created later. The config directory never goes away, we only
            // register it once for watching.
            configDirWatchKey = enableSettingsWatcher(configDirWatchKey, settingsDirPath.getParent());
        } catch (Exception e) {
            if (watchService != null) {
                try {
                    // this will also close any keys
                    this.watchService.close();
                } catch (Exception ce) {
                    e.addSuppressed(ce);
                } finally {
                    this.watchService = null;
                }
            }

            throw new IllegalStateException("unable to launch a new watch service", e);
        }

        watcherThread = new Thread(this::watcherThread, "elasticsearch[file-settings-watcher]");
        watcherThread.start();
    }

    private void watcherThread() {
        try {
            logger.info("file settings service up and running [tid={}]", Thread.currentThread().getId());

            Path path = operatorSettingsFile();

            if (Files.exists(path)) {
                logger.debug("found initial operator settings file [{}], applying...", path);
                processSettingsAndNotifyListeners();
            } else {
                // Notify everyone we don't have any initial file settings
                for (var listener : eventListeners) {
                    listener.settingsChanged();
                }
            }

            WatchKey key;
            while ((key = watchService.take()) != null) {
                /*
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
                 * real path of our desired file. We don't actually care what changed, we just re-check ourselves.
                 */
                Path settingsPath = operatorSettingsDir();
                if (Files.exists(settingsPath)) {
                    try {
                        if (logger.isDebugEnabled()) {
                            key.pollEvents().forEach(e -> logger.debug("{}:{}", e.kind().toString(), e.context().toString()));
                        } else {
                            key.pollEvents();
                        }
                        key.reset();

                        // We re-register the settings directory watch key, because we don't know
                        // if the file name maps to the same native file system file id. Symlinks
                        // are one potential cause of inconsistency here, since their handling by
                        // the WatchService is platform dependent.
                        settingsDirWatchKey = enableSettingsWatcher(settingsDirWatchKey, settingsPath);

                        if (watchedFileChanged(path)) {
                            processSettingsAndNotifyListeners();
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
        }
    }

    // package private for testing
    void processSettingsAndNotifyListeners() throws InterruptedException {
        try {
            processFileSettings(operatorSettingsFile()).get();
            for (var listener : eventListeners) {
                listener.settingsChanged();
            }
        } catch (ExecutionException e) {
            logger.error("Error processing operator settings json file", e.getCause());
        }
    }

    synchronized void stopWatcher() {
        if (watching()) {
            logger.debug("stopping watcher ...");
            // make sure watch service is closed whatever
            // this will also close any outstanding keys
            try (var ws = watchService) {
                watcherThread.interrupt();
                watcherThread.join();

                // make sure any keys are closed - if watchService.close() throws, it may not close the keys first
                if (configDirWatchKey != null) {
                    configDirWatchKey.cancel();
                }
                if (settingsDirWatchKey != null) {
                    settingsDirWatchKey.cancel();
                }
            } catch (IOException e) {
                logger.warn("encountered exception while closing watch service", e);
            } catch (InterruptedException interruptedException) {
                logger.info("interrupted while closing the watch service", interruptedException);
            } finally {
                watcherThread = null;
                settingsDirWatchKey = null;
                configDirWatchKey = null;
                watchService = null;
                logger.info("watcher service stopped");
            }
        } else {
            logger.trace("file settings service already stopped");
        }
    }

    // package private for testing
    long retryDelayMillis(int failedCount) {
        assert failedCount < 31; // don't let the count overflow
        return 100 * (1 << failedCount) + Randomness.get().nextInt(10); // add a bit of jitter to avoid two processes in lockstep
    }

    // package private for testing
    WatchKey enableSettingsWatcher(WatchKey previousKey, Path settingsDir) throws IOException, InterruptedException {
        if (previousKey != null) {
            previousKey.cancel();
        }
        int retryCount = 0;

        do {
            try {
                return settingsDir.register(
                    watchService,
                    StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_DELETE
                );
            } catch (IOException e) {
                if (retryCount == REGISTER_RETRY_COUNT - 1) {
                    throw e;
                }
                Thread.sleep(retryDelayMillis(retryCount));
                retryCount++;
            }
        } while (true);
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
