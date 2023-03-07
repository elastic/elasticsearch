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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

public abstract class AbstractFileWatchingService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(FileSettingsService.class);
    protected static final int REGISTER_RETRY_COUNT = 5;
    protected final ClusterService clusterService;
    protected final ReservedClusterStateService stateService;
    protected final Path watchedFileDir;
    protected final Path watchedFile;
    protected final List<FileSettingsChangedListener> eventListeners;
    protected WatchService watchService; // null;
    protected Thread watcherThread;
    protected FileSettingsService.FileUpdateState fileUpdateState;
    protected WatchKey settingsDirWatchKey;
    protected WatchKey configDirWatchKey;
    protected volatile boolean active = false;

    public AbstractFileWatchingService(
        ClusterService clusterService,
        ReservedClusterStateService stateService,
        Environment environment,
        String watchedDirName,
        String watchedFileName
    ) {
        this.clusterService = clusterService;
        this.stateService = stateService;
        this.watchedFileDir = environment.configFile().toAbsolutePath().resolve(watchedDirName);
        this.watchedFile = watchedFileDir.resolve(watchedFileName);
        this.eventListeners = new CopyOnWriteArrayList<>();
    }

    // platform independent way to tell if a file changed
    // we compare the file modified timestamp, the absolute path (symlinks), and file id on the system
    boolean watchedFileChanged(Path path) throws IOException {
        if (Files.exists(path) == false) {
            return false;
        }

        FileSettingsService.FileUpdateState previousUpdateState = fileUpdateState;

        BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
        fileUpdateState = new FileSettingsService.FileUpdateState(
            attr.lastModifiedTime().toMillis(),
            path.toRealPath().toString(),
            attr.fileKey()
        );

        return (previousUpdateState == null || previousUpdateState.equals(fileUpdateState) == false);
    }

    public Path watchedFileDir() {
        return this.watchedFileDir;
    }

    public Path watchedFile() {
        return this.watchedFile;
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

    // TODO[wrb]: update logging
    @Override
    protected void doStop() {
        this.active = false;
        logger.debug("Stopping file settings service");
        stopWatcher();
    }

    @Override
    protected void doClose() {}

    public boolean watching() {
        return watcherThread != null;
    }

    // TODO[wrb]: update logging
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

    protected boolean currentNodeMaster(ClusterState clusterState) {
        return clusterState.nodes().getLocalNodeId().equals(clusterState.nodes().getMasterNodeId());
    }

    // package private for testing
    long retryDelayMillis(int failedCount) {
        assert failedCount < 31; // don't let the count overflow
        return 100 * (1 << failedCount) + Randomness.get().nextInt(10); // add a bit of jitter to avoid two processes in lockstep
    }

    // package private for testing
    WatchKey enableDirectoryWatcher(WatchKey previousKey, Path settingsDir) throws IOException, InterruptedException {
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

    // TODO[wrb]: rename
    public void addFileSettingsChangedListener(FileSettingsChangedListener listener) {
        eventListeners.add(listener);
    }

    // TODO[wrb]: update logging
    // package private for testing
    void processSettingsAndNotifyListeners() throws InterruptedException {
        try {
            processFileSettings(watchedFile()).get();
            for (var listener : eventListeners) {
                listener.settingsChanged();
            }
        } catch (ExecutionException e) {
            logger.error("Error processing operator settings json file", e.getCause());
        }
    }

    protected void watcherThread() {
        try {
            logger.info("file settings service up and running [tid={}]", Thread.currentThread().getId());

            Path path = watchedFile();

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
                Path settingsPath = watchedFileDir();
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
                        settingsDirWatchKey = enableDirectoryWatcher(settingsDirWatchKey, settingsPath);

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

    // TODO[wrb]: to superclass
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ClusterState clusterState = event.state();
        startIfMaster(clusterState);
    }

    // TODO[wrb]: to superclass
    private void startIfMaster(ClusterState clusterState) {
        if (currentNodeMaster(clusterState)) {
            startWatcher(clusterState);
        } else {
            stopWatcher();
        }
    }

    protected abstract void refreshExistingFileStateIfNeeded(ClusterState clusterState);

    // TODO[wrb]: update logging
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
            Path settingsDirPath = watchedFileDir();
            this.watchService = settingsDirPath.getParent().getFileSystem().newWatchService();
            if (Files.exists(settingsDirPath)) {
                settingsDirWatchKey = enableDirectoryWatcher(settingsDirWatchKey, settingsDirPath);
            } else {
                logger.debug("operator settings directory [{}] not found, will watch for its creation...", settingsDirPath);
            }
            // We watch the config directory always, even if initially we had an operator directory
            // it can be deleted and created later. The config directory never goes away, we only
            // register it once for watching.
            configDirWatchKey = enableDirectoryWatcher(configDirWatchKey, settingsDirPath.getParent());
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

    abstract PlainActionFuture<Void> processFileSettings(Path path);

    /**
     * Holds information about the last known state of the file we watched. We use this
     * class to determine if a file has been changed.
     */
    record FileUpdateState(long timestamp, String path, Object fileKey) {}
}
