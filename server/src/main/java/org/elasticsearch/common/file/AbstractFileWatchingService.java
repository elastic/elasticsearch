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
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.reservedstate.service.FileChangedListener;

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

/**
 * A skeleton service for watching and reacting to a single file changing on disk
 *
 * <p>A file watching service watches for changes in a particular file on disk. There
 * are three assumptions about the file structure:</p>
 * <ol>
 *     <li>The file itself may or may not exist.</li>
 *     <li>The file's parent directory may or may not exist.</li>
 *     <li>The directory above the file's parent directory must always exist.</li>
 * </ol>
 *
 * <p>For example, if the watched file is under /usr/elastic/elasticsearch/conf/special/settings.yml,
 * then /usr/elastic/elasticsearch/conf/ must exist, but special/ and special/settings.yml may
 * be created, updated, or deleted during runtime.</p>
 *
 * <p>What this class does not do is define what should happen after the file changes.
 * An implementation class should override {@link #processFileChanges()} to define
 * the correct behavior.</p>
 */
public abstract class AbstractFileWatchingService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(AbstractFileWatchingService.class);
    private static final int REGISTER_RETRY_COUNT = 5;
    private final ClusterService clusterService;
    private final Path watchedFileDir;
    private final Path watchedFile;
    private final List<FileChangedListener> eventListeners;
    private WatchService watchService; // null;
    private Thread watcherThread;
    private FileUpdateState fileUpdateState;
    private WatchKey settingsDirWatchKey;
    private WatchKey configDirWatchKey;
    private volatile boolean active = false;

    public AbstractFileWatchingService(ClusterService clusterService, Path watchedFile) {
        this.clusterService = clusterService;
        this.watchedFile = watchedFile;
        this.watchedFileDir = watchedFile.getParent();
        this.eventListeners = new CopyOnWriteArrayList<>();
    }

    /**
     * Any implementation of this class must implement this method in order
     * to define what happens once the watched file changes.
     *
     * @throws IOException if there is an error reading the file itself
     * @throws ExecutionException if there is an issue while applying the changes from the file
     * @throws InterruptedException if the file processing is interrupted by another thread.
     */
    protected abstract void processFileChanges() throws InterruptedException, ExecutionException, IOException;

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

    public final void addFileChangedListener(FileChangedListener listener) {
        eventListeners.add(listener);
    }

    public final Path watchedFileDir() {
        return this.watchedFileDir;
    }

    public final Path watchedFile() {
        return this.watchedFile;
    }

    @Override
    public final void clusterChanged(ClusterChangedEvent event) {
        ClusterState clusterState = event.state();
        if (clusterState.nodes().isLocalNodeElectedMaster()) {
            startWatcher(clusterState);
        } else if (event.previousState().nodes().isLocalNodeElectedMaster()) {
            stopWatcher();
        }
    }

    @Override
    protected final void doStart() {
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
    protected final void doStop() {
        this.active = false;
        logger.debug("Stopping file watching service");
        stopWatcher();
    }

    @Override
    protected final void doClose() {}

    public final boolean watching() {
        return watcherThread != null;
    }

    // platform independent way to tell if a file changed
    // we compare the file modified timestamp, the absolute path (symlinks), and file id on the system
    final boolean watchedFileChanged(Path path) throws IOException {
        if (Files.exists(path) == false) {
            return false;
        }

        FileUpdateState previousUpdateState = fileUpdateState;

        BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
        fileUpdateState = new FileUpdateState(attr.lastModifiedTime().toMillis(), path.toRealPath().toString(), attr.fileKey());

        return (previousUpdateState == null || previousUpdateState.equals(fileUpdateState) == false);
    }

    private synchronized void startWatcher(ClusterState clusterState) {
        if (watching() || active == false) {
            refreshExistingFileStateIfNeeded(clusterState);

            return;
        }

        logger.info("starting file watcher ...");

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
                logger.debug("watched directory [{}] not found, will watch for its creation...", settingsDirPath);
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

        watcherThread = new Thread(this::watcherThread, "elasticsearch[file-watcher[" + watchedFile + "]]");
        watcherThread.start();
    }

    protected final void watcherThread() {
        try {
            logger.info("file settings service up and running [tid={}]", Thread.currentThread().getId());

            Path path = watchedFile();

            if (Files.exists(path)) {
                logger.debug("found initial operator settings file [{}], applying...", path);
                processSettingsAndNotifyListeners();
            } else {
                // Notify everyone we don't have any initial file settings
                for (var listener : eventListeners) {
                    listener.watchedFileChanged();
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

    final synchronized void stopWatcher() {
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
            logger.trace("file watch service already stopped");
        }
    }

    // package private for testing
    final WatchKey enableDirectoryWatcher(WatchKey previousKey, Path settingsDir) throws IOException, InterruptedException {
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

    void processSettingsAndNotifyListeners() throws InterruptedException {
        try {
            processFileChanges();
            for (var listener : eventListeners) {
                listener.watchedFileChanged();
            }
        } catch (IOException | ExecutionException e) {
            logger.error(() -> "Error processing watched file: " + watchedFile(), e);
        }
    }

    // package private for testing
    long retryDelayMillis(int failedCount) {
        assert failedCount < 31; // don't let the count overflow
        return 100 * (1 << failedCount) + Randomness.get().nextInt(10); // add a bit of jitter to avoid two processes in lockstep
    }

    /**
     * Holds information about the last known state of the file we watched. We use this
     * class to determine if a file has been changed.
     */
    private record FileUpdateState(long timestamp, String path, Object fileKey) {}
}
