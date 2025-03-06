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
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.core.FixForMultiProject;

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

/**
 * A skeleton service for watching and reacting to files changing in a directory
 *
 * <p>A file watching service watches for changes to a particular directory. There
 * are two assumptions about the file structure:</p>
 * <ol>
 *     <li>The directory may or may not exist.</li>
 *     <li>The directory's parent directory must always exist.</li>
 * </ol>
 *
 * <p>For example, if the watched directory is {@code /usr/elastic/elasticsearch/conf/special},
 * then {@code /usr/elastic/elasticsearch/conf} must exist, but {@code special} and any files in that directory
 * may be created, updated, or deleted during runtime.</p>
 *
 * <p>What this class does not do is define what should happen after the directory changes.
 * An implementation class should override {@link #processFileChanges(Path)} to define
 * the correct behavior.</p>
 */
public abstract class AbstractFileWatchingService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(AbstractFileWatchingService.class);
    private static final int REGISTER_RETRY_COUNT = 5;
    private final Path settingsDir;
    private final Map<Path, FileUpdateState> fileUpdateState = new HashMap<>();
    private WatchService watchService; // null;
    private Thread watcherThread;
    private WatchKey settingsDirWatchKey;
    private WatchKey configDirWatchKey;

    public AbstractFileWatchingService(Path settingsDir) {
        if (Files.exists(settingsDir) && Files.isDirectory(settingsDir) == false) {
            throw new IllegalArgumentException("settingsDir should be a directory");
        }
        this.settingsDir = settingsDir;
    }

    /**
     * Any implementation of this class must implement this method in order
     * to define what happens once a file in the watched directory changes.
     *
     * @param file  the full path of the file that has changed inside the watched directory
     * @throws IOException if there is an error reading the file itself
     * @throws ExecutionException if there is an issue while applying the changes from the file
     * @throws InterruptedException if the file processing is interrupted by another thread.
     */
    protected abstract void processFileChanges(Path file) throws InterruptedException, ExecutionException, IOException;

    protected abstract void processInitialFilesMissing() throws InterruptedException, ExecutionException, IOException;

    /**
     * Defaults to generic {@link #processFileChanges(Path)} behavior.
     * An implementation can override this to define different file handling when the directory is processed during
     * initial service start.
     */
    protected void processFileOnServiceStart(Path file) throws IOException, ExecutionException, InterruptedException {
        processFileChanges(file);
    }

    public final Path watchedFileDir() {
        return this.settingsDir;
    }

    @Override
    protected void doStart() {
        startWatcher();
    }

    @Override
    protected void doStop() {
        logger.debug("Stopping file watching service");
        stopWatcher();
    }

    @Override
    protected final void doClose() {}

    public final boolean watching() {
        return watcherThread != null;
    }

    private static FileUpdateState readFileUpdateState(Path path) throws IOException {
        BasicFileAttributes attr;
        try {
            attr = Files.readAttributes(path, BasicFileAttributes.class);
        } catch (NoSuchFileException e) {
            // file doesn't exist anymore
            return null;
        }

        return new FileUpdateState(attr.lastModifiedTime().toMillis(), path.toRealPath().toString(), attr.fileKey());
    }

    // platform independent way to tell if a file changed
    // we compare the file modified timestamp, the absolute path (symlinks), and file id on the system
    @FixForMultiProject // what do we do when a file is removed?
    final boolean fileChanged(Path path) throws IOException {
        FileUpdateState newFileState = readFileUpdateState(path);
        if (newFileState == null) {
            fileUpdateState.remove(path);
            return false;
        } else {
            FileUpdateState previousUpdateState = fileUpdateState.put(path, newFileState);

            return (previousUpdateState == null || previousUpdateState.equals(newFileState) == false);
        }
    }

    protected final synchronized void startWatcher() {
        if (Files.exists(settingsDir.getParent()) == false) {
            logger.warn("File watcher for [{}] cannot start because parent directory does not exist", settingsDir);
            return;
        }

        logger.info("starting file watcher ...");

        /*
         * We essentially watch for two things:
         *  - the creation of the operator directory (if it doesn't exist), symlink changes to the operator directory
         *  - any changes to files inside the operator directory if it exists
         */
        try {
            this.watchService = settingsDir.getParent().getFileSystem().newWatchService();
            if (Files.exists(settingsDir)) {
                settingsDirWatchKey = enableDirectoryWatcher(settingsDirWatchKey, settingsDir);
            } else {
                logger.debug("watched directory [{}] not found, will watch for its creation...", settingsDir);
            }
            // We watch the config directory always, even if initially we had an operator directory
            // it can be deleted and created later. The config directory never goes away, we only
            // register it once for watching.
            configDirWatchKey = enableDirectoryWatcher(configDirWatchKey, settingsDir.getParent());
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

        watcherThread = new Thread(this::watcherThread, "elasticsearch[file-watcher[" + settingsDir + "]]");
        watcherThread.start();
    }

    @FixForMultiProject // handle file removals
    protected final void watcherThread() {
        try {
            logger.info("file settings service up and running [tid={}]", Thread.currentThread().getId());

            if (Files.exists(settingsDir)) {
                try (Stream<Path> files = Files.list(settingsDir)) {
                    var f = files.iterator();
                    if (f.hasNext() == false) {
                        // no files in directory
                        processInitialFilesMissing();
                    } else {
                        do {
                            Path next = f.next();
                            var state = readFileUpdateState(next);
                            if (state == null) {
                                // file has disappeared in the meantime. Just skip it
                                continue;
                            }
                            fileUpdateState.put(next, state);
                            logger.debug("found initial settings file [{}], applying...", next);
                            processOnServiceStart(next);
                        } while (f.hasNext());
                    }
                }
            } else {
                // no directory, no files...
                processInitialFilesMissing();
            }

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
             * real path of the recorded file. We don't actually care what changed, we just re-check ourselves.
             */
            WatchKey key;
            while ((key = watchService.take()) != null) {
                List<WatchEvent<?>> events = key.pollEvents();
                if (logger.isDebugEnabled()) {
                    logger.debug("Processing events from {}", key.watchable());
                    events.forEach(e -> logger.debug("{}:{}", e.kind(), e.context()));
                }
                key.reset();

                if (key == settingsDirWatchKey) {
                    // there may be multiple events for the same file - we only want to re-read once
                    Set<Path> processedFiles = new HashSet<>();
                    for (WatchEvent<?> e : events) {
                        Path fullFile = settingsDir.resolve(e.context().toString());
                        if (processedFiles.add(fullFile)) {
                            if (fileChanged(fullFile)) {
                                process(fullFile);
                            }
                        }
                    }
                } else if (key == configDirWatchKey) {
                    if (Files.exists(settingsDir)) {
                        // We re-register the settings directory watch key, because we don't know
                        // if the file name maps to the same native file system file id. Symlinks
                        // are one potential cause of inconsistency here, since their handling by
                        // the WatchService is platform dependent.
                        logger.debug("Re-registering settings directory watch");
                        settingsDirWatchKey = enableDirectoryWatcher(settingsDirWatchKey, settingsDir);

                        // re-read the settings directory, and ping for any changes
                        try (Stream<Path> files = Files.list(settingsDir)) {
                            for (var f = files.iterator(); f.hasNext();) {
                                Path file = f.next();
                                if (fileChanged(file)) {
                                    process(file);
                                }
                            }
                        }
                    } else if (settingsDirWatchKey != null) {
                        settingsDirWatchKey.cancel();
                    }
                } else {
                    logger.warn("Received events for unknown watch key {}", key);
                }
            }
        } catch (ClosedWatchServiceException | InterruptedException expected) {
            logger.info("shutting down watcher thread");
        } catch (Exception e) {
            logger.error("shutting down watcher thread with exception", e);
        }
    }

    protected final synchronized void stopWatcher() {
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

    void processOnServiceStart(Path file) throws InterruptedException {
        try {
            processFileOnServiceStart(file);
        } catch (IOException | ExecutionException e) {
            onProcessFileChangesException(file, e);
        }
    }

    void process(Path file) throws InterruptedException {
        try {
            processFileChanges(file);
        } catch (IOException | ExecutionException e) {
            onProcessFileChangesException(file, e);
        }
    }

    /**
     * Called for checked exceptions only.
     */
    protected void onProcessFileChangesException(Path file, Exception e) {
        logger.error(() -> "Error processing file notification: " + file, e);
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
