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
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.component.AbstractLifecycleComponent;

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.stream.Stream;

// Settings have a path, a file update state, and a watch key
// TODO[wrb]: is this really a service? does it need start/stop logic? Since it manages a thread,
//   probably...
public class FileWatchService extends AbstractLifecycleComponent {

    private static final int REGISTER_RETRY_COUNT = 5;
    private static final Logger logger = LogManager.getLogger(FileWatchService.class);

    final Path operatorSettingsDir;
    final String settingsFileName;
    FileSettingsService.FileUpdateState fileUpdateState;
    private WatchService watchService; // null;
    WatchKey settingsDirWatchKey;
    WatchKey configDirWatchKey; // there is only one config dir
    private volatile boolean active = false;

    FileWatchService(Path operatorSettingsDir, String settingsFileName) {
        this.operatorSettingsDir = operatorSettingsDir;
        this.settingsFileName = settingsFileName;
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

    WatchService watchService() {
        return this.watchService;
    }

    @Override
    protected void doStart() {
        // We start the file watcher when we know we are master from a cluster state change notification.
        // We need the additional active flag, since cluster state can change after we've shutdown the service
        // causing the watcher to start again.
        this.setActive(Stream.of(this).map(e -> e.operatorSettingsDir.getParent()).anyMatch(Files::exists));

        // TODO[wrb]: remove
        if (this.isActive() == false) {
            // we don't have a config directory, we can't possibly launch the file settings service
            return;
        }
    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {
        watchService = null;
    }

    // TODO[wrb]: remove when possible
    synchronized void setActive(boolean flag) {
        active = flag;
    }

    // TODO[wrb]: remove when possible
    boolean isActive() {
        return active;
    }

    synchronized void startWatcher() {
        /*
         * We essentially watch for two things:
         *  - the creation of the operator directory (if it doesn't exist), symlink changes to the operator directory
         *  - any changes to files inside the operator directory if it exists, filtering for settings.json
         */
        try {
            // TODO[wrb]: can we assume that all our settings are on the same filesystem? for now throw error if this isn't the case
            // TODO[wrb]: delegate this down to the file watch service
            List<Path> settingsDirPathList = List.of(operatorSettingsDir);
            assert settingsDirPathList.stream().map(Path::getParent).map(Path::getFileSystem).distinct().count() == 1;
            Path settingsDirPath = settingsDirPathList.stream().findAny().orElseThrow();
            this.watchService = settingsDirPath.getParent().getFileSystem().newWatchService();
            if (Files.exists(settingsDirPath)) {
                this.settingsDirWatchKey = enableSettingsWatcher(settingsDirWatchKey, settingsDirPath);
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
    }

    synchronized void stopWatcher() {
        settingsDirWatchKey = null;
        configDirWatchKey = null;
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

    // package private for testing
    long retryDelayMillis(int failedCount) {
        assert failedCount < 31; // don't let the count overflow
        return 100 * (1 << failedCount) + Randomness.get().nextInt(10); // add a bit of jitter to avoid two processes in lockstep
    }

    // Get this thing down into FileWatchService somehow
    void watcherThread(Runnable processOperation, Runnable listenOperation) {
        try {
            logger.info("file settings service up and running [tid={}]", Thread.currentThread().getId());

            List<Path> operatorSettingsFiles = List.of(operatorSettingsDir.resolve(settingsFileName));
            if (operatorSettingsFiles.stream().anyMatch(Files::exists)) {
                logger.debug("found initial operator settings file [{}], applying...", operatorSettingsFiles);
                processOperation.run();
            } else {
                // Notify everyone we don't have any initial file settings
                listenOperation.run();
            }

            WatchKey key;
            while ((key = watchService().take()) != null) {
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
                List<Path> settingsPathList = List.of(operatorSettingsDir);

                if (settingsPathList.stream().anyMatch(Files::exists)) {
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
                        settingsDirWatchKey = enableSettingsWatcher(settingsDirWatchKey, operatorSettingsDir);

                        for (Path path : operatorSettingsFiles) {
                            if (watchedFileChanged(path)) {
                                processOperation.run();
                                break;
                            }
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

}
