/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.operator.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.CountDownLatch;

public class FileSettingsService extends AbstractLifecycleComponent implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(FileSettingsService.class);

    private static final String SETTINGS_FILE_NAME = "settings.json";
    private static final String NAMESPACE = "file_settings";

    private final ClusterService clusterService;
    private final OperatorClusterStateController controller;
    private final Environment environment;

    private WatchService watchService; // null;
    private CountDownLatch watcherThreadLatch;

    private volatile FileUpdateState fileUpdateState = null;

    private volatile boolean active = false;

    public static final Setting<String> OPERATOR_DIRECTORY = Setting.simpleString(
        "path.config.operator_directory",
        "operator",
        Setting.Property.NodeScope
    );

    public FileSettingsService(ClusterService clusterService, OperatorClusterStateController controller, Environment environment) {
        this.clusterService = clusterService;
        this.controller = controller;
        this.environment = environment;
        clusterService.addListener(this);
    }

    // package private for testing
    Path operatorSettingsDir() {
        String dirPath = OPERATOR_DIRECTORY.get(environment.settings());
        return environment.configFile().toAbsolutePath().resolve(dirPath);
    }

    // package private for testing
    Path operatorSettingsFile() {
        return operatorSettingsDir().resolve(SETTINGS_FILE_NAME);
    }

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
        // We start the file watcher when we know we are master.
        // We need this additional flag, since cluster state can change after we've shutdown the service
        // causing the watcher to start again.
        this.active = true;
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
        return clusterState.nodes().getMasterNodeId().equals(clusterState.nodes().getLocalNodeId());
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ClusterState clusterState = event.state();
        setWatching(currentNodeMaster(clusterState));
    }

    private void setWatching(boolean watching) {
        if (watching) {
            startWatcher();
        } else {
            stopWatcher();
        }
    }

    // package private for testing
    boolean watching() {
        return this.watchService != null;
    }

    synchronized void startWatcher() {
        if (watching() || active == false) {
            // already watching or inactive, nothing to do
            return;
        }

        logger.info("starting file settings watcher ...");

        Path settingsDir = operatorSettingsDir();

        try {
            this.watchService = PathUtils.getDefaultFileSystem().newWatchService();
            if (Files.exists(settingsDir)) {
                Path settingsFilePath = operatorSettingsFile();
                if (Files.exists(settingsFilePath)) {
                    logger.info("found initial operator settings file [{}], applying...", settingsFilePath);
                    processFileSettings(settingsFilePath);
                }
                enableSettingsWatcher(settingsDir);
            } else {
                logger.info("operator settings directory [{}] not found, will watch for its creation...", settingsDir);
                enableSettingsWatcher(environment.configFile());
            }
        } catch (Exception e) {
            if (watchService != null) {
                try {
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

                            enableSettingsWatcher(settingsDir);

                            if (watchedFileChanged(path)) {
                                processFileSettings(path);
                            }
                        } catch (Exception e) {
                            logger.warn("unable to watch or read operator settings file", e);
                        }
                    } else {
                        key.pollEvents();
                        key.reset();
                    }
                }
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("encountered exception watching", e);
                }
                logger.info("shutting down watcher thread");
            } finally {
                watcherThreadLatch.countDown();
            }
        }, "elasticsearch[file-settings-watcher]").start();
    }

    synchronized void stopWatcher() {
        logger.debug("stopping watcher ...");
        if (watching()) {
            try {
                watchService.close();
                watcherThreadLatch.await();
            } catch (IOException | InterruptedException e) {
                logger.info("encountered exception while closing watch service", e);
            } finally {
                watchService = null;
                logger.info("watcher service stopped");
            }
        } else {
            logger.debug("file settings service already stopped");
        }
    }

    private void enableSettingsWatcher(Path settingsDir) throws IOException {
        settingsDir.register(
            watchService,
            StandardWatchEventKinds.ENTRY_MODIFY,
            StandardWatchEventKinds.ENTRY_CREATE,
            StandardWatchEventKinds.ENTRY_DELETE
        );
    }

    void processFileSettings(Path path) {
        logger.info("processing path [{}] for [{}]", path, NAMESPACE);
        try (
            XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, Files.newInputStream(path))
        ) {
            controller.process(NAMESPACE, parser, (e) -> {
                if (e != null) {
                    if (e instanceof OperatorClusterStateController.IncompatibleVersionException) {
                        logger.info(e.getMessage());
                    } else {
                        logger.error("Error processing operator settings json file", e);
                    }
                }
            });
        } catch (Exception e) {
            logger.error("Error processing operator settings json file", e);
        }
    }

    record FileUpdateState(long timestamp, String path, Object fileKey) {}
}
