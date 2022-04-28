/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.operator;

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

    private final ClusterService clusterService;
    private final Environment environment;

    private WatchService watchService; // null;
    private CountDownLatch watcherThreadLatch;

    private volatile long lastUpdatedTime = 0L;

    public static final Setting<String> OPERATOR_SETTINGS = Setting.simpleString(
        "readiness.port",
        "operatorSettings.json",
        Setting.Property.NodeScope
    );

    public FileSettingsService(ClusterService clusterService, Environment environment) {
        this.clusterService = clusterService;
        this.environment = environment;
        clusterService.addListener(this);
    }

    // package private for testing
    Path operatorSettingsFile() {
        String fileName = OPERATOR_SETTINGS.get(environment.settings());
        return environment.configFile().resolve(fileName);
    }

    // package private for testing
    static long watchedFileTimestamp(Path path) throws IOException {
        if (Files.exists(path) == false) {
            return 0;
        }
        BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);

        return attr.lastModifiedTime().toMillis();
    }

    @Override
    // We start the file watcher when we know we are master
    protected void doStart() {}

    @Override
    protected void doStop() {
        stopWatcher();
    }

    @Override
    protected void doClose() {}

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ClusterState clusterState = event.state();
        setWatching(clusterState.nodes().getMasterNodeId().equals(clusterState.nodes().getLocalNodeId()));
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
        if (watching()) {
            // already watching, nothing to do
            return;
        }

        Path path = operatorSettingsFile();
        try {
            this.lastUpdatedTime = watchedFileTimestamp(path);
            if (lastUpdatedTime > 0L) {
                processFileSettings(path);
            }
        } catch (IOException e) {
            logger.warn("Encountered I/O exception trying to read file attributes for the file based settings", e);
        }

        try {
            this.watchService = PathUtils.getDefaultFileSystem().newWatchService();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to launch a new watch service", e);
        }
        this.watcherThreadLatch = new CountDownLatch(1);

        new Thread(() -> {
            try {
                path.getParent()
                    .register(
                        watchService,
                        StandardWatchEventKinds.ENTRY_MODIFY,
                        StandardWatchEventKinds.ENTRY_CREATE,
                        StandardWatchEventKinds.ENTRY_DELETE
                    );

                logger.info("File settings service up and running...");

                WatchKey key;
                while ((key = watchService.take()) != null) {
                    // Reading and interpreting watch service events can vary from platform to platform.
                    // After we get an indication that something has changed, we check the timestamp of our desired file.
                    try {
                        long updatedTime = watchedFileTimestamp(path);
                        if (updatedTime > lastUpdatedTime) {
                            this.lastUpdatedTime = updatedTime;
                            processFileSettings(path);
                        }
                    } catch (IOException e) {
                        logger.warn("Unable to read file attributes of " + path, e);
                    }
                    key.reset();
                }
            } catch (InterruptedException | IOException e) {
                logger.error("Encountered I/O error watching " + path, e);
            } finally {
                watcherThreadLatch.countDown();
            }
        }, "elasticsearch[file-settings-watcher]").start();
    }

    synchronized void stopWatcher() {
        if (watching()) {
            try {
                watchService.close();
                watcherThreadLatch.await();
            } catch (IOException | InterruptedException e) {
                logger.info("Encountered exception while closing watch service", e);
            } finally {
                watchService = null;
                logger.info("watcher service stopped");
            }
        }
    }

    void processFileSettings(Path path) {
        // TODO: implement me
        logger.info("Settings file changed event");
    }
}
