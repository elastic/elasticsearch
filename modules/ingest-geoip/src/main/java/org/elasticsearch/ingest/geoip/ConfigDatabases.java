/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.ingest.geoip;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

/**
 * Keeps track of user provided databases in the ES_HOME/config/ingest-geoip directory.
 * This directory is monitored and files updates are picked up and may cause databases being loaded or removed at runtime.
 */
final class ConfigDatabases implements Closeable {

    private static final Logger logger = LogManager.getLogger(ConfigDatabases.class);

    private final GeoIpCache cache;
    private final Path geoipConfigDir;

    // private final ConcurrentMap<ProjectId, ConcurrentMap<String, DatabaseReaderLazyLoader>> configDatabasesByProject;
    private final ConcurrentMap<String, DatabaseReaderLazyLoader> configDatabases;

    ConfigDatabases(Environment environment, GeoIpCache cache) {
        this(environment.configDir().resolve("ingest-geoip"), cache);
    }

    ConfigDatabases(Path geoipConfigDir, GeoIpCache cache) {
        this.cache = cache;
        this.geoipConfigDir = geoipConfigDir;
        this.configDatabases = new ConcurrentHashMap<>();
    }

    void initialize(ResourceWatcherService resourceWatcher) throws IOException {
        configDatabases.putAll(initConfigDatabases());

        FileWatcher watcher = new FileWatcher(geoipConfigDir);
        watcher.addListener(new GeoipDirectoryListener());
        resourceWatcher.add(watcher, ResourceWatcherService.Frequency.HIGH);

        logger.debug("initialized config databases [{}] and watching [{}] for changes", configDatabases.keySet(), geoipConfigDir);
    }

    DatabaseReaderLazyLoader getDatabase(String name) {
        return configDatabases.get(name);
    }

    Map<String, DatabaseReaderLazyLoader> getConfigDatabases() {
        return configDatabases;
    }

    void updateDatabase(Path file, boolean update) {
        String databaseFileName = file.getFileName().toString();
        try {
            if (update) {
                logger.info("database file changed [{}], reloading database...", file);
                DatabaseReaderLazyLoader loader = new DatabaseReaderLazyLoader(cache, file, null);
                DatabaseReaderLazyLoader existing = configDatabases.put(databaseFileName, loader);
                if (existing != null) {
                    existing.shutdown();
                }
            } else {
                logger.info("database file removed [{}], closing database...", file);
                DatabaseReaderLazyLoader existing = configDatabases.remove(databaseFileName);
                assert existing != null;
                existing.shutdown();
            }
        } catch (Exception e) {
            logger.error(() -> "failed to update database [" + databaseFileName + "]", e);
        }
    }

    Map<String, DatabaseReaderLazyLoader> initConfigDatabases() throws IOException {
        Map<String, DatabaseReaderLazyLoader> databases = new HashMap<>();

        if (geoipConfigDir != null && Files.exists(geoipConfigDir)) {
            try (Stream<Path> databaseFiles = Files.list(geoipConfigDir)) {
                PathMatcher pathMatcher = geoipConfigDir.getFileSystem().getPathMatcher("glob:**.mmdb");
                // Use iterator instead of forEach otherwise IOException needs to be caught twice...
                Iterator<Path> iterator = databaseFiles.iterator();
                while (iterator.hasNext()) {
                    Path databasePath = iterator.next();
                    if (Files.isRegularFile(databasePath) && pathMatcher.matches(databasePath)) {
                        assert Files.exists(databasePath);
                        String databaseFileName = databasePath.getFileName().toString();
                        DatabaseReaderLazyLoader loader = new DatabaseReaderLazyLoader(cache, databasePath, null);
                        databases.put(databaseFileName, loader);
                    }
                }
            }
        }

        return Collections.unmodifiableMap(databases);
    }

    @Override
    public void close() throws IOException {
        for (DatabaseReaderLazyLoader lazyLoader : configDatabases.values()) {
            lazyLoader.shutdown();
        }
    }

    private class GeoipDirectoryListener implements FileChangesListener {

        @Override
        public void onFileCreated(Path file) {
            onFileChanged(file);
        }

        @Override
        public void onFileDeleted(Path file) {
            PathMatcher pathMatcher = file.getFileSystem().getPathMatcher("glob:**.mmdb");
            if (pathMatcher.matches(file)) {
                updateDatabase(file, false);
            }
        }

        @Override
        public void onFileChanged(Path file) {
            PathMatcher pathMatcher = file.getFileSystem().getPathMatcher("glob:**.mmdb");
            if (pathMatcher.matches(file)) {
                updateDatabase(file, true);
            }
        }
    }
}
