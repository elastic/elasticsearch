/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.stateless.allocation.CacheRestoredAllocationDecider;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/**
 * Prepares a replacement node that boots from a cache-clone volume. Invoked from
 * {@link org.elasticsearch.xpack.stateless.StatelessPlugin#additionalSettings()} before
 * {@link org.elasticsearch.env.NodeEnvironment} reads persisted node identity from disk.
 */
public final class CacheSnapshotBootstrap {

    private static final Logger logger = LogManager.getLogger(CacheSnapshotBootstrap.class);

    static final String SHARED_CACHE_FILE_NAME = "shared_snapshot_cache";

    private CacheSnapshotBootstrap() {}

    /**
     * When {@link StatelessSharedBlobCacheService#STATELESS_CACHE_SNAPSHOT_ENABLED_SETTING} is
     * enabled and a snapshot metadata file is present, resets persisted node identity on the
     * data path and returns settings that advertise the source node for allocation preference.
     * Explicit operator settings for the allocation attribute are not overridden.
     */
    public static Settings applyCloneBootSettings(Settings settings) {
        if (StatelessSharedBlobCacheService.STATELESS_CACHE_SNAPSHOT_ENABLED_SETTING.get(settings) == false) {
            return Settings.EMPTY;
        }
        Path dataPath = resolveSingleDataPath(settings);
        if (dataPath == null) {
            return Settings.EMPTY;
        }
        Path snapshotFile = CacheSnapshotService.snapshotFilePath(dataPath.resolve(SHARED_CACHE_FILE_NAME));
        Optional<String> sourceNodeId = CacheSnapshotService.readSourceNodeId(snapshotFile);
        if (sourceNodeId.isEmpty()) {
            return Settings.EMPTY;
        }
        try {
            resetPersistedNodeIdentity(dataPath);
        } catch (IOException e) {
            throw new IllegalStateException("failed to reset node identity for cache clone boot from [" + snapshotFile + "]", e);
        }
        String attrKey = "node.attr." + CacheRestoredAllocationDecider.CACHE_RESTORED_FROM_ATTR;
        if (settings.hasValue(attrKey)) {
            logger.info("cache clone boot: reset persisted node identity; allocation attribute already set via [{}]", attrKey);
            return Settings.EMPTY;
        }
        logger.info("cache clone boot: reset persisted node identity; source node [{}] from [{}]", sourceNodeId.get(), snapshotFile);
        return Settings.builder().put(attrKey, sourceNodeId.get()).build();
    }

    static Path resolveSingleDataPath(Settings settings) {
        List<String> dataPaths = Environment.PATH_DATA_SETTING.get(settings);
        if (dataPaths.size() != 1) {
            if (dataPaths.size() > 1) {
                logger.warn("cache clone boot skipped: path.data must be a single directory, got [{}]", dataPaths.size());
            }
            return null;
        }
        return PathUtils.get(dataPaths.get(0));
    }

    static void resetPersistedNodeIdentity(Path dataPath) throws IOException {
        Path stateDir = dataPath.resolve(MetadataStateFormat.STATE_DIR_NAME);
        if (Files.exists(stateDir)) {
            IOUtils.rm(stateDir);
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataPath, "node-*.json")) {
            for (Path legacyMetadata : stream) {
                Files.deleteIfExists(legacyMetadata);
            }
        }
    }
}
