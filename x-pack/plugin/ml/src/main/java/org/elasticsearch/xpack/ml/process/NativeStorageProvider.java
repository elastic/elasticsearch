/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Provide storage for native components.
 */
public class NativeStorageProvider {

    private static final Logger LOGGER = LogManager.getLogger(NativeStorageProvider.class);

    private static final String LOCAL_STORAGE_SUBFOLDER = "ml-local-data";
    private static final String LOCAL_STORAGE_TMP_FOLDER = "tmp";

    private final Environment environment;

    // do not allow any usage below this threshold
    private final ByteSizeValue minLocalStorageAvailable;

    // A map to keep track of allocated native storage by resource id
    private final ConcurrentMap<String, Path> allocatedStorage = new ConcurrentHashMap<>();

    public NativeStorageProvider(Environment environment, ByteSizeValue minDiskSpaceOffHeap) {
        this.environment = environment;
        this.minLocalStorageAvailable = minDiskSpaceOffHeap;
    }

    /**
     * Removes any temporary storage leftovers.
     *
     * Removes all temp files and folder which might be there as a result of an
     * unclean node shutdown or broken clients.
     *
     * Do not call while there are running jobs.
     */
    public void cleanupLocalTmpStorageInCaseOfUncleanShutdown() {
        try {
            IOUtils.rm(environment.dataFile().resolve(LOCAL_STORAGE_SUBFOLDER).resolve(LOCAL_STORAGE_TMP_FOLDER));
        } catch (Exception e) {
            LOGGER.warn("Failed to cleanup native storage from previous invocation", e);
        }
    }

    /**
     * Tries to find local storage for storing temporary data.
     *
     * @param uniqueIdentifier An identifier to be used as sub folder
     * @param requestedSize The maximum size required
     * @return Path for temporary storage if available, null otherwise
     */
    public Path tryGetLocalTmpStorage(String uniqueIdentifier, ByteSizeValue requestedSize) {
        Path path = allocatedStorage.get(uniqueIdentifier);
        if (path != null && localTmpStorageHasEnoughSpace(path, requestedSize) == false) {
            LOGGER.warn("Previous tmp storage for [{}] run out, returning null", uniqueIdentifier);
            return null;
        } else {
            path = tryAllocateStorage(uniqueIdentifier, requestedSize);
        }
        return path;
    }

    private Path tryAllocateStorage(String uniqueIdentifier, ByteSizeValue requestedSize) {
        final Path path = environment.dataFile();
        try {
            if (getUsableSpace(path) >= requestedSize.getBytes() + minLocalStorageAvailable.getBytes()) {
                Path tmpDirectory = path.resolve(LOCAL_STORAGE_SUBFOLDER).resolve(LOCAL_STORAGE_TMP_FOLDER).resolve(uniqueIdentifier);
                Files.createDirectories(tmpDirectory);
                allocatedStorage.put(uniqueIdentifier, tmpDirectory);
                return tmpDirectory;
            }
        } catch (IOException e) {
            LOGGER.warn("Failed to obtain information about path [{}]: {}", path, e);
        }
        LOGGER.warn("Failed to find native storage for [{}], returning null", uniqueIdentifier);
        return null;
    }

    public boolean localTmpStorageHasEnoughSpace(Path path, ByteSizeValue requestedSize) {
        Path realPath = path.toAbsolutePath();
        Path dataPath = environment.dataFile();
        try {
            if (realPath.startsWith(dataPath.resolve(LOCAL_STORAGE_SUBFOLDER).resolve(LOCAL_STORAGE_TMP_FOLDER))) {
                return getUsableSpace(dataPath) >= requestedSize.getBytes() + minLocalStorageAvailable.getBytes();
            }
        } catch (IOException e) {
            LOGGER.warn("Failed to obtain information about path [{}]: {}", path, e);
        }

        LOGGER.debug("Not enough space left for path [{}]", path);
        return false;
    }

    /**
     * Delete temporary storage, previously allocated
     *
     * @param uniqueIdentifier the identifier to which storage was allocated
     * @throws IOException if path can not be cleaned up
     */
    public void cleanupLocalTmpStorage(String uniqueIdentifier) throws IOException {
        Path path = allocatedStorage.remove(uniqueIdentifier);
        if (path != null) {
            // do not allow to breakout from the tmp storage provided
            Path realPath = path.toAbsolutePath();
            Path dataPath = environment.dataFile();
            if (realPath.startsWith(dataPath.resolve(LOCAL_STORAGE_SUBFOLDER).resolve(LOCAL_STORAGE_TMP_FOLDER))) {
                IOUtils.rm(path);
            }
        }
    }

    public ByteSizeValue getMinLocalStorageAvailable() {
        return minLocalStorageAvailable;
    }

    // non-static indirection to enable mocking in tests
    long getUsableSpace(Path path) throws IOException {
        return Environment.getUsableSpace(path);
    }
}
