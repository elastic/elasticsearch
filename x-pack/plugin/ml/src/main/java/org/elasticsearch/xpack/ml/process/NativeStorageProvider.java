/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
     *
     * @throws IOException if cleanup fails
     */
    public void cleanupLocalTmpStorageInCaseOfUncleanShutdown() throws IOException {
        for (Path p : environment.dataFiles()) {
            IOUtils.rm(p.resolve(LOCAL_STORAGE_SUBFOLDER).resolve(LOCAL_STORAGE_TMP_FOLDER));
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
        for (Path path : environment.dataFiles()) {
            try {
                if (getUsableSpace(path) >= requestedSize.getBytes() + minLocalStorageAvailable.getBytes()) {
                    Path tmpDirectory = path.resolve(LOCAL_STORAGE_SUBFOLDER).resolve(LOCAL_STORAGE_TMP_FOLDER).resolve(uniqueIdentifier);
                    Files.createDirectories(tmpDirectory);
                    return tmpDirectory;
                }
            } catch (IOException e) {
                LOGGER.debug("Failed to obtain information about path [{}]: {}", path, e);
            }

        }
        LOGGER.debug("Failed to find native storage for [{}], returning null", uniqueIdentifier);
        return null;
    }

    public boolean localTmpStorageHasEnoughSpace(Path path, ByteSizeValue requestedSize) {
        Path realPath = path.toAbsolutePath();
        for (Path p : environment.dataFiles()) {
            try {
                if (realPath.startsWith(p.resolve(LOCAL_STORAGE_SUBFOLDER).resolve(LOCAL_STORAGE_TMP_FOLDER))) {
                    return getUsableSpace(p) >= requestedSize.getBytes() + minLocalStorageAvailable.getBytes();
                }
            } catch (IOException e) {
                LOGGER.debug("Failed to optain information about path [{}]: {}", path, e);
            }
        }

        LOGGER.debug("Not enough space left for path [{}]", path);
        return false;
    }

    /**
     * Delete temporary storage, previously allocated
     *
     * @param path
     *            Path to temporary storage
     * @throws IOException
     *             if path can not be cleaned up
     */
    public void cleanupLocalTmpStorage(Path path) throws IOException {
        // do not allow to breakout from the tmp storage provided
        Path realPath = path.toAbsolutePath();
        for (Path p : environment.dataFiles()) {
            if (realPath.startsWith(p.resolve(LOCAL_STORAGE_SUBFOLDER).resolve(LOCAL_STORAGE_TMP_FOLDER))) {
                IOUtils.rm(path);
            }
        }
    }

    long getUsableSpace(Path path) throws IOException {
        long freeSpaceInBytes = Environment.getFileStore(path).getUsableSpace();

        /* See: https://bugs.openjdk.java.net/browse/JDK-8162520 */
        if (freeSpaceInBytes < 0) {
            freeSpaceInBytes = Long.MAX_VALUE;
        }
        return freeSpaceInBytes;
    }
}
