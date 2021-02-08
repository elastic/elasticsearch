/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import com.sun.jna.Native;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.snapshots.SnapshotUtils;
import org.elasticsearch.snapshots.SnapshotsService;

import java.io.FileOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;

/**
 * The Natives class is a wrapper class that checks if the classes necessary for calling native methods are available on
 * startup. If they are not available, this class will avoid calling code that loads these classes.
 */
final class Natives {
    /** no instantiation */
    private Natives() {}

    private static final Logger logger = LogManager.getLogger(Natives.class);

    // marker to determine if the JNA class files are available to the JVM
    static final boolean JNA_AVAILABLE;

    static {
        boolean v = false;
        try {
            // load one of the main JNA classes to see if the classes are available. this does not ensure that all native
            // libraries are available, only the ones necessary by JNA to function
            Class.forName("com.sun.jna.Native");
            v = true;
        } catch (ClassNotFoundException e) {
            logger.warn("JNA not found. native methods will be disabled.", e);
        } catch (UnsatisfiedLinkError e) {
            logger.warn("unable to load JNA native support library, native methods will be disabled.", e);
        }
        JNA_AVAILABLE = v;
    }

    static void tryMlockall() {
        if (JNA_AVAILABLE == false) {
            logger.warn("cannot mlockall because JNA is not available");
            return;
        }
        JNANatives.tryMlockall();
    }

    static boolean definitelyRunningAsRoot() {
        if (JNA_AVAILABLE == false) {
            logger.warn("cannot check if running as root because JNA is not available");
            return false;
        }
        return JNANatives.definitelyRunningAsRoot();
    }

    static void tryVirtualLock() {
        if (JNA_AVAILABLE == false) {
            logger.warn("cannot virtual lock because JNA is not available");
            return;
        }
        JNANatives.tryVirtualLock();
    }

    /**
     * Retrieves the short path form of the specified path.
     *
     * @param path the path
     * @return the short path name (or the original path if getting the short path name fails for any reason)
     */
    static String getShortPathName(final String path) {
        if (JNA_AVAILABLE == false) {
            logger.warn("cannot obtain short path for [{}] because JNA is not available", path);
            return path;
        }
        return JNANatives.getShortPathName(path);
    }

    static void addConsoleCtrlHandler(ConsoleCtrlHandler handler) {
        if (JNA_AVAILABLE == false) {
            logger.warn("cannot register console handler because JNA is not available");
            return;
        }
        JNANatives.addConsoleCtrlHandler(handler);
    }

    static boolean isMemoryLocked() {
        if (JNA_AVAILABLE == false) {
            return false;
        }
        return JNANatives.LOCAL_MLOCKALL;
    }

    static void tryInstallSystemCallFilter(Path tmpFile) {
        if (JNA_AVAILABLE == false) {
            logger.warn("cannot install system call filter because JNA is not available");
            return;
        }
        JNANatives.tryInstallSystemCallFilter(tmpFile);
    }

    static void trySetMaxNumberOfThreads() {
        if (JNA_AVAILABLE == false) {
            logger.warn("cannot getrlimit RLIMIT_NPROC because JNA is not available");
            return;
        }
        JNANatives.trySetMaxNumberOfThreads();
    }

    static void trySetMaxSizeVirtualMemory() {
        if (JNA_AVAILABLE == false) {
            logger.warn("cannot getrlimit RLIMIT_AS because JNA is not available");
            return;
        }
        JNANatives.trySetMaxSizeVirtualMemory();
    }

    static void trySetMaxFileSize() {
        if (JNA_AVAILABLE == false) {
            logger.warn("cannot getrlimit RLIMIT_FSIZE because JNA is not available");
            return;
        }
        JNANatives.trySetMaxFileSize();
    }

    static boolean isSystemCallFilterInstalled() {
        if (JNA_AVAILABLE == false) {
            return false;
        }
        return JNANatives.LOCAL_SYSTEM_CALL_FILTER;
    }

    /**
     * On Linux, this method tries to create the searchable snapshot frozen cache file using fallocate if JNA is available. This enables
     * a much faster creation of the file than the fallback mechanism in the searchable snapshots plugin that will pre-allocate the cache
     * file by writing zeros to the file.
     *
     * @throws IOException on failure to determine free disk space for a data path
     */
    @SuppressForbidden(reason = "need access fd on FileOutputStream")
    public static void tryCreateCacheFile(Environment environment) throws IOException {
        Settings settings = environment.settings();
        final long cacheSize = SnapshotsService.SNAPSHOT_CACHE_SIZE_SETTING.get(settings).getBytes();
        final long regionSize = SnapshotsService.SNAPSHOT_CACHE_REGION_SIZE_SETTING.get(settings).getBytes();
        final int numRegions = Math.toIntExact(cacheSize / regionSize);
        if (numRegions == 0) {
            return;
        }
        if (Constants.LINUX == false) {
            logger.debug("not trying to create a shared cache file using fallocate on non-Linux platform");
            return;
        }
        if (JNA_AVAILABLE == false) {
            logger.warn("cannot use fallocate to create cache file because JNA is not available");
            return;
        }

        final long fileSize = numRegions * regionSize;
        Path cacheFile = SnapshotUtils.findCacheSnapshotCacheFilePath(environment, fileSize);
        if (cacheFile == null) {
            throw new IOError(new IOException("Could not find a directory with adequate free space for cache file"));
        }
        try (FileOutputStream fileChannel = new FileOutputStream(cacheFile.toFile())) {
            long currentSize = fileChannel.getChannel().size();
            if (currentSize < fileSize) {
                final Field field = fileChannel.getFD().getClass().getDeclaredField("fd");
                field.setAccessible(true);
                final int result = JNACLibrary.fallocate((int) field.get(fileChannel.getFD()), 0, currentSize, fileSize - currentSize);
                final int errno = result == 0 ? 0 : Native.getLastError();
                if (errno == 0) {
                    logger.info("Allocated cache file [{}] using fallocate", cacheFile);
                } else {
                    logger.warn("Failed to initialize cache file [{}] using fallocate errno [{}]", cacheFile, errno);
                }
            }
        } catch (Exception e) {
            logger.warn(new ParameterizedMessage("Failed to initialize cache file [{}] using fallocate", cacheFile), e);
        }
    }

}
