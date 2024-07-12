/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;
import org.elasticsearch.nativeaccess.lib.PosixCLibrary;
import org.elasticsearch.nativeaccess.lib.VectorLibrary;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.OptionalLong;

abstract class PosixNativeAccess extends AbstractNativeAccess {

    public static final int MCL_CURRENT = 1;
    public static final int ENOMEM = 12;
    public static final int O_RDONLY = 0;
    public static final int O_WRONLY = 1;

    protected final PosixCLibrary libc;
    protected final VectorSimilarityFunctions vectorDistance;
    protected final PosixConstants constants;
    protected final ProcessLimits processLimits;

    PosixNativeAccess(String name, NativeLibraryProvider libraryProvider, PosixConstants constants) {
        super(name, libraryProvider);
        this.libc = libraryProvider.getLibrary(PosixCLibrary.class);
        this.vectorDistance = vectorSimilarityFunctionsOrNull(libraryProvider);
        this.constants = constants;
        this.processLimits = new ProcessLimits(
            getMaxThreads(),
            getRLimit(constants.RLIMIT_AS(), "max size virtual memory"),
            getRLimit(constants.RLIMIT_FSIZE(), "max file size")
        );
    }

    /**
     * Return the maximum number of threads this process may start, or {@link ProcessLimits#UNKNOWN}.
     */
    protected abstract long getMaxThreads();

    /**
     * Return the current rlimit for the given resource.
     * If getrlimit fails, returns {@link ProcessLimits#UNKNOWN}.
     * If the rlimit is unlimited, returns {@link ProcessLimits#UNLIMITED}.
     * */
    protected long getRLimit(int resource, String description) {
        var rlimit = libc.newRLimit();
        if (libc.getrlimit(resource, rlimit) == 0) {
            long value = rlimit.rlim_cur();
            return value == constants.RLIMIT_INFINITY() ? ProcessLimits.UNLIMITED : value;
        } else {
            logger.warn("unable to retrieve " + description + " [" + libc.strerror(libc.errno()) + "]");
            return ProcessLimits.UNKNOWN;
        }
    }

    static VectorSimilarityFunctions vectorSimilarityFunctionsOrNull(NativeLibraryProvider libraryProvider) {
        if (isNativeVectorLibSupported()) {
            var lib = libraryProvider.getLibrary(VectorLibrary.class).getVectorSimilarityFunctions();
            logger.info("Using native vector library; to disable start with -D" + ENABLE_JDK_VECTOR_LIBRARY + "=false");
            return lib;
        }
        return null;
    }

    @Override
    public boolean definitelyRunningAsRoot() {
        return libc.geteuid() == 0;
    }

    @Override
    public ProcessLimits getProcessLimits() {
        return processLimits;
    }

    @Override
    public void tryLockMemory() {
        int result = libc.mlockall(MCL_CURRENT);
        if (result == 0) {
            isMemoryLocked = true;
            return;
        }

        // mlockall failed for some reason
        int errno = libc.errno();
        String errMsg = libc.strerror(errno);
        logger.warn("Unable to lock JVM Memory: error={}, reason={}", errno, errMsg);
        logger.warn("This can result in part of the JVM being swapped out.");

        if (errno == ENOMEM) {

            boolean rlimitSuccess = false;
            long softLimit = 0;
            long hardLimit = 0;

            // we only know RLIMIT_MEMLOCK for these two at the moment.
            var rlimit = libc.newRLimit();
            if (libc.getrlimit(constants.RLIMIT_MEMLOCK(), rlimit) == 0) {
                rlimitSuccess = true;
                softLimit = rlimit.rlim_cur();
                hardLimit = rlimit.rlim_max();
            } else {
                logger.warn("Unable to retrieve resource limits: {}", libc.strerror(libc.errno()));
            }

            if (rlimitSuccess) {
                logger.warn(
                    "Increase RLIMIT_MEMLOCK, soft limit: {}, hard limit: {}",
                    rlimitToString(softLimit),
                    rlimitToString(hardLimit)
                );
                logMemoryLimitInstructions();
            } else {
                logger.warn("Increase RLIMIT_MEMLOCK (ulimit).");
            }
        }
    }

    protected abstract void logMemoryLimitInstructions();

    @Override
    public OptionalLong allocatedSizeInBytes(Path path) {
        assert Files.isRegularFile(path) : path;
        var stats = libc.newStat64(constants.statStructSize(), constants.statStructSizeOffset(), constants.statStructBlocksOffset());

        int fd = libc.open(path.toAbsolutePath().toString(), O_RDONLY);
        if (fd == -1) {
            logger.warn("Could not open file [" + path + "] to get allocated size: " + libc.strerror(libc.errno()));
            return OptionalLong.empty();
        }

        if (libc.fstat64(fd, stats) != 0) {
            logger.warn("Could not get stats for file [" + path + "] to get allocated size: " + libc.strerror(libc.errno()));
            return OptionalLong.empty();
        }
        if (libc.close(fd) != 0) {
            logger.warn("Failed to close file [" + path + "] after getting stats: " + libc.strerror(libc.errno()));
        }
        return OptionalLong.of(stats.st_blocks() * 512);
    }

    @Override
    public void tryPreallocate(Path file, long newSize) {
        // get fd and current size, then pass to OS variant
        int fd = libc.open(file.toAbsolutePath().toString(), O_WRONLY, constants.O_CREAT());
        if (fd == -1) {
            logger.warn("Could not open file [" + file + "] to preallocate size: " + libc.strerror(libc.errno()));
            return;
        }

        var stats = libc.newStat64(constants.statStructSize(), constants.statStructSizeOffset(), constants.statStructBlocksOffset());
        if (libc.fstat64(fd, stats) != 0) {
            logger.warn("Could not get stats for file [" + file + "] to preallocate size: " + libc.strerror(libc.errno()));
        } else {
            if (nativePreallocate(fd, stats.st_size(), newSize)) {
                logger.debug("pre-allocated file [{}] to {} bytes", file, newSize);
            } // OS specific preallocate logs its own errors
        }

        if (libc.close(fd) != 0) {
            logger.warn("Could not close file [" + file + "] after trying to preallocate size: " + libc.strerror(libc.errno()));
        }
    }

    protected abstract boolean nativePreallocate(int fd, long currentSize, long newSize);

    @Override
    public Optional<VectorSimilarityFunctions> getVectorSimilarityFunctions() {
        return Optional.ofNullable(vectorDistance);
    }

    String rlimitToString(long value) {
        if (value == constants.RLIMIT_INFINITY()) {
            return "unlimited";
        } else {
            return Long.toUnsignedString(value);
        }
    }

    static boolean isNativeVectorLibSupported() {
        return Runtime.version().feature() >= 21 && (isMacOrLinuxAarch64() || isLinuxAmd64()) && checkEnableSystemProperty();
    }

    /**
     * Returns true iff the architecture is x64 (amd64) and the OS Linux (the OS we currently support for the native lib).
     */
    static boolean isLinuxAmd64() {
        String name = System.getProperty("os.name");
        return (name.startsWith("Linux")) && System.getProperty("os.arch").equals("amd64");
    }

    /** Returns true iff the OS is Mac or Linux, and the architecture is aarch64. */
    static boolean isMacOrLinuxAarch64() {
        String name = System.getProperty("os.name");
        return (name.startsWith("Mac") || name.startsWith("Linux")) && System.getProperty("os.arch").equals("aarch64");
    }

    /** -Dorg.elasticsearch.nativeaccess.enableVectorLibrary=false to disable.*/
    static final String ENABLE_JDK_VECTOR_LIBRARY = "org.elasticsearch.nativeaccess.enableVectorLibrary";

    static boolean checkEnableSystemProperty() {
        return Optional.ofNullable(System.getProperty(ENABLE_JDK_VECTOR_LIBRARY)).map(Boolean::valueOf).orElse(Boolean.TRUE);
    }
}
