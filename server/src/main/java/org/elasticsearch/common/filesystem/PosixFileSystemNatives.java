/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.filesystem;

import com.sun.jna.LastErrorException;
import com.sun.jna.Library;
import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalLong;

import static org.elasticsearch.core.Strings.format;

/**
 * {@link FileSystemNatives.Provider} implementation for Linux x86-64bits
 */
final class PosixFileSystemNatives implements FileSystemNatives.Provider {

    private static final Logger logger = LogManager.getLogger(PosixFileSystemNatives.class);

    private static final PosixFileSystemNatives INSTANCE = new PosixFileSystemNatives();

    /** st_blocks field indicates the number of blocks allocated to the file, 512-byte units **/
    private static final long ST_BLOCKS_UNIT = 512L;

    /**
     * Version of the `struct stat' data structure.
     *
     * To allow the `struct stat' structure bits to vary without changing shared library major version number, the `stat' function is often
     * an inline wrapper around `xstat' which takes a leading version-number argument designating the data structure and bits used.
     *
     * In glibc this version is defined in bits/stat.h (or bits/struct_stat.h in glibc 2.33, or bits/xstatver.h in more recent versions).
     *
     * For x86-64 the _STAT_VER used is:
     *  # define _STAT_VER_LINUX    1
     *  # define _STAT_VER _STAT_VER_LINUX
     *
     * For other architectures the _STAT_VER used is:
     *  # define _STAT_VER_LINUX    0
     *  # define _STAT_VER _STAT_VER_LINUX
     **/
    private static int loadStatVersion() {
        return "aarch64".equalsIgnoreCase(Constants.OS_ARCH) ? 0 : 1;
    }

    private final Stat64Library lib;

    private PosixFileSystemNatives() {
        assert Constants.JRE_IS_64BIT : Constants.OS_ARCH;
        Stat64Library statFunction;
        try {
            var libc = NativeLibrary.getInstance(Platform.C_LIBRARY_NAME);
            libc.getFunction("stat64");
            // getfunction didn't fail, so the symbol is available
            statFunction = Native.load(Platform.C_LIBRARY_NAME, Stat64Library.class);
        } catch (UnsatisfiedLinkError e) {
            var xstat = Native.load(Platform.C_LIBRARY_NAME, XStatLibrary.class);
            var version = loadStatVersion();
            statFunction = (path, stats) -> xstat.__xstat(version, path, stats);
        }
        lib = statFunction;
        logger.debug("C library loaded");
    }

    static PosixFileSystemNatives getInstance() {
        return INSTANCE;
    }

    public interface Stat64Library extends Library {
        int stat64(String path, Pointer stats) throws LastErrorException;
    }

    public interface XStatLibrary extends Library {
        int __xstat(int version, String path, Pointer stats) throws LastErrorException;
    }

    /**
     * Retrieves the actual number of bytes of disk storage used to store a specified file.
     *
     * @param path the path to the file
     * @return an {@link OptionalLong} that contains the number of allocated bytes on disk for the file, or empty if the size is invalid
     */
    @Override
    public OptionalLong allocatedSizeInBytes(Path path) {
        assert Files.isRegularFile(path) : path;
        try {
            final Stat stats = new Stat(Constants.LINUX ? 64 : 104);
            final int rc = lib.stat64(path.toString(), stats.memory);
            if (logger.isTraceEnabled()) {
                logger.trace("executing native method __xstat() returned {} with error code [{}] for file [{}]", stats, rc, path);
            }
            return OptionalLong.of(stats.getBlocks() * ST_BLOCKS_UNIT);
        } catch (LastErrorException e) {
            logger.warn(
                () -> format(
                    "error when executing native method __xstat(int vers, const char *name, struct stat *buf) for file [%s]",
                    path
                ),
                e
            );
        }
        return OptionalLong.empty();
    }

    public static class Stat {
        final Memory memory = new Memory(144);
        final int blocksOffset;

        Stat(int blocksOffset) {
            this.blocksOffset = blocksOffset;
        }

        public long getBlocks() {
            return memory.getLong(blocksOffset);
        }

        @Override
        public String toString() {
            return "Stat [blocks=" + getBlocks() + "]";
        }
    }
}
