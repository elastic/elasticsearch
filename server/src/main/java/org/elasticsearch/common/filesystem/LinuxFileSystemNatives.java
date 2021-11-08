/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.filesystem;

import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Structure;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;

/**
 * {@link FileSystemNatives.Provider} implementation for Linux x86-64bits
 */
final class LinuxFileSystemNatives implements FileSystemNatives.Provider {

    private static final Logger logger = LogManager.getLogger(LinuxFileSystemNatives.class);

    private static final LinuxFileSystemNatives INSTANCE = new LinuxFileSystemNatives();

    /** st_blocks field indicates the number of blocks allocated to the file, 512-byte units **/
    private static final long ST_BLOCKS_UNIT = 512L;

    private final Class<? extends BaseMethod> method;

    private LinuxFileSystemNatives() {
        assert Constants.LINUX : Constants.OS_NAME;
        assert Constants.JRE_IS_64BIT : Constants.OS_ARCH;

        Class<? extends BaseMethod> method = null;
        try {
            logger.debug("trying to register Stat64Method...");
            Native.register(Stat64Method.class, Platform.C_LIBRARY_NAME);
            method = Stat64Method.class;
        } catch (UnsatisfiedLinkError ue0) {
            try {
                logger.debug("trying to register XStat64Method...");
                Native.register(XStat64Method.class, Platform.C_LIBRARY_NAME);
                method = XStat64Method.class;
            } catch (UnsatisfiedLinkError ue1) {
                ue1.addSuppressed(ue0);
                try {
                    logger.debug("trying to register StatMethod...");
                    Native.register(StatMethod.class, Platform.C_LIBRARY_NAME);
                    method = StatMethod.class;
                } catch (UnsatisfiedLinkError ue2) {
                    ue2.addSuppressed(ue1);
                    logger.warn("unable to load JNA native support library for stat() on Linux", ue2);
                    throw ue2;
                }
            }
        }
        logger.debug("library {} is registered", method.getSimpleName());
        this.method = method;
    }

    static LinuxFileSystemNatives getInstance() {
        return INSTANCE;
    }

    private abstract static class BaseMethod {

        /**
         * char *strerror(int errnum)
         *
         * https://linux.die.net/man/3/strerror
         */
        private static native String strerror(int errno);
    }

    private static final class Stat64Method extends BaseMethod {

        private Stat64Method() {}

        /**
         * stat(const char *path, struct stat *buf)
         *
         * https://linux.die.net/man/2/stat64
         */
        private static native int stat64(String path, Stat64 stats);
    }

    private static final class XStat64Method extends BaseMethod {

        private XStat64Method() {}

        private static native int __xstat64(int version, String path, Stat64 stats);
    }

    private static final class StatMethod extends BaseMethod {

        private StatMethod() {}

        private static native int stat(String path, Stat64 stats);
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
        final Stat64 stats = new Stat64();

        final int rc;
        if (method == Stat64Method.class) {
            rc = Stat64Method.stat64(path.toString(), stats);
        } else if (method == XStat64Method.class) {
            rc = XStat64Method.__xstat64(1, path.toString(), stats);
        } else {
            assert method == StatMethod.class;
            rc = StatMethod.stat(path.toString(), stats);
        }
        if (rc != 0) {
            final int err = Native.getLastError();
            logger.warn("error [{}] when executing native method stat(const char *path, struct stat *buf) for file [{}]", err, path);
            return OptionalLong.empty();
        }
        if (logger.isTraceEnabled()) {
            logger.trace("executing native method stat() returned {} for file [{}]", stats, path);
        }
        return OptionalLong.of(stats.st_blocks * ST_BLOCKS_UNIT);
    }

    /**
     * Linux X86-64 for stat()
     */
    public static class Stat64 extends Structure {

        public long st_dev;         // dev_t
        public long st_ino;         // ino_t
        public long st_nlink;       // nlink_t
        public int st_mode;         // mode_t
        public int st_uid;          // uid_t
        public int st_gid;          // gid_t
        public int pad0;            // int
        public long st_rdev;        // dev_t
        public long st_size;        // off_t
        public long st_blksize;     // blksize_t
        public long st_blocks;      // blkcnt_t
        public Time st_atim;    // struct timespec
        public Time st_mtim;    // struct timespec
        public Time st_ctim;    // struct timespec
        public long reserved1;      // __syscall_slong_t
        public long reserved2;      // __syscall_slong_t
        public long reserved3;      // __syscall_slong_t

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList(
                "st_dev",
                "st_ino",
                "st_nlink",
                "st_mode",
                "st_uid",
                "st_gid",
                "pad0",
                "st_rdev",
                "st_size",
                "st_blksize",
                "st_blocks",
                "st_atim",
                "st_mtim",
                "st_ctim",
                "reserved1",
                "reserved2",
                "reserved3"
            );
        }

        @Override
        public String toString() {
            return "[st_dev="
                + st_dev
                + ", st_ino="
                + st_ino
                + ", st_nlink="
                + st_nlink
                + ", st_mode="
                + st_mode
                + ", st_uid="
                + st_uid
                + ", st_gid="
                + st_gid
                + ", st_rdev="
                + st_rdev
                + ", st_size="
                + st_size
                + ", st_blksize="
                + st_blksize
                + ", st_blocks="
                + st_blocks
                + ", st_atim="
                + Instant.ofEpochSecond(st_atim.tv_sec, st_atim.tv_nsec)
                + ", st_mtim="
                + Instant.ofEpochSecond(st_mtim.tv_sec, st_mtim.tv_nsec)
                + ", st_ctim="
                + Instant.ofEpochSecond(st_ctim.tv_sec, st_ctim.tv_nsec)
                + ']';
        }
    }

    public static class Time extends Structure {

        public long tv_sec;
        public long tv_nsec;

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList("tv_sec", "tv_nsec");
        }
    }
}
