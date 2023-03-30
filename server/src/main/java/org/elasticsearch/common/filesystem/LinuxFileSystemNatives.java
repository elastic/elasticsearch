/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.filesystem;

import com.sun.jna.LastErrorException;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Structure;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.OptionalLong;

import static org.elasticsearch.core.Strings.format;

/**
 * {@link FileSystemNatives.Provider} implementation for Linux x86-64bits
 */
final class LinuxFileSystemNatives implements FileSystemNatives.Provider {

    private static final Logger logger = LogManager.getLogger(LinuxFileSystemNatives.class);

    private static final LinuxFileSystemNatives INSTANCE = new LinuxFileSystemNatives();

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

    private static final int STAT_VER = loadStatVersion();

    private LinuxFileSystemNatives() {
        assert Constants.LINUX : Constants.OS_NAME;
        assert Constants.JRE_IS_64BIT : Constants.OS_ARCH;
        try {
            Native.register(XStatLibrary.class, Platform.C_LIBRARY_NAME);
            logger.debug("C library loaded");
        } catch (LinkageError e) {
            logger.warn("unable to link C library. native methods and handlers will be disabled.", e);
            throw e;
        }
    }

    static LinuxFileSystemNatives getInstance() {
        return INSTANCE;
    }

    public static class XStatLibrary {
        public static native int __xstat(int version, String path, Stat stats) throws LastErrorException;
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
            final Stat stats = new Stat();
            final int rc = XStatLibrary.__xstat(STAT_VER, path.toString(), stats);
            if (logger.isTraceEnabled()) {
                logger.trace("executing native method __xstat() returned {} with error code [{}] for file [{}]", stats, rc, path);
            }
            return OptionalLong.of(stats.st_blocks * ST_BLOCKS_UNIT);
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

    @Structure.FieldOrder(
        {
            "st_dev",
            "st_ino",
            "st_nlink",
            "st_mode",
            "st_uid",
            "st_gid",
            "__pad0",
            "st_rdev",
            "st_size",
            "st_blksize",
            "st_blocks",
            "st_atim",
            "st_mtim",
            "st_ctim",
            "__glibc_reserved0",
            "__glibc_reserved1",
            "__glibc_reserved2" }
    )
    public static class Stat extends Structure {

        /**
         * The stat structure varies across architectures in the glibc and kernel source codes. For example some fields might be ordered
         * differently and/or some padding bytes might be present between some fields.
         *
         * The struct implemented here refers to the Linux x86 architecture in the glibc source files:
         * - glibc version 2.23: sysdeps/unix/sysv/linux/x86/bits/stat.h
         * - glibc version 2.33: sysdeps/unix/sysv/linux/x86/bits/struct_stat.h
         *
         * The following command is useful to compile the stat struct on a given system:
         *     echo "#include &lt;sys/stat.h&gt;" | gcc -xc - -E -dD | grep -ve '^$' | grep -A23 '^struct stat'
         */
        public long st_dev;         // __dev_t st_dev; /* Device. */
        public long st_ino;         // __ino_t st_ino; /* File serial number. */
        public long st_nlink;       // __nlink_t st_nlink; /* Link count. */
        public int st_mode;         // __mode_t st_mode; /* File mode. */
        public int st_uid;          // __uid_t st_uid; /* User ID of the file's owner. */
        public int st_gid;          // __gid_t st_gid; /* Group ID of the file's group. */
        public int __pad0;
        public long st_rdev;        // __dev_t st_rdev; /* Device number, if device. */
        public long st_size;        // __off_t st_size; /* Size of file, in bytes. */
        public long st_blksize;     // __blksize_t st_blksize; /* Optimal block size for I/O. */
        public long st_blocks;      // __blkcnt_t st_blocks; /* Number 512-byte blocks allocated. */
        public Time st_atim;        // struct timespec st_atim; /* Time of last access. */
        public Time st_mtim;        // struct timespec st_mtim; /* Time of last modification. */
        public Time st_ctim;        // struct timespec st_ctim; /* Time of last status change. */
        public long __glibc_reserved0;      // __syscall_slong_t
        public long __glibc_reserved1;      // __syscall_slong_t
        public long __glibc_reserved2;      // __syscall_slong_t

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

    @Structure.FieldOrder({ "tv_sec", "tv_nsec" })
    public static class Time extends Structure {
        public long tv_sec;
        public long tv_nsec;
    }
}
