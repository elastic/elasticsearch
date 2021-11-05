/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.filesystem;

import com.sun.jna.Native;
import com.sun.jna.WString;
import com.sun.jna.ptr.IntByReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalLong;

/**
 * {@link FileSystemNatives.Provider} implementation for Windows/Kernel32
 */
final class WindowsFileSystemNatives implements FileSystemNatives.Provider {

    private static final Logger logger = LogManager.getLogger(WindowsFileSystemNatives.class);

    private static final WindowsFileSystemNatives INSTANCE = new WindowsFileSystemNatives();

    private static final int INVALID_FILE_SIZE = -1;
    private static final int NO_ERROR = 0;

    private WindowsFileSystemNatives() {
        assert Constants.WINDOWS : Constants.OS_NAME;
        try {
            Native.register("kernel32");
            logger.debug("windows/Kernel32 library loaded");
        } catch (LinkageError e) {
            logger.warn("unable to link Windows/Kernel32 library. native methods and handlers will be disabled.", e);
            throw e;
        }
    }

    static WindowsFileSystemNatives getInstance() {
        return INSTANCE;
    }

    /**
     * Retrieves the actual number of bytes of disk storage used to store a specified file.
     *
     * https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getcompressedfilesizew
     *
     * @param lpFileName the path string
     * @param lpFileSizeHigh pointer to high-order DWORD for compressed file size (or null if not needed)
     * @return the low-order DWORD for compressed file siz
     */
    private native int GetCompressedFileSizeW(WString lpFileName, IntByReference lpFileSizeHigh);

    /**
     * Retrieves the actual number of bytes of disk storage used to store a specified file. If the file is located on a volume that supports
     * compression and the file is compressed, the value obtained is the compressed size of the specified file. If the file is located on a
     * volume that supports sparse files and the file is a sparse file, the value obtained is the sparse size of the specified file.
     *
     * This method uses Win32 DLL native method {@link #GetCompressedFileSizeW(WString, IntByReference)}.
     *
     * @param path the path to the file
     * @return an {@link OptionalLong} that contains the number of allocated bytes on disk for the file, or empty if the size is invalid
     */
    public OptionalLong allocatedSizeInBytes(Path path) {
        assert Files.isRegularFile(path) : path;
        final WString fileName = new WString("\\\\?\\" + path);
        final IntByReference lpFileSizeHigh = new IntByReference();

        final int lpFileSizeLow = GetCompressedFileSizeW(fileName, lpFileSizeHigh);
        if (lpFileSizeLow == INVALID_FILE_SIZE) {
            final int err = Native.getLastError();
            if (err != NO_ERROR) {
                logger.warn("error [{}] when executing native method GetCompressedFileSizeW for file [{}]", err, path);
                return OptionalLong.empty();
            }
        }

        // convert lpFileSizeLow to unsigned long and combine with signed/shifted lpFileSizeHigh
        final long allocatedSize = (((long) lpFileSizeHigh.getValue()) << Integer.SIZE) | Integer.toUnsignedLong(lpFileSizeLow);
        if (logger.isTraceEnabled()) {
            logger.trace(
                "executing native method GetCompressedFileSizeW returned [high={}, low={}, allocated={}] for file [{}]",
                lpFileSizeHigh,
                lpFileSizeLow,
                allocatedSize,
                path
            );
        }
        return OptionalLong.of(allocatedSize);
    }
}
