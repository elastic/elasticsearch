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
import org.elasticsearch.core.Nullable;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * {@link FileSystemNatives.Provider} implementation for Windows/Kernel32
 */
final class WindowsFileSystemNatives implements FileSystemNatives.Provider {

    private static final Logger logger = LogManager.getLogger(WindowsFileSystemNatives.class);

    private static final class Holder {
        private static final WindowsFileSystemNatives instance = new WindowsFileSystemNatives();
    }

    private WindowsFileSystemNatives() {
        if (Constants.WINDOWS) {
            try {
                Native.register("kernel32");
                logger.debug("windows/Kernel32 library loaded");
            } catch (NoClassDefFoundError e) {
                logger.warn("JNA not found. native methods and handlers will be disabled.");
            } catch (UnsatisfiedLinkError e) {
                logger.warn("unable to link Windows/Kernel32 library. native methods and handlers will be disabled.");
            }
        }
    }

    static WindowsFileSystemNatives getInstance() {
        return WindowsFileSystemNatives.Holder.instance;
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
    native int GetCompressedFileSizeW(WString lpFileName, IntByReference lpFileSizeHigh);

    /**
     * Retrieves the actual number of bytes of disk storage used to store a specified file. If the file is located on a volume that supports
     * compression and the file is compressed, the value obtained is the compressed size of the specified file. If the file is located on a
     * volume that supports sparse files and the file is a sparse file, the value obtained is the sparse size of the specified file.
     *
     * This method uses Win32 DLL native method {@link #GetCompressedFileSizeW(WString, IntByReference)}.
     *
     * @param path the path to the file
     * @return the number of allocated bytes on disk for the file or {@code null} if the allocated size is invalid
     */
    @Nullable
    public Long allocatedSizeInBytes(Path path) {
        assert Files.isRegularFile(path) : path;
        final WString fileName = new WString("\\\\?\\" + path);
        final IntByReference lpFileSizeHigh = new IntByReference();

        final int lpFileSizeLow = GetCompressedFileSizeW(fileName, lpFileSizeHigh);
        if (lpFileSizeLow == 0xffffffff) {
            final int err = Native.getLastError();
            logger.warn("error [{}] when executing native method GetCompressedFileSizeW for file [{}]", err, path);
            return null;
        }

        final long allocatedSize = (((long) lpFileSizeHigh.getValue()) << 32) | (lpFileSizeLow & 0xffffffffL);
        if (logger.isTraceEnabled()) {
            logger.trace(
                "executing native method GetCompressedFileSizeW returned [high={}, low={}, allocated={}] for file [{}]",
                lpFileSizeHigh,
                lpFileSizeLow,
                allocatedSize,
                path
            );
        }
        return allocatedSize;
    }
}
