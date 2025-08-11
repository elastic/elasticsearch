/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.preallocate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.SuppressForbidden;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

public class Preallocate {

    private static final Logger logger = LogManager.getLogger(Preallocate.class);

    public static void preallocate(final Path cacheFile, final long fileSize) throws IOException {
        if (Constants.LINUX) {
            preallocate(cacheFile, fileSize, new LinuxPreallocator());
        } else if (Constants.MAC_OS_X) {
            preallocate(cacheFile, fileSize, new MacOsPreallocator());
        } else {
            preallocate(cacheFile, fileSize, new NoNativePreallocator());
        }
    }

    @SuppressForbidden(reason = "need access to fd on FileOutputStream")
    private static void preallocate(final Path cacheFile, final long fileSize, final Preallocator prealloactor) throws IOException {
        boolean success = false;
        try {
            if (prealloactor.useNative()) {
                try (FileOutputStream fileChannel = new FileOutputStream(cacheFile.toFile())) {
                    long currentSize = fileChannel.getChannel().size();
                    if (currentSize < fileSize) {
                        logger.info("pre-allocating cache file [{}] ({}) using native methods", cacheFile, new ByteSizeValue(fileSize));
                        final Field field = AccessController.doPrivileged(new FileDescriptorFieldAction(fileChannel));
                        final int errno = prealloactor.preallocate(
                            (int) field.get(fileChannel.getFD()),
                            currentSize,
                            fileSize - currentSize
                        );
                        if (errno == 0) {
                            success = true;
                            logger.debug("pre-allocated cache file [{}] using native methods", cacheFile);
                        } else {
                            logger.warn(
                                "failed to pre-allocate cache file [{}] using native methods, errno: [{}], error: [{}]",
                                cacheFile,
                                errno,
                                prealloactor.error(errno)
                            );
                        }
                    }
                } catch (final Exception e) {
                    logger.warn(new ParameterizedMessage("failed to pre-allocate cache file [{}] using native methods", cacheFile), e);
                }
            }
            // even if allocation was successful above, verify again here
            try (RandomAccessFile raf = new RandomAccessFile(cacheFile.toFile(), "rw")) {
                if (raf.length() != fileSize) {
                    logger.info("pre-allocating cache file [{}] ({}) using setLength method", cacheFile, new ByteSizeValue(fileSize));
                    raf.setLength(fileSize);
                    logger.debug("pre-allocated cache file [{}] using setLength method", cacheFile);
                }
                success = raf.length() == fileSize;
            } catch (final Exception e) {
                logger.warn(new ParameterizedMessage("failed to pre-allocate cache file [{}] using setLength method", cacheFile), e);
                throw e;
            }
        } finally {
            if (success == false) {
                // if anything goes wrong, delete the potentially created file to not waste disk space
                Files.deleteIfExists(cacheFile);
            }
        }
    }

    @SuppressForbidden(reason = "need access to fd on FileOutputStream")
    private static class FileDescriptorFieldAction implements PrivilegedExceptionAction<Field> {

        private final FileOutputStream fileOutputStream;

        private FileDescriptorFieldAction(FileOutputStream fileOutputStream) {
            this.fileOutputStream = fileOutputStream;
        }

        @Override
        public Field run() throws IOException, NoSuchFieldException {
            // accessDeclaredMembers
            final Field f = fileOutputStream.getFD().getClass().getDeclaredField("fd");
            // suppressAccessChecks
            f.setAccessible(true);
            return f;
        }
    }

}
