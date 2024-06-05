/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;

/**
 * Some utility functions for managing files.
 */
public final class FileUtils {

    private FileUtils() {}

    private static final FileAttribute<?>[] POSIX_TMP_DIR_PERMISSIONS = new FileAttribute<?>[] {
        PosixFilePermissions.asFileAttribute(
            EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE)
        ) };

    /**
     * Recreates the Elasticsearch temporary directory if it doesn't exist.
     * The operating system may have cleaned it up due to inactivity, which
     * causes some (machine learning) processes to fail.
     * @param tmpDir the path to the temporary directory
     */
    public static void recreateTempDirectoryIfNeeded(Path tmpDir) throws IOException {
        if (tmpDir.getFileSystem().supportedFileAttributeViews().contains("posix")) {
            Files.createDirectories(tmpDir, POSIX_TMP_DIR_PERMISSIONS);
        } else {
            Files.createDirectories(tmpDir);
        }
    }
}
