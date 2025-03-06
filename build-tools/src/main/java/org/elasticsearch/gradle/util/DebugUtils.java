/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.util;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class DebugUtils {

    private static final Logger LOGGER = Logging.getLogger(DebugUtils.class);

    public static void logDiskSpaceAndPrivileges(Path directoryPath) {
        try {
            File file = directoryPath.toFile();

            LOGGER.warn("Checking disk space and file permissions for directory: {}", directoryPath);

            LOGGER.warn("=================");
            LOGGER.warn("Listing all files");
            listFilesRecursively(directoryPath);
            LOGGER.warn("=================");

            // Check if the directory exists
            if (Files.exists(directoryPath) == false) {
                LOGGER.error("Directory does not exist: {}", directoryPath);
                return;
            }

            // Log disk space information
            long freeSpace = file.getFreeSpace();
            long totalSpace = file.getTotalSpace();
            long usableSpace = file.getUsableSpace();
            LOGGER.warn("Disk space information for directory: {}", directoryPath);
            LOGGER.warn("Free space: {} bytes", freeSpace);
            LOGGER.warn("Total space: {} bytes", totalSpace);
            LOGGER.warn("Usable space: {} bytes", usableSpace);

            // Log file permissions
            boolean canRead = file.canRead();
            boolean canWrite = file.canWrite();
            boolean canExecute = file.canExecute();
            LOGGER.warn("File permissions for directory: {}", directoryPath);
            LOGGER.warn("Can read: {}", canRead);
            LOGGER.warn("Can write: {}", canWrite);
            LOGGER.warn("Can execute: {}", canExecute);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void listFilesRecursively(Path directoryPath) {
        try (Stream<Path> paths = Files.walk(directoryPath)) {
            paths.forEach(it -> LOGGER.warn("{}", it));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
