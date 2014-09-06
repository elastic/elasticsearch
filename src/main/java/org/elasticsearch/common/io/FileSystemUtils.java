/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.io;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.logging.ESLogger;

import java.io.File;
import java.io.IOException;

/**
 *
 */
public class FileSystemUtils {

    public static boolean mkdirs(File dir) {
        return dir.mkdirs();
    }

    public static boolean hasExtensions(File root, String... extensions) {
        if (root != null && root.exists()) {
            if (root.isDirectory()) {
                File[] children = root.listFiles();
                if (children != null) {
                    for (File child : children) {
                        if (child.isDirectory()) {
                            boolean has = hasExtensions(child, extensions);
                            if (has) {
                                return true;
                            }
                        } else {
                            for (String extension : extensions) {
                                if (child.getName().endsWith(extension)) {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * Returns true if at least one of the files exists.
     */
    public static boolean exists(File... files) {
        for (File file : files) {
            if (file.exists()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Ensure that any writes to the given file is written to the storage device that contains it.
     * @param fileToSync the file to fsync
     * @param isDir if true, the given file is a directory (we open for read and ignore IOExceptions,
     *  because not all file systems and operating systems allow to fsync on a directory)
     */
    public static void syncFile(File fileToSync, boolean isDir) throws IOException {
        IOUtils.fsync(fileToSync, isDir);
    }

    /**
     * Check that a directory exists, is a directory and is readable
     * by the current user
     */
    public static boolean isAccessibleDirectory(File directory, ESLogger logger) {
        assert directory != null && logger != null;

        if (!directory.exists()) {
            logger.debug("[{}] directory does not exist.", directory.getAbsolutePath());
            return false;
        }
        if (!directory.isDirectory()) {
            logger.debug("[{}] should be a directory but is not.", directory.getAbsolutePath());
            return false;
        }
        if (!directory.canRead()) {
            logger.debug("[{}] directory is not readable.", directory.getAbsolutePath());
            return false;
        }
        return true;
    }

    private FileSystemUtils() {}
}
