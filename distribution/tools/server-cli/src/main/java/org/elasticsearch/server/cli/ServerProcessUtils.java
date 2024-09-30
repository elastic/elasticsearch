/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;

public class ServerProcessUtils {

    /**
     * Returns the java.io.tmpdir Elasticsearch should use, creating it if necessary.
     *
     * <p> On non-Windows OS, this will be created as a subdirectory of the default temporary directory.
     * Note that this causes the created temporary directory to be a private temporary directory.
     */
    public static Path setupTempDir(ProcessInfo processInfo) throws UserException {
        final Path path;
        String tmpDirOverride = processInfo.envVars().get("ES_TMPDIR");
        if (tmpDirOverride != null) {
            path = Paths.get(tmpDirOverride);
            if (Files.exists(path) == false) {
                throw new UserException(ExitCodes.CONFIG, "Temporary directory [" + path + "] does not exist or is not accessible");
            }
            if (Files.isDirectory(path) == false) {
                throw new UserException(ExitCodes.CONFIG, "Temporary directory [" + path + "] is not a directory");
            }
        } else {
            try {
                if (processInfo.sysprops().get("os.name").startsWith("Windows")) {
                    /*
                     * On Windows, we avoid creating a unique temporary directory per invocation lest
                     * we pollute the temporary directory. On other operating systems, temporary directories
                     * will be cleaned automatically via various mechanisms (e.g., systemd, or restarts).
                     */
                    path = Paths.get(processInfo.sysprops().get("java.io.tmpdir"), "elasticsearch");
                    Files.createDirectories(path);
                } else {
                    path = createTempDirectory("elasticsearch-");
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return path;
    }

    @SuppressForbidden(reason = "Files#createTempDirectory(String, FileAttribute...)")
    private static Path createTempDirectory(final String prefix, final FileAttribute<?>... attrs) throws IOException {
        return Files.createTempDirectory(prefix, attrs);
    }
}
