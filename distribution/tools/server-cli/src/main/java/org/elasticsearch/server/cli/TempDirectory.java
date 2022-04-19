/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.util.Map;

/**
 * Provides a path for a temporary directory. On non-Windows OS, this will be created as a sub-directory of the default temporary directory.
 * Note that this causes the created temporary directory to be a private temporary directory.
 */
final class TempDirectory {

    /**
     * Ensures the environment map has ES_TMPDIR and LIBFFI_TMPDIR.
     */
    public static Path initialize(Map<String, String> env) throws UserException {
        final Path path;
        String existingTempDir = env.remove("ES_TMPDIR");
        if (existingTempDir != null) {
            path = Paths.get(existingTempDir);
        } else {
            try {
                if (System.getProperty("os.name").startsWith("Windows")) {
                    /*
                     * On Windows, we avoid creating a unique temporary directory per invocation lest we pollute the temporary directory. On other
                     * operating systems, temporary directories will be cleaned automatically via various mechanisms (e.g., systemd, or restarts).
                     */
                    path = Paths.get(System.getProperty("java.io.tmpdir"), "elasticsearch");
                    Files.createDirectories(path);
                } else {
                    path = createTempDirectory("elasticsearch-");
                }
            } catch (IOException e) {
                // TODO: don't mask this exception, should we just propagate or try to summarize? in shell-land we would have printed it and
                // exited
                throw new UserException(ExitCodes.CONFIG, "Could not create temporary directory");
            }
        }

        if (env.containsKey("LIBFFI_TMPDIR") == false) {
            env.put("LIBFFI_TMPDIR", path.toString());
        }
        return path;
    }

    @SuppressForbidden(reason = "Files#createTempDirectory(String, FileAttribute...)")
    private static Path createTempDirectory(final String prefix, final FileAttribute<?>... attrs) throws IOException {
        return Files.createTempDirectory(prefix, attrs);
    }

}
