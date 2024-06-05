/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.core.SuppressForbidden;

import java.io.FilePermission;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Permissions;

public class FilePermissionUtils {

    /** no instantiation */
    private FilePermissionUtils() {}

    /**
     * Add access to single file path
     * @param policy current policy to add permissions to
     * @param path the path itself
     * @param permissions set of file permissions to grant to the path
     */
    @SuppressForbidden(reason = "only place where creating Java-9 compatible FilePermission objects is possible")
    public static void addSingleFilePath(Permissions policy, Path path, String permissions) throws IOException {
        policy.add(new FilePermission(path.toString(), permissions));
        if (Files.exists(path)) {
            /*
             * The file permission model since JDK 9 requires this due to the removal of pathname canonicalization. See also
             * https://github.com/elastic/elasticsearch/issues/21534.
             */
            final Path realPath = path.toRealPath();
            if (path.toString().equals(realPath.toString()) == false) {
                policy.add(new FilePermission(realPath.toString(), permissions));
            }
        }
    }

    /**
     * Add access to path with direct and/or recursive access. This also creates the directory if it does not exist.
     *
     * @param policy            current policy to add permissions to
     * @param configurationName the configuration name associated with the path (for error messages only)
     * @param path              the path itself
     * @param permissions       set of file permissions to grant to the path
     * @param recursiveAccessOnly   indicates if the permission should provide recursive access to files underneath
     */
    @SuppressForbidden(reason = "only place where creating Java-9 compatible FilePermission objects is possible")
    public static void addDirectoryPath(
        Permissions policy,
        String configurationName,
        Path path,
        String permissions,
        boolean recursiveAccessOnly
    ) throws IOException {
        // paths may not exist yet, this also checks accessibility
        try {
            Security.ensureDirectoryExists(path);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to access '" + configurationName + "' (" + path + ")", e);
        }

        // For some file permissions (data.path) we create a Permissions object that only checks the concrete
        // path. Adding the directory would only create more overhead for this fast path.
        if (recursiveAccessOnly == false) {
            // add access for path itself
            policy.add(new FilePermission(path.toString(), permissions));
        }
        policy.add(new FilePermission(path.toString() + path.getFileSystem().getSeparator() + "-", permissions));
        /*
         * The file permission model since JDK 9 requires this due to the removal of pathname canonicalization. See also
         * https://github.com/elastic/elasticsearch/issues/21534.
         */
        final Path realPath = path.toRealPath();
        if (path.toString().equals(realPath.toString()) == false) {
            if (recursiveAccessOnly == false) {
                // add access for path itself
                policy.add(new FilePermission(realPath.toString(), permissions));
            }
            // add access for files underneath
            policy.add(new FilePermission(realPath.toString() + realPath.getFileSystem().getSeparator() + "-", permissions));
        }
    }
}
