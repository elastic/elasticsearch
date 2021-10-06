/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

public class PermissionUtils {

    public static void chmod(Path path, int mode) throws IOException {
        final PosixFileAttributeView view = Files.getFileAttributeView(path, PosixFileAttributeView.class);
        if (view != null && (mode != 0)) {
            final Set<PosixFilePermission> permissions = permissionsFromInt(mode);
            Files.setPosixFilePermissions(path, permissions);
        }
    }

    private static Set<PosixFilePermission> permissionsFromInt(int mode) {
        return PosixFilePermissions.fromString(
            permissions((mode >> 6) & 07) + permissions((mode >> 3) & 07) + permissions((mode >> 0) & 07)
        );
    }

    private static String permissions(final int permissions) {
        if (permissions < 0 || permissions > 7) {
            throw new IllegalArgumentException("permissions [" + permissions + "] out of range");
        }
        final StringBuilder sb = new StringBuilder(3);
        if ((permissions & 4) == 4) {
            sb.append('r');
        } else {
            sb.append('-');
        }
        if ((permissions & 2) == 2) {
            sb.append('w');
        } else {
            sb.append('-');
        }
        if ((permissions & 1) == 1) {
            sb.append('x');
        } else {
            sb.append('-');
        }
        return sb.toString();
    }

}
