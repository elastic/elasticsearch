/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
