/*
 * @notice
 * Copyright 2011-2019 the original author or authors.
 * Modifications copyright (C) 2026 Elasticsearch B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file is derived from the nebula gradle-ospackage-plugin
 * (https://github.com/nebula-plugins/gradle-ospackage-plugin), reduced to the
 * functionality used by the Elasticsearch build and rewritten in Java.
 */

package org.elasticsearch.gradle.internal.ospackage;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Symlink and permission helpers shared by the packaging walker and writers.
 */
public final class PackagingUtils {

    private static final int EXECUTE_MASK = 0111;
    private static final int DEFAULT_FILE_PERMISSION = 0644;
    private static final int OWNER_RWX_GROUP_RX_OTHER_RX = 0755;
    private static final int ALL_READ_EXECUTE = 0555;

    private PackagingUtils() {}

    public static boolean isSymbolicLink(File file) {
        return Files.isSymbolicLink(file.toPath());
    }

    /**
     * Returns the raw link target when {@code file} is a symbolic link whose target resolves
     * within {@code root} (and can therefore be represented as a package link entry), or
     * {@code null} when the file is not a link or points outside the packaged tree (in which case
     * the link target's content is packaged as a regular file, matching the original plugin).
     */
    public static String relativeLinkTarget(Path root, File file) {
        if (Files.isSymbolicLink(file.toPath()) == false) {
            return null;
        }
        try {
            Path target = Files.readSymbolicLink(file.toPath());
            Path resolved = file.getParentFile().toPath().resolve(target).normalize();
            if (resolved.startsWith(root.normalize())) {
                return target.toString();
            }
            return null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Gets the unix permissions of a visited file. Works around the Gradle permissions API not
     * always reflecting executable bits of the underlying file by falling back to filesystem-level
     * checks in that case.
     */
    public static int getUnixPermission(int apiMode, File file) {
        if (file != null && file.canExecute() && (apiMode & EXECUTE_MASK) == 0) {
            boolean readable = file.canRead();
            boolean writable = file.canWrite();
            if (readable && writable) {
                return OWNER_RWX_GROUP_RX_OTHER_RX;
            } else if (readable) {
                return ALL_READ_EXECUTE;
            } else {
                return DEFAULT_FILE_PERMISSION;
            }
        }
        return apiMode;
    }
}
