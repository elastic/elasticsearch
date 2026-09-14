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

import org.gradle.api.file.FileCopyDetails;
import org.gradle.api.internal.file.copy.CopySpecInternal;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Small helpers shared by the rpm and deb copy actions: path mapping, symlink relativization and
 * unix permission extraction.
 */
public final class PackagingUtils {

    private static final int EXECUTE_MASK = 0111;
    private static final int DEFAULT_FILE_PERMISSION = 0644;
    private static final int OWNER_RWX_GROUP_RX_OTHER_RX = 0755;
    private static final int ALL_READ_EXECUTE = 0555;

    private PackagingUtils() {}

    /** A relativized symlink: {@link #path()} inside the package pointing at {@link #target()}. */
    public record Symlink(String path, String target) {}

    public static String getRootPath(FileCopyDetails details) {
        return "/" + details.getPath();
    }

    public static boolean isSymbolicLink(File file) {
        return Files.isSymbolicLink(file.toPath());
    }

    /** Returns the closest ancestor directory that is a symbolic link, or null. */
    public static File parentSymbolicLink(File file) {
        File parent = file.getParentFile();
        if (parent == null || isSymbolicLink(parent)) {
            return parent;
        }
        return parentSymbolicLink(parent);
    }

    /**
     * Translates a symlink found in the copied file tree into a link entry within the package,
     * provided the link target lies within the copied tree. Returns {@code null} when the link
     * cannot be represented relative to the copied sources.
     */
    public static Symlink relativizeSymlink(FileCopyDetails details, File target) {
        try {
            String sourcePath = details.getFile().getPath();
            String sourceBasePath = sourcePath.substring(0, sourcePath.length() - details.getRelativeSourcePath().getPathString().length());
            if (target.getPath().startsWith(sourceBasePath) == false) {
                return null;
            }
            String sourceRelative = target.getPath().substring(sourceBasePath.length());
            String sourceBase = details.getPath().substring(0, details.getPath().indexOf(sourceRelative));

            File targetFile = Files.readSymbolicLink(target.toPath()).toFile();
            String targetPath = targetFile.isAbsolute()
                ? targetFile.getPath()
                : new File(target.getParentFile(), targetFile.getPath()).getCanonicalPath();

            if (targetPath.startsWith(sourceBasePath)) {
                File sourceRoot = new File("/" + sourceBase, sourceRelative);
                File targetRoot = new File("/" + sourceBase, targetPath.substring(sourceBasePath.length()));
                Path relativeTarget = sourceRoot.isDirectory()
                    ? sourceRoot.toPath().relativize(targetRoot.toPath())
                    : sourceRoot.getParentFile().toPath().relativize(targetRoot.toPath());
                return new Symlink(sourceRoot.getPath(), relativeTarget.toString());
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Gets the unix permissions of a visited file. Works around the Gradle permissions API not
     * always reflecting executable bits of the underlying file by falling back to filesystem-level
     * checks in that case.
     */
    public static int getUnixPermission(FileCopyDetails details) {
        int newApiMode = details.getPermissions().toUnixNumeric();
        try {
            File file = details.getFile();
            if (file != null && file.canExecute() && (newApiMode & EXECUTE_MASK) == 0) {
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
        } catch (UnsupportedOperationException e) {
            // filtered copies cannot expose the backing file; fall through to the API value
        }
        return newApiMode;
    }

    /**
     * Returns explicitly configured file permissions of a spec, or {@code null} when the spec uses
     * defaults. Permissions equal to 0644 are only considered explicit when the spec shows other
     * signs of dedicated configuration (include or exclude patterns).
     */
    public static Integer getFileMode(CopySpecInternal spec) {
        if (spec == null || spec.getFilePermissions().isPresent() == false) {
            return null;
        }
        int numeric = spec.getFilePermissions().get().toUnixNumeric();
        boolean hasExplicitConfiguration = spec.getIncludes().isEmpty() == false || spec.getExcludes().isEmpty() == false;
        if (hasExplicitConfiguration || numeric != DEFAULT_FILE_PERMISSION) {
            return numeric;
        }
        return null;
    }

    /**
     * Returns explicitly configured directory permissions of a spec, or {@code null} when the spec
     * uses defaults.
     */
    public static Integer getDirMode(CopySpecInternal spec) {
        if (spec == null || spec.getDirPermissions().isPresent() == false) {
            return null;
        }
        return spec.getDirPermissions().get().toUnixNumeric();
    }
}
