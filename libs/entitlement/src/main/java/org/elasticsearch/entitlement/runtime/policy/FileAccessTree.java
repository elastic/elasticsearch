/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.core.PathUtils.getDefaultFileSystem;

public final class FileAccessTree {
    public static final FileAccessTree EMPTY = new FileAccessTree(FilesEntitlement.EMPTY);
    private static final String FILE_SEPARATOR = getDefaultFileSystem().getSeparator();

    private final String[] readPaths;
    private final String[] writePaths;

    private FileAccessTree(FilesEntitlement filesEntitlement) {
        List<String> readPaths = new ArrayList<>();
        List<String> writePaths = new ArrayList<>();
        for (FilesEntitlement.FileData fileData : filesEntitlement.filesData()) {
            var path = normalizePath(Path.of(fileData.path()));
            var mode = fileData.mode();
            if (mode == FilesEntitlement.Mode.READ_WRITE) {
                writePaths.add(path);
            }
            readPaths.add(path);
        }

        readPaths.sort(String::compareTo);
        writePaths.sort(String::compareTo);

        this.readPaths = readPaths.toArray(new String[0]);
        this.writePaths = writePaths.toArray(new String[0]);
    }

    public static FileAccessTree of(FilesEntitlement filesEntitlement) {
        return new FileAccessTree(filesEntitlement);
    }

    boolean canRead(Path path) {
        return checkPath(normalizePath(path), readPaths);
    }

    boolean canWrite(Path path) {
        return checkPath(normalizePath(path), writePaths);
    }

    /**
     * @return the "canonical" form of the given {@code path}, to be used for entitlement checks.
     */
    static String normalizePath(Path path) {
        // Note that toAbsolutePath produces paths separated by the default file separator,
        // so on Windows, if the given path uses forward slashes, this consistently
        // converts it to backslashes.
        return path.toAbsolutePath().normalize().toString();
    }

    private static boolean checkPath(String path, String[] paths) {
        if (paths.length == 0) {
            return false;
        }
        int ndx = Arrays.binarySearch(paths, path);
        if (ndx < -1) {
            String maybeParent = paths[-ndx - 2];
            return path.startsWith(maybeParent) && path.startsWith(FILE_SEPARATOR, maybeParent.length());
        }
        return ndx >= 0;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        FileAccessTree that = (FileAccessTree) o;
        return Objects.deepEquals(readPaths, that.readPaths) && Objects.deepEquals(writePaths, that.writePaths);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(readPaths), Arrays.hashCode(writePaths));
    }
}
