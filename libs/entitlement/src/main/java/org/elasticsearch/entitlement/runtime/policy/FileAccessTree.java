/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.core.Strings;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement.Mode;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

import static org.elasticsearch.core.PathUtils.getDefaultFileSystem;

public final class FileAccessTree {

    /**
     * An intermediary structure to help build exclusive paths for files entitlements.
     */
    record ExclusiveFileEntitlement(String componentName, String moduleName, FilesEntitlement filesEntitlement) {}

    /**
     * An intermediary structure to help globally validate exclusive paths, and then build exclusive paths for individual modules.
     */
    record ExclusivePath(String componentName, String moduleName, String path) {

        @Override
        public String toString() {
            return "[[" + componentName + "] [" + moduleName + "] [" + path + "]]";
        }
    }

    static List<ExclusivePath> buildExclusivePathList(List<ExclusiveFileEntitlement> exclusiveFileEntitlements, PathLookup pathLookup) {
        List<ExclusivePath> exclusivePaths = new ArrayList<>();
        for (ExclusiveFileEntitlement efe : exclusiveFileEntitlements) {
            for (FilesEntitlement.FileData fd : efe.filesEntitlement().filesData()) {
                if (fd.exclusive()) {
                    List<Path> paths = fd.resolvePaths(pathLookup).toList();
                    for (Path path : paths) {
                        exclusivePaths.add(new ExclusivePath(efe.componentName(), efe.moduleName(), normalizePath(path)));
                    }
                }
            }
        }
        exclusivePaths.sort((ep1, ep2) -> PATH_ORDER.compare(ep1.path(), ep2.path()));
        return exclusivePaths;
    }

    static void validateExclusivePaths(List<ExclusivePath> exclusivePaths) {
        if (exclusivePaths.isEmpty() == false) {
            ExclusivePath currentExclusivePath = exclusivePaths.get(0);
            for (int i = 1; i < exclusivePaths.size(); ++i) {
                ExclusivePath nextPath = exclusivePaths.get(i);
                if (currentExclusivePath.path().equals(nextPath.path) || isParent(currentExclusivePath.path(), nextPath.path())) {
                    throw new IllegalArgumentException(
                        "duplicate/overlapping exclusive paths found in files entitlements: " + currentExclusivePath + " and " + nextPath
                    );
                }
                currentExclusivePath = nextPath;
            }
        }
    }

    private static final Logger logger = LogManager.getLogger(FileAccessTree.class);
    private static final String FILE_SEPARATOR = getDefaultFileSystem().getSeparator();

    private final String[] exclusivePaths;
    private final String[] readPaths;
    private final String[] writePaths;

    private FileAccessTree(
        String componentName,
        String moduleName,
        FilesEntitlement filesEntitlement,
        PathLookup pathLookup,
        List<ExclusivePath> exclusivePaths
    ) {
        List<String> updatedExclusivePaths = new ArrayList<>();
        for (ExclusivePath exclusivePath : exclusivePaths) {
            if (exclusivePath.componentName().equals(componentName) == false || exclusivePath.moduleName().equals(moduleName) == false) {
                updatedExclusivePaths.add(exclusivePath.path());
            }
        }

        List<String> readPaths = new ArrayList<>();
        List<String> writePaths = new ArrayList<>();
        BiConsumer<Path, Mode> addPath = (path, mode) -> {
            var normalized = normalizePath(path);
            if (mode == Mode.READ_WRITE) {
                writePaths.add(normalized);
            }
            readPaths.add(normalized);
        };
        BiConsumer<Path, Mode> addPathAndMaybeLink = (path, mode) -> {
            addPath.accept(path, mode);
            // also try to follow symlinks. Lucene does this and writes to the target path.
            if (Files.exists(path)) {
                try {
                    Path realPath = path.toRealPath();
                    if (realPath.equals(path) == false) {
                        addPath.accept(realPath, mode);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
        for (FilesEntitlement.FileData fileData : filesEntitlement.filesData()) {
            var platform = fileData.platform();
            if (platform != null && platform.isCurrent() == false) {
                continue;
            }
            var mode = fileData.mode();
            var paths = fileData.resolvePaths(pathLookup);
            paths.forEach(path -> {
                if (path == null) {
                    // TODO: null paths shouldn't be allowed, but they can occur due to repo paths
                    return;
                }
                addPathAndMaybeLink.accept(path, mode);
            });
        }

        // everything has access to the temp dir and the jdk
        addPathAndMaybeLink.accept(pathLookup.tempDir(), Mode.READ_WRITE);

        // TODO: watcher uses javax.activation which looks for known mime types configuration, should this be global or explicit in watcher?
        Path jdk = Paths.get(System.getProperty("java.home"));
        addPathAndMaybeLink.accept(jdk.resolve("conf"), Mode.READ);

        updatedExclusivePaths.sort(PATH_ORDER);
        readPaths.sort(PATH_ORDER);
        writePaths.sort(PATH_ORDER);

        this.exclusivePaths = updatedExclusivePaths.toArray(new String[0]);
        this.readPaths = pruneSortedPaths(readPaths).toArray(new String[0]);
        this.writePaths = pruneSortedPaths(writePaths).toArray(new String[0]);
    }

    private static List<String> pruneSortedPaths(List<String> paths) {
        List<String> prunedReadPaths = new ArrayList<>();
        if (paths.isEmpty() == false) {
            String currentPath = paths.get(0);
            prunedReadPaths.add(currentPath);
            for (int i = 1; i < paths.size(); ++i) {
                String nextPath = paths.get(i);
                if (isParent(currentPath, nextPath) == false) {
                    prunedReadPaths.add(nextPath);
                    currentPath = nextPath;
                }
            }
        }
        return prunedReadPaths;
    }

    public static FileAccessTree of(
        String componentName,
        String moduleName,
        FilesEntitlement filesEntitlement,
        PathLookup pathLookup,
        List<ExclusivePath> exclusivePaths
    ) {
        return new FileAccessTree(componentName, moduleName, filesEntitlement, pathLookup, exclusivePaths);
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
        String result = path.toAbsolutePath().normalize().toString();
        while (result.endsWith(FILE_SEPARATOR)) {
            result = result.substring(0, result.length() - FILE_SEPARATOR.length());
        }
        return result;
    }

    private boolean checkPath(String path, String[] paths) {
        logger.trace(() -> Strings.format("checking [%s] against [%s]", path, String.join(",", paths)));
        if (paths.length == 0) {
            return false;
        }

        int endx = Arrays.binarySearch(exclusivePaths, path, PATH_ORDER);
        if (endx < -1 && isParent(exclusivePaths[-endx - 2], path) || endx >= 0) {
            return false;
        }

        int ndx = Arrays.binarySearch(paths, path, PATH_ORDER);
        if (ndx < -1) {
            return isParent(paths[-ndx - 2], path);
        }
        return ndx >= 0;
    }

    private static boolean isParent(String maybeParent, String path) {
        logger.trace(() -> Strings.format("checking isParent [%s] for [%s]", maybeParent, path));
        return path.startsWith(maybeParent) && path.startsWith(FILE_SEPARATOR, maybeParent.length());
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

    /**
     * For our lexicographic sort trick to work correctly, we must have path separators sort before
     * any other character so that files in a directory appear immediately after that directory.
     * For example, we require [/a, /a/b, /a.xml] rather than the natural order [/a, /a.xml, /a/b].
     */
    private static final Comparator<String> PATH_ORDER = (s1, s2) -> {
        Path p1 = Path.of(s1);
        Path p2 = Path.of(s2);
        var i1 = p1.iterator();
        var i2 = p2.iterator();
        while (i1.hasNext() && i2.hasNext()) {
            int cmp = i1.next().compareTo(i2.next());
            if (cmp != 0) {
                return cmp;
            }
        }
        if (i1.hasNext()) {
            return 1;
        } else if (i2.hasNext()) {
            return -1;
        } else {
            assert p1.equals(p2);
            return 0;
        }
    };
}
