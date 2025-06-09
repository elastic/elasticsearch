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
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement.Mode;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

import static java.util.Comparator.comparing;
import static org.elasticsearch.core.PathUtils.getDefaultFileSystem;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.CONFIG;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.TEMP;
import static org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement.Mode.READ_WRITE;

/**
 * <p>
 * This structure facilitates looking up files entitlements for a particular component+module combination, given that grants can occur
 * at the directory level in addition to the individual file level.
 * </p>
 * <p>
 * Broadly, this class operates on strings rather than abstractions like {@link java.io.File} or {@link Path}, since those can have
 * behaviour that varies by platform in surprising ways; using strings makes the behaviour predictable. The strings are produced by a
 * method called {@link FileAccessTree#normalizePath} to make sure they are absolute paths with consistent separator characters.
 * </p>
 * <p>
 * Internally, it does not use a tree data structure; the name "tree" refers to the tree structure of the filesystem, not the choice of
 * data structure. It takes advantage of the fact that, after normalization, the name of a directory containing a file is a prefix of
 * the file's own name. The permissions are maintained in several sorted arrays of {@link String}, and permission checks are implemented
 * as a binary search within these arrays.
 * </p>
 * <p>
 * We want the binary search to locate the relevant entry immediately, either because there's an exact match, or else because there's no
 * exact match and the binary search points us as an entry for the containing directory. This seems straightforward if the paths are
 * absolute and sorted, but there are several subtleties.
 * </p>
 * <p>
 * Firstly, there could be intervening siblings; for example, in {@code ["/a", "/a/b"]}, if we look up {@code "/a/c"}, the binary search
 * will land on {@code "/a/b"}, which is not a relevant entry for {@code "/a/c"}. The solution here is (1) segregate the read and write
 * permissions so that each array covers one homogeneous kind of permission, and (2) prune the list so redundant child entries are removed
 * when they are already covered by parent entries. In our example, this pruning process would produce the array {@code ["/a"]} only,
 * and so the binary search for {@code "/a/c"} lands on the relevant entry for the containing directory.
 * </p>
 * <p>
 * Secondly, the path separator (whether slash or backslash) sorts after the dot character. This means, for example, if the array contains
 * {@code ["/a", "/a.xml"]} and we look up {@code "/a/b"} using normal string comparison, the binary search would land on {@code "/a.xml"},
 * which is neither an exact match nor a containing directory, and so the lookup would incorrectly report no match, even though
 * {@code "/a"} is in the array. To fix this, we define {@link FileAccessTreeComparison#pathComparator()} which sorts path separators
 * before any other character. In the example, this would cause {@code "/a/b"} to sort between {@code "/a"} and {@code "/a.xml"} so that
 * it correctly finds {@code "/a"}.
 * </p>
 * With the paths pruned, sorted, and segregated by permission, each binary search has the following properties:
 * <ul>
 * <li>
 * If an exact match exists, it will be found at the returned index
 * </li>
 * <li>
 * Else, if a containing folder exists, it will be found immediately before the returned index
 * </li>
 * <li>
 * Else, there is no match
 * </li>
 * </ul>
 * Permission is granted if both:
 * <ul>
 * <li>
 * there is no match in exclusivePaths, and
 * </li>
 * <li>
 * there is a match in the array corresponding to the desired operation (read or write).
 * </li>
 * </ul>
 * <p>
 * Some additional care is required in the unit tests for this code, since it must also run on Windows where the separator is a
 * backslash and absolute paths don't start with a separator. See {@code FileAccessTreeTests#testDuplicateExclusivePaths} for an example.
 * </p>
 */
public final class FileAccessTree {

    /**
     * An intermediary structure to help build exclusive paths for files entitlements.
     */
    record ExclusiveFileEntitlement(String componentName, String moduleName, FilesEntitlement filesEntitlement) {}

    /**
     * An intermediary structure to help globally validate exclusive paths, and then build exclusive paths for individual modules.
     */
    record ExclusivePath(String componentName, Set<String> moduleNames, String path) {

        @Override
        public String toString() {
            return "[[" + componentName + "] " + moduleNames + " [" + path + "]]";
        }
    }

    static List<ExclusivePath> buildExclusivePathList(
        List<ExclusiveFileEntitlement> exclusiveFileEntitlements,
        PathLookup pathLookup,
        FileAccessTreeComparison comparison
    ) {
        Map<String, ExclusivePath> exclusivePaths = new HashMap<>();
        for (ExclusiveFileEntitlement efe : exclusiveFileEntitlements) {
            for (FilesEntitlement.FileData fd : efe.filesEntitlement().filesData()) {
                if (fd.exclusive()) {
                    List<Path> paths = fd.resolvePaths(pathLookup).toList();
                    for (Path path : paths) {
                        String normalizedPath = normalizePath(path);
                        var exclusivePath = exclusivePaths.computeIfAbsent(
                            normalizedPath,
                            k -> new ExclusivePath(efe.componentName(), new HashSet<>(), normalizedPath)
                        );
                        if (exclusivePath.componentName().equals(efe.componentName()) == false) {
                            throw new IllegalArgumentException(
                                "Path ["
                                    + normalizedPath
                                    + "] is already exclusive to ["
                                    + exclusivePath.componentName()
                                    + "]"
                                    + exclusivePath.moduleNames
                                    + ", cannot add exclusive access for ["
                                    + efe.componentName()
                                    + "]["
                                    + efe.moduleName
                                    + "]"
                            );
                        }
                        exclusivePath.moduleNames.add(efe.moduleName());
                    }
                }
            }
        }
        return exclusivePaths.values().stream().sorted(comparing(ExclusivePath::path, comparison.pathComparator())).distinct().toList();
    }

    static void validateExclusivePaths(List<ExclusivePath> exclusivePaths, FileAccessTreeComparison comparison) {
        if (exclusivePaths.isEmpty() == false) {
            ExclusivePath currentExclusivePath = exclusivePaths.get(0);
            for (int i = 1; i < exclusivePaths.size(); ++i) {
                ExclusivePath nextPath = exclusivePaths.get(i);
                if (comparison.samePath(currentExclusivePath.path(), nextPath.path)
                    || comparison.isParent(currentExclusivePath.path(), nextPath.path())) {
                    throw new IllegalArgumentException(
                        "duplicate/overlapping exclusive paths found in files entitlements: " + currentExclusivePath + " and " + nextPath
                    );
                }
                currentExclusivePath = nextPath;
            }
        }
    }

    @SuppressForbidden(reason = "we need the separator as a char, not a string")
    static char separatorChar() {
        return File.separatorChar;
    }

    private static final Logger logger = LogManager.getLogger(FileAccessTree.class);
    private static final String FILE_SEPARATOR = getDefaultFileSystem().getSeparator();
    static final FileAccessTreeComparison DEFAULT_COMPARISON = Platform.LINUX.isCurrent()
        ? new CaseSensitiveComparison(separatorChar())
        : new CaseInsensitiveComparison(separatorChar());

    private final FileAccessTreeComparison comparison;
    /**
     * lists paths that are forbidden for this component+module because some other component has granted exclusive access to one of its
     * modules
     */
    private final String[] exclusivePaths;
    /**
     * lists paths for which the component has granted read or read_write access to the module
     */
    private final String[] readPaths;
    /**
     * lists paths for which the component has granted read_write access to the module
     */
    private final String[] writePaths;

    private static String[] buildUpdatedAndSortedExclusivePaths(
        String componentName,
        String moduleName,
        List<ExclusivePath> exclusivePaths,
        FileAccessTreeComparison comparison
    ) {
        List<String> updatedExclusivePaths = new ArrayList<>();
        for (ExclusivePath exclusivePath : exclusivePaths) {
            if (exclusivePath.componentName().equals(componentName) == false || exclusivePath.moduleNames().contains(moduleName) == false) {
                updatedExclusivePaths.add(exclusivePath.path());
            }
        }
        updatedExclusivePaths.sort(comparison.pathComparator());
        return updatedExclusivePaths.toArray(new String[0]);
    }

    FileAccessTree(
        FilesEntitlement filesEntitlement,
        PathLookup pathLookup,
        Iterable<Path> componentPaths,
        String[] sortedExclusivePaths,
        FileAccessTreeComparison comparison
    ) {
        this.comparison = comparison;
        List<String> readPaths = new ArrayList<>();
        List<String> writePaths = new ArrayList<>();
        BiConsumer<Path, Mode> addPath = (path, mode) -> {
            var normalized = normalizePath(path);
            if (mode == READ_WRITE) {
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

        // everything has access to the temp dir, config dir, to their own dir (their own jar files) and the jdk
        pathLookup.getBaseDirPaths(TEMP).forEach(tempPath -> addPathAndMaybeLink.accept(tempPath, READ_WRITE));
        // TODO: this grants read access to the config dir for all modules until explicit read entitlements can be added
        pathLookup.getBaseDirPaths(CONFIG).forEach(configPath -> addPathAndMaybeLink.accept(configPath, Mode.READ));
        if (componentPaths != null) {
            componentPaths.forEach(path -> addPathAndMaybeLink.accept(path, Mode.READ));
        }

        // TODO: watcher uses javax.activation which looks for known mime types configuration, should this be global or explicit in watcher?
        Path jdk = Paths.get(System.getProperty("java.home"));
        addPathAndMaybeLink.accept(jdk.resolve("conf"), Mode.READ);

        readPaths.sort(comparison.pathComparator());
        writePaths.sort(comparison.pathComparator());

        this.exclusivePaths = sortedExclusivePaths;
        this.readPaths = pruneSortedPaths(readPaths, comparison).toArray(new String[0]);
        this.writePaths = pruneSortedPaths(writePaths, comparison).toArray(new String[0]);

        logger.debug(
            () -> Strings.format(
                "Created FileAccessTree with paths: exclusive [%s], read [%s], write [%s]",
                String.join(",", this.exclusivePaths),
                String.join(",", this.readPaths),
                String.join(",", this.writePaths)
            )
        );
    }

    // package private for testing
    static List<String> pruneSortedPaths(List<String> paths, FileAccessTreeComparison comparison) {
        List<String> prunedReadPaths = new ArrayList<>();
        if (paths.isEmpty() == false) {
            String currentPath = paths.get(0);
            prunedReadPaths.add(currentPath);
            for (int i = 1; i < paths.size(); ++i) {
                String nextPath = paths.get(i);
                if (comparison.samePath(currentPath, nextPath) == false && comparison.isParent(currentPath, nextPath) == false) {
                    prunedReadPaths.add(nextPath);
                    currentPath = nextPath;
                }
            }
        }
        return prunedReadPaths;
    }

    static FileAccessTree of(
        String componentName,
        String moduleName,
        FilesEntitlement filesEntitlement,
        PathLookup pathLookup,
        Iterable<Path> componentPaths,
        List<ExclusivePath> exclusivePaths
    ) {
        return new FileAccessTree(
            filesEntitlement,
            pathLookup,
            componentPaths,
            buildUpdatedAndSortedExclusivePaths(componentName, moduleName, exclusivePaths, DEFAULT_COMPARISON),
            DEFAULT_COMPARISON
        );
    }

    /**
     * A special factory method to create a FileAccessTree with no ExclusivePaths, e.g. for quick validation or for default file access
     */
    public static FileAccessTree withoutExclusivePaths(
        FilesEntitlement filesEntitlement,
        PathLookup pathLookup,
        Iterable<Path> componentPaths
    ) {
        return new FileAccessTree(filesEntitlement, pathLookup, componentPaths, new String[0], DEFAULT_COMPARISON);
    }

    public boolean canRead(Path path) {
        var normalizedPath = normalizePath(path);
        var canRead = checkPath(normalizedPath, readPaths);
        logger.trace(() -> Strings.format("checking [%s] (normalized to [%s]) for read: %b", path, normalizedPath, canRead));
        return canRead;
    }

    public boolean canWrite(Path path) {
        var normalizedPath = normalizePath(path);
        var canWrite = checkPath(normalizedPath, writePaths);
        logger.trace(() -> Strings.format("checking [%s] (normalized to [%s]) for write: %b", path, normalizedPath, canWrite));
        return canWrite;
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
        if (paths.length == 0) {
            return false;
        }

        int endx = Arrays.binarySearch(exclusivePaths, path, comparison.pathComparator());
        if (endx < -1 && comparison.isParent(exclusivePaths[-endx - 2], path) || endx >= 0) {
            return false;
        }

        int ndx = Arrays.binarySearch(paths, path, comparison.pathComparator());
        if (ndx < -1) {
            return comparison.isParent(paths[-ndx - 2], path);
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
