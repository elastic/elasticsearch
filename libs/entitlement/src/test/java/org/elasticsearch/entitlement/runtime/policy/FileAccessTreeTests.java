/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.runtime.policy.FileAccessTree.ExclusiveFileEntitlement;
import org.elasticsearch.entitlement.runtime.policy.FileAccessTree.ExclusivePath;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement.FileData;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.core.PathUtils.getDefaultFileSystem;
import static org.elasticsearch.entitlement.runtime.policy.FileAccessTree.buildExclusivePathList;
import static org.elasticsearch.entitlement.runtime.policy.FileAccessTree.normalizePath;
import static org.elasticsearch.entitlement.runtime.policy.FileAccessTree.separatorChar;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.CONFIG;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.TEMP;
import static org.elasticsearch.entitlement.runtime.policy.Platform.WINDOWS;
import static org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement.Mode.READ;
import static org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement.Mode.READ_WRITE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class FileAccessTreeTests extends ESTestCase {

    static Path root;
    static Settings settings;

    @BeforeClass
    public static void setupRoot() {
        root = createTempDir();
        settings = Settings.EMPTY;
    }

    private static final PathLookup TEST_PATH_LOOKUP = new PathLookupImpl(
        Path.of("/home"),
        Path.of("/config"),
        new Path[] { Path.of("/data1"), Path.of("/data2") },
        Path.of("/shareddata"),
        new Path[] { Path.of("/shared1"), Path.of("/shared2") },
        Path.of("/lib"),
        Path.of("/modules"),
        Path.of("/plugins"),
        Path.of("/logs"),
        Path.of("/tmp"),
        null,
        pattern -> settings.getValues(pattern)
    );

    public void testEmpty() {
        var tree = accessTree(FilesEntitlement.EMPTY);
        assertThat(tree.canRead(path("path")), is(false));
        assertThat(tree.canWrite(path("path")), is(false));
    }

    public void testRead() {
        var tree = accessTree(entitlement("foo", "read"));
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canRead(path("foo/subdir")), is(true));
        assertThat(tree.canRead(path("food")), is(false));
        assertThat(tree.canWrite(path("foo")), is(false));
        assertThat(tree.canWrite(path("food")), is(false));

        assertThat(tree.canRead(path("before")), is(false));
        assertThat(tree.canRead(path("later")), is(false));
    }

    public void testWrite() {
        var tree = accessTree(entitlement("foo", "read_write"));
        assertThat(tree.canWrite(path("foo")), is(true));
        assertThat(tree.canWrite(path("foo/subdir")), is(true));
        assertThat(tree.canWrite(path("food")), is(false));
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canRead(path("food")), is(false));

        assertThat(tree.canWrite(path("before")), is(false));
        assertThat(tree.canWrite(path("later")), is(false));
    }

    public void testTwoPaths() {
        var tree = accessTree(entitlement("foo", "read", "bar", "read"));
        assertThat(tree.canRead(path("a")), is(false));
        assertThat(tree.canRead(path("bar")), is(true));
        assertThat(tree.canRead(path("bar/subdir")), is(true));
        assertThat(tree.canRead(path("c")), is(false));
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canRead(path("foo/subdir")), is(true));
        assertThat(tree.canRead(path("z")), is(false));
    }

    public void testReadWriteUnderRead() {
        var tree = accessTree(entitlement("foo", "read", "foo/bar", "read_write"));
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canWrite(path("foo")), is(false));
        assertThat(tree.canRead(path("foo/bar")), is(true));
        assertThat(tree.canWrite(path("foo/bar")), is(true));
        assertThat(tree.canRead(path("foo/baz")), is(true));
        assertThat(tree.canWrite(path("foo/baz")), is(false));
    }

    public void testPrunedPaths() {
        var tree = accessTree(entitlement("foo", "read", "foo/baz", "read", "foo/bar", "read"));
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canWrite(path("foo")), is(false));
        assertThat(tree.canRead(path("foo/bar")), is(true));
        assertThat(tree.canWrite(path("foo/bar")), is(false));
        assertThat(tree.canRead(path("foo/baz")), is(true));
        assertThat(tree.canWrite(path("foo/baz")), is(false));
        // also test a non-existent subpath
        assertThat(tree.canRead(path("foo/barf")), is(true));
        assertThat(tree.canWrite(path("foo/barf")), is(false));

        tree = accessTree(entitlement("foo", "read", "foo/bar", "read_write"));
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canWrite(path("foo")), is(false));
        assertThat(tree.canRead(path("foo/bar")), is(true));
        assertThat(tree.canWrite(path("foo/bar")), is(true));
        assertThat(tree.canRead(path("foo/baz")), is(true));
        assertThat(tree.canWrite(path("foo/baz")), is(false));
    }

    public void testPathAndFileWithSamePrefix() {
        var tree = accessTree(entitlement("foo/bar/", "read", "foo/bar.xml", "read"));
        assertThat(tree.canRead(path("foo")), is(false));
        assertThat(tree.canRead(path("foo/bar")), is(true));
        assertThat(tree.canRead(path("foo/bar/baz")), is(true));
        assertThat(tree.canRead(path("foo/bar.xml")), is(true));
        assertThat(tree.canRead(path("foo/bar.txt")), is(false));
    }

    public void testReadWithRelativePath() {
        for (var dir : List.of("home")) {
            var tree = accessTree(entitlement(Map.of("relative_path", "foo", "mode", "read", "relative_to", dir)));
            assertThat(tree.canRead(path("foo")), is(false));

            assertThat(tree.canRead(path("/" + dir + "/foo")), is(true));

            assertThat(tree.canRead(path("/" + dir + "/foo/subdir")), is(true));
            assertThat(tree.canRead(path("/" + dir + "/food")), is(false));
            assertThat(tree.canWrite(path("/" + dir + "/foo")), is(false));

            assertThat(tree.canRead(path("/" + dir)), is(false));
            assertThat(tree.canRead(path("/" + dir + "/before")), is(false));
            assertThat(tree.canRead(path("/" + dir + "/later")), is(false));
        }
    }

    public void testWriteWithRelativePath() {
        for (var dir : List.of("home")) {
            var tree = accessTree(entitlement(Map.of("relative_path", "foo", "mode", "read_write", "relative_to", dir)));
            assertThat(tree.canWrite(path("/" + dir + "/foo")), is(true));
            assertThat(tree.canWrite(path("/" + dir + "/foo/subdir")), is(true));
            assertThat(tree.canWrite(path("/" + dir)), is(false));
            assertThat(tree.canWrite(path("/" + dir + "/food")), is(false));
            assertThat(tree.canRead(path("/" + dir + "/foo")), is(true));
            assertThat(tree.canRead(path("/" + dir)), is(false));

            assertThat(tree.canWrite(path("/" + dir)), is(false));
            assertThat(tree.canWrite(path("/" + dir + "/before")), is(false));
            assertThat(tree.canWrite(path("/" + dir + "/later")), is(false));
        }
    }

    public void testMultipleDataDirs() {
        var tree = accessTree(entitlement(Map.of("relative_path", "foo", "mode", "read_write", "relative_to", "data")));
        assertThat(tree.canWrite(path("/data1/foo")), is(true));
        assertThat(tree.canWrite(path("/data2/foo")), is(true));
        assertThat(tree.canWrite(path("/data3/foo")), is(false));
        assertThat(tree.canWrite(path("/data1/foo/subdir")), is(true));
        assertThat(tree.canWrite(path("foo")), is(false));
        assertThat(tree.canWrite(path("/data1/food")), is(false));
        assertThat(tree.canRead(path("/data1/foo")), is(true));
        assertThat(tree.canRead(path("/data2/foo")), is(true));
        assertThat(tree.canRead(path("foo")), is(false));

        assertThat(tree.canWrite(path("/data1")), is(false));
        assertThat(tree.canWrite(path("/data2")), is(false));
        assertThat(tree.canWrite(path("/config/before")), is(false));
        assertThat(tree.canWrite(path("/config/later")), is(false));
    }

    public void testNormalizePath() {
        var tree = accessTree(entitlement("foo/../bar", "read"));
        assertThat(tree.canRead(path("foo/../bar")), is(true));
        assertThat(tree.canRead(path("foo/../bar/")), is(true));
        assertThat(tree.canRead(path("foo")), is(false));
        assertThat(tree.canRead(path("")), is(false));
    }

    public void testNormalizeDirectorySeparatorWindows() {
        assumeTrue("normalization of windows paths", WINDOWS.isCurrent());

        assertThat(FileAccessTree.normalizePath(Path.of("C:\\a\\b")), equalTo("C:\\a\\b"));
        assertThat(FileAccessTree.normalizePath(Path.of("C:/a.xml")), equalTo("C:\\a.xml"));
        assertThat(FileAccessTree.normalizePath(Path.of("C:/a/b.txt")), equalTo("C:\\a\\b.txt"));
        assertThat(FileAccessTree.normalizePath(Path.of("C:/a/c\\foo.txt")), equalTo("C:\\a\\c\\foo.txt"));

        var tree = accessTree(entitlement("C:\\a\\b", "read", "C:/a.xml", "read", "C:/a/b.txt", "read", "C:/a/c\\foo.txt", "read"));

        assertThat(tree.canRead(Path.of("C:/a.xml")), is(true));
        assertThat(tree.canRead(Path.of("C:\\a.xml")), is(true));
        assertThat(tree.canRead(Path.of("C:/a/")), is(false));
        assertThat(tree.canRead(Path.of("C:/a/b.txt")), is(true));
        assertThat(tree.canRead(Path.of("C:/a/b/c.txt")), is(true));
        assertThat(tree.canRead(Path.of("C:\\a\\b\\c.txt")), is(true));
        assertThat(tree.canRead(Path.of("C:\\a\\c\\")), is(false));
        assertThat(tree.canRead(Path.of("C:\\a\\c\\foo.txt")), is(true));
    }

    public void testNormalizeTrailingSlashes() {
        var tree = accessTree(entitlement("/trailing/slash/", "read", "/no/trailing/slash", "read"));
        assertThat(tree.canRead(path("/trailing/slash")), is(true));
        assertThat(tree.canRead(path("/trailing/slash/")), is(true));
        assertThat(tree.canRead(path("/trailing/slash.xml")), is(false));
        assertThat(tree.canRead(path("/trailing/slash/file.xml")), is(true));
        assertThat(tree.canRead(path("/no/trailing/slash")), is(true));
        assertThat(tree.canRead(path("/no/trailing/slash/")), is(true));
        assertThat(tree.canRead(path("/no/trailing/slash.xml")), is(false));
        assertThat(tree.canRead(path("/no/trailing/slash/file.xml")), is(true));
    }

    public void testForwardSlashes() {
        String sep = getDefaultFileSystem().getSeparator();
        var tree = accessTree(entitlement("a/b", "read", "m" + sep + "n", "read"));

        // Native separators work
        assertThat(tree.canRead(path("a" + sep + "b")), is(true));
        assertThat(tree.canRead(path("m" + sep + "n")), is(true));

        // Forward slashes also work
        assertThat(tree.canRead(path("a/b")), is(true));
        assertThat(tree.canRead(path("m/n")), is(true));
    }

    public void testJdkAccess() {
        Path jdkDir = Paths.get(System.getProperty("java.home"));
        var confDir = jdkDir.resolve("conf");
        var tree = accessTree(FilesEntitlement.EMPTY);

        assertThat(tree.canRead(confDir), is(true));
        assertThat(tree.canWrite(confDir), is(false));
        assertThat(tree.canRead(jdkDir), is(false));
    }

    @SuppressForbidden(reason = "don't care about the directory location in tests")
    public void testFollowLinks() throws IOException {
        assumeFalse("Windows requires admin right to create symbolic links", WINDOWS.isCurrent());

        Path baseSourceDir = Files.createTempDirectory("fileaccess_source");
        Path source1Dir = baseSourceDir.resolve("source1");
        Files.createDirectory(source1Dir);
        Path source2Dir = baseSourceDir.resolve("source2");
        Files.createDirectory(source2Dir);

        Path baseTargetDir = Files.createTempDirectory("fileaccess_target");
        Path readTarget = baseTargetDir.resolve("read_link");
        Path writeTarget = baseTargetDir.resolve("write_link");
        Files.createSymbolicLink(readTarget, source1Dir);
        Files.createSymbolicLink(writeTarget, source2Dir);
        var tree = accessTree(entitlement(readTarget.toString(), "read", writeTarget.toString(), "read_write"));

        assertThat(tree.canRead(baseSourceDir), is(false));
        assertThat(tree.canRead(baseTargetDir), is(false));

        assertThat(tree.canRead(readTarget), is(true));
        assertThat(tree.canWrite(readTarget), is(false));
        assertThat(tree.canRead(source1Dir), is(true));
        assertThat(tree.canWrite(source1Dir), is(false));

        assertThat(tree.canRead(writeTarget), is(true));
        assertThat(tree.canWrite(writeTarget), is(true));
        assertThat(tree.canRead(source2Dir), is(true));
        assertThat(tree.canWrite(source2Dir), is(true));
    }

    public void testTempDirAccess() {
        var tree = accessTree(FilesEntitlement.EMPTY);
        assertThat(tree.canRead(TEST_PATH_LOOKUP.getBaseDirPaths(TEMP).findFirst().get()), is(true));
        assertThat(tree.canWrite(TEST_PATH_LOOKUP.getBaseDirPaths(TEMP).findFirst().get()), is(true));
    }

    public void testConfigDirAccess() {
        var tree = accessTree(FilesEntitlement.EMPTY);
        assertThat(tree.canRead(configDirPath()), is(true));
        assertThat(tree.canWrite(configDirPath()), is(false));
    }

    public void testBasicExclusiveAccess() {
        var tree = accessTree(entitlement("foo", "read"), exclusivePaths("test-component", "test-module", "foo"), List.of());
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canWrite(path("foo")), is(false));
        tree = accessTree(entitlement("foo", "read_write"), exclusivePaths("test-component", "test-module", "foo"), List.of());
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canWrite(path("foo")), is(true));
        tree = accessTree(entitlement("foo", "read"), exclusivePaths("test-component", "diff-module", "foo/bar"), List.of());
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canWrite(path("foo")), is(false));
        assertThat(tree.canRead(path("foo/baz")), is(true));
        assertThat(tree.canWrite(path("foo/baz")), is(false));
        assertThat(tree.canRead(path("foo/bar")), is(false));
        assertThat(tree.canWrite(path("foo/bar")), is(false));
        tree = accessTree(
            entitlement("foo", "read", "foo.xml", "read", "foo/bar.xml", "read_write"),
            exclusivePaths("test-component", "diff-module", "foo/bar", "foo/baz", "other"),
            List.of()
        );
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canWrite(path("foo")), is(false));
        assertThat(tree.canRead(path("foo.xml")), is(true));
        assertThat(tree.canWrite(path("foo.xml")), is(false));
        assertThat(tree.canRead(path("foo/baz")), is(false));
        assertThat(tree.canWrite(path("foo/baz")), is(false));
        assertThat(tree.canRead(path("foo/bar")), is(false));
        assertThat(tree.canWrite(path("foo/bar")), is(false));
        assertThat(tree.canRead(path("foo/bar.xml")), is(true));
        assertThat(tree.canWrite(path("foo/bar.xml")), is(true));
        assertThat(tree.canRead(path("foo/bar.baz")), is(true));
        assertThat(tree.canWrite(path("foo/bar.baz")), is(false));
        assertThat(tree.canRead(path("foo/biz/bar.xml")), is(true));
        assertThat(tree.canWrite(path("foo/biz/bar.xml")), is(false));
    }

    public void testInvalidExclusiveAccess() {
        var tree = accessTree(entitlement("a", "read"), exclusivePaths("diff-component", "diff-module", "a/b"), List.of());
        assertThat(tree.canRead(path("a")), is(true));
        assertThat(tree.canWrite(path("a")), is(false));
        assertThat(tree.canRead(path("a/b")), is(false));
        assertThat(tree.canWrite(path("a/b")), is(false));
        assertThat(tree.canRead(path("a/b/c")), is(false));
        assertThat(tree.canWrite(path("a/b/c")), is(false));
        tree = accessTree(entitlement("a/b", "read"), exclusivePaths("diff-component", "diff-module", "a"), List.of());
        assertThat(tree.canRead(path("a")), is(false));
        assertThat(tree.canWrite(path("a")), is(false));
        assertThat(tree.canRead(path("a/b")), is(false));
        assertThat(tree.canWrite(path("a/b")), is(false));
        tree = accessTree(entitlement("a", "read"), exclusivePaths("diff-component", "diff-module", "a"), List.of());
        assertThat(tree.canRead(path("a")), is(false));
        assertThat(tree.canWrite(path("a")), is(false));
    }

    public void testForbiddenAccess() {
        var tree = accessTree(FilesEntitlement.EMPTY, List.of(), pathsInConfigDir("foo"));
        // Can always read from config
        assertThat(tree.canRead(configDirPath("bar")), is(true));
        // Unless forbidden
        assertThat(tree.canRead(configDirPath("foo")), is(false));
        assertThat(tree.canWrite(configDirPath("foo")), is(false));
        tree = accessTree(entitlement("foo", "read_write"), List.of(), paths("foo"));
        // Forbidden wins over entitlement
        assertThat(tree.canRead(path("foo")), is(false));
        assertThat(tree.canWrite(path("foo")), is(false));
        tree = accessTree(entitlement("foo", "read_write"), exclusivePaths("test-component", "test-module", "foo"), paths("foo"));
        // Even over exclusive paths
        assertThat(tree.canRead(path("foo")), is(false));
        assertThat(tree.canWrite(path("foo")), is(false));
        tree = accessTree(entitlement("foo", "read"), List.of(), paths("foo/bar"));
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canWrite(path("foo")), is(false));
        assertThat(tree.canRead(path("foo/baz")), is(true));
        assertThat(tree.canWrite(path("foo/baz")), is(false));
        // Forbidden sub-path
        assertThat(tree.canRead(path("foo/bar")), is(false));
        assertThat(tree.canWrite(path("foo/bar")), is(false));
        tree = accessTree(
            entitlement("foo", "read", "foo.xml", "read", "foo/bar.xml", "read_write"),
            List.of(),
            paths("test-component", "diff-module", "foo/bar", "foo/baz", "other")
        );
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canWrite(path("foo")), is(false));
        assertThat(tree.canRead(path("foo.xml")), is(true));
        assertThat(tree.canWrite(path("foo.xml")), is(false));
        assertThat(tree.canRead(path("foo/baz")), is(false));
        assertThat(tree.canWrite(path("foo/baz")), is(false));
        assertThat(tree.canRead(path("foo/bar")), is(false));
        assertThat(tree.canWrite(path("foo/bar")), is(false));
        assertThat(tree.canRead(path("foo/bar.xml")), is(true));
        assertThat(tree.canWrite(path("foo/bar.xml")), is(true));
        assertThat(tree.canRead(path("foo/bar.baz")), is(true));
        assertThat(tree.canWrite(path("foo/bar.baz")), is(false));
        assertThat(tree.canRead(path("foo/biz/bar.xml")), is(true));
        assertThat(tree.canWrite(path("foo/biz/bar.xml")), is(false));
    }

    public void testDuplicatePrunedPaths() {
        var comparison = new CaseSensitiveComparison(separatorChar());
        List<String> inputPaths = List.of("/a", "/a", "/a/b", "/a/b", "/b/c", "b/c/d", "b/c/d", "b/c/d", "e/f", "e/f");
        List<String> outputPaths = List.of("/a", "/b/c", "b/c/d", "e/f");
        var actual = FileAccessTree.pruneSortedPaths(inputPaths.stream().map(p -> normalizePath(path(p))).toList(), comparison);
        var expected = outputPaths.stream().map(p -> normalizePath(path(p))).toList();
        assertEquals(expected, actual);
    }

    public void testDuplicatePrunedPathsWindows() {
        var comparison = new CaseInsensitiveComparison(separatorChar());
        List<String> inputPaths = List.of("/a", "/A", "/a/b", "/a/B", "/b/c", "b/c/d", "B/c/d", "b/c/D", "e/f", "e/f");
        List<String> outputPaths = List.of("/a", "/b/c", "b/c/d", "e/f");
        var actual = FileAccessTree.pruneSortedPaths(inputPaths.stream().map(p -> normalizePath(path(p))).toList(), comparison);
        var expected = outputPaths.stream().map(p -> normalizePath(path(p))).toList();
        assertEquals(expected, actual);
    }

    public void testDuplicateExclusivePaths() {
        // Bunch o' handy definitions
        var pathAB = path("/a/b");
        var pathCD = path("/c/d");
        var comparison = randomBoolean() ? new CaseSensitiveComparison('/') : new CaseInsensitiveComparison('/');
        var originalFileData = FileData.ofPath(pathAB, READ).withExclusive(true);
        var fileDataWithWriteMode = FileData.ofPath(pathAB, READ_WRITE).withExclusive(true);
        var original = new ExclusiveFileEntitlement("component1", "module1", new FilesEntitlement(List.of(originalFileData)));
        var differentComponent = new ExclusiveFileEntitlement("component2", original.moduleName(), original.filesEntitlement());
        var differentModule = new ExclusiveFileEntitlement(original.componentName(), "module2", original.filesEntitlement());
        var differentPath = new ExclusiveFileEntitlement(
            original.componentName(),
            original.moduleName(),
            new FilesEntitlement(List.of(FileData.ofPath(pathCD, originalFileData.mode()).withExclusive(originalFileData.exclusive())))
        );
        var differentMode = new ExclusiveFileEntitlement(
            original.componentName(),
            original.moduleName(),
            new FilesEntitlement(List.of(fileDataWithWriteMode))
        );
        var differentPlatform = new ExclusiveFileEntitlement(
            original.componentName(),
            original.moduleName(),
            new FilesEntitlement(List.of(originalFileData.withPlatform(WINDOWS)))
        );
        var originalExclusivePath = new ExclusivePath("component1", Set.of("module1"), normalizePath(pathAB));

        // Some basic tests

        assertEquals(
            "Single element should trivially work",
            List.of(originalExclusivePath),
            buildExclusivePathList(List.of(original), TEST_PATH_LOOKUP, comparison)
        );
        assertEquals(
            "Two identical elements should be combined",
            List.of(originalExclusivePath),
            buildExclusivePathList(List.of(original, original), TEST_PATH_LOOKUP, comparison)
        );

        // Don't merge things we shouldn't

        var distinctEntitlements = List.of(original, differentComponent, differentModule, differentPath);
        var iae = expectThrows(
            IllegalArgumentException.class,
            () -> buildExclusivePathList(distinctEntitlements, TEST_PATH_LOOKUP, comparison)
        );
        var pathABString = pathAB.toAbsolutePath().toString();
        assertThat(
            iae.getMessage(),
            equalTo(
                "Path ["
                    + pathABString
                    + "] is already exclusive to [component1][module1], cannot add exclusive access for [component2][module1]"
            )
        );

        var equivalentEntitlements = List.of(original, differentMode, differentPlatform);
        var equivalentPaths = List.of(originalExclusivePath);
        assertEquals(
            "Exclusive paths should be combined even if the entitlements are different",
            equivalentPaths,
            buildExclusivePathList(equivalentEntitlements, TEST_PATH_LOOKUP, comparison)
        );
    }

    public void testWindowsAbsolutPathAccess() {
        assumeTrue("Specific to windows for paths with a root (DOS or UNC)", WINDOWS.isCurrent());

        var fileAccessTree = FileAccessTree.of(
            "test",
            "test",
            new FilesEntitlement(
                List.of(
                    FileData.ofPath(Path.of("\\\\.\\pipe\\"), READ),
                    FileData.ofPath(Path.of("D:\\.gradle"), READ),
                    FileData.ofPath(Path.of("D:\\foo"), READ),
                    FileData.ofPath(Path.of("C:\\foo"), FilesEntitlement.Mode.READ_WRITE)
                )
            ),
            TEST_PATH_LOOKUP,
            List.of(),
            List.of(),
            List.of()
        );

        assertThat(fileAccessTree.canRead(Path.of("\\\\.\\pipe\\bar")), is(true));
        assertThat(fileAccessTree.canRead(Path.of("C:\\foo")), is(true));
        assertThat(fileAccessTree.canWrite(Path.of("C:\\foo")), is(true));
        assertThat(fileAccessTree.canRead(Path.of("D:\\foo")), is(true));
        assertThat(fileAccessTree.canWrite(Path.of("D:\\foo")), is(false));
    }

    public void testWindowsMixedCaseAccess() {
        assumeTrue("Specific to windows for paths with mixed casing", WINDOWS.isCurrent());

        var fileAccessTree = FileAccessTree.of(
            "test",
            "test",
            new FilesEntitlement(
                List.of(
                    FileData.ofPath(Path.of("\\\\.\\pipe\\"), READ),
                    FileData.ofPath(Path.of("D:\\.gradle"), READ),
                    FileData.ofPath(Path.of("D:\\foo"), READ),
                    FileData.ofPath(Path.of("C:\\foo"), FilesEntitlement.Mode.READ_WRITE)
                )
            ),
            TEST_PATH_LOOKUP,
            List.of(),
            List.of(),
            List.of()
        );

        assertThat(fileAccessTree.canRead(Path.of("\\\\.\\PIPE\\bar")), is(true));
        assertThat(fileAccessTree.canRead(Path.of("c:\\foo")), is(true));
        assertThat(fileAccessTree.canRead(Path.of("C:\\FOO")), is(true));
        assertThat(fileAccessTree.canWrite(Path.of("C:\\foo")), is(true));
        assertThat(fileAccessTree.canRead(Path.of("c:\\foo")), is(true));
        assertThat(fileAccessTree.canRead(Path.of("C:\\FOO")), is(true));
        assertThat(fileAccessTree.canRead(Path.of("d:\\foo")), is(true));
        assertThat(fileAccessTree.canRead(Path.of("d:\\FOO")), is(true));
        assertThat(fileAccessTree.canWrite(Path.of("D:\\foo")), is(false));
        assertThat(fileAccessTree.canWrite(Path.of("d:\\foo")), is(false));
    }

    FileAccessTree accessTree(FilesEntitlement entitlement) {
        return FileAccessTree.of("test-component", "test-module", entitlement, TEST_PATH_LOOKUP, List.of(), List.of(), List.of());
    }

    FileAccessTree accessTree(FilesEntitlement entitlement, List<ExclusivePath> exclusivePaths, Collection<String> forbiddenPaths) {
        return FileAccessTree.of("test-component", "test-module", entitlement, TEST_PATH_LOOKUP, List.of(), exclusivePaths, forbiddenPaths);
    }

    static FilesEntitlement entitlement(String... values) {
        List<Object> filesData = new ArrayList<>();
        for (int i = 0; i < values.length; i += 2) {
            Map<String, String> fileData = new HashMap<>();
            fileData.put("path", path(values[i]).toString());
            fileData.put("mode", values[i + 1]);
            filesData.add(fileData);
        }
        return FilesEntitlement.build(filesData);
    }

    static FilesEntitlement entitlement(Map<String, String> value) {
        return FilesEntitlement.build(List.of(value));
    }

    static Path path(String s) {
        return root.resolve(s);
    }

    static List<String> paths(String... paths) {
        return Arrays.stream(paths).map(x -> FileAccessTree.normalizePath(path(x))).toList();
    }

    static List<ExclusivePath> exclusivePaths(String componentName, String moduleName, String... paths) {
        List<ExclusivePath> exclusivePaths = new ArrayList<>();
        for (String path : paths) {
            exclusivePaths.add(new ExclusivePath(componentName, Set.of(moduleName), normalizePath(path(path))));
        }
        return exclusivePaths;
    }

    static Set<String> pathsInConfigDir(String... files) {
        return TEST_PATH_LOOKUP.getBaseDirPaths(PathLookup.BaseDir.CONFIG)
            .flatMap(baseDir -> Arrays.stream(files).map(baseDir::resolve))
            .map(FileAccessTree::normalizePath)
            .collect(Collectors.toSet());
    }

    static Path configDirPath() {
        return TEST_PATH_LOOKUP.getBaseDirPaths(CONFIG).findFirst().orElseThrow();
    }

    static Path configDirPath(String file) {
        return configDirPath().resolve(file);
    }
}
