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
import org.elasticsearch.entitlement.runtime.policy.FileAccessTree.ExclusivePath;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.core.PathUtils.getDefaultFileSystem;
import static org.hamcrest.Matchers.is;

public class FileAccessTreeTests extends ESTestCase {

    static Path root;
    static Settings settings;

    @BeforeClass
    public static void setupRoot() {
        root = createTempDir();
        settings = Settings.EMPTY;
    }

    private static Path path(String s) {
        return root.resolve(s);
    }

    private static final PathLookup TEST_PATH_LOOKUP = new PathLookup(
        Path.of("/home"),
        Path.of("/config"),
        new Path[] { Path.of("/data1"), Path.of("/data2") },
        new Path[] { Path.of("/shared1"), Path.of("/shared2") },
        Path.of("/tmp"),
        setting -> settings.get(setting),
        glob -> settings.getGlobValues(glob)
    );

    public void testEmpty() {
        var tree = accessTree(FilesEntitlement.EMPTY, List.of());
        assertThat(tree.canRead(path("path")), is(false));
        assertThat(tree.canWrite(path("path")), is(false));
    }

    public void testRead() {
        var tree = accessTree(entitlement("foo", "read"), List.of());
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canRead(path("foo/subdir")), is(true));
        assertThat(tree.canRead(path("food")), is(false));
        assertThat(tree.canWrite(path("foo")), is(false));
        assertThat(tree.canWrite(path("food")), is(false));

        assertThat(tree.canRead(path("before")), is(false));
        assertThat(tree.canRead(path("later")), is(false));
    }

    public void testWrite() {
        var tree = accessTree(entitlement("foo", "read_write"), List.of());
        assertThat(tree.canWrite(path("foo")), is(true));
        assertThat(tree.canWrite(path("foo/subdir")), is(true));
        assertThat(tree.canWrite(path("food")), is(false));
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canRead(path("food")), is(false));

        assertThat(tree.canWrite(path("before")), is(false));
        assertThat(tree.canWrite(path("later")), is(false));
    }

    public void testTwoPaths() {
        var tree = accessTree(entitlement("foo", "read", "bar", "read"), List.of());
        assertThat(tree.canRead(path("a")), is(false));
        assertThat(tree.canRead(path("bar")), is(true));
        assertThat(tree.canRead(path("bar/subdir")), is(true));
        assertThat(tree.canRead(path("c")), is(false));
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canRead(path("foo/subdir")), is(true));
        assertThat(tree.canRead(path("z")), is(false));
    }

    public void testReadWriteUnderRead() {
        var tree = accessTree(entitlement("foo", "read", "foo/bar", "read_write"), List.of());
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canWrite(path("foo")), is(false));
        assertThat(tree.canRead(path("foo/bar")), is(true));
        assertThat(tree.canWrite(path("foo/bar")), is(true));
        assertThat(tree.canRead(path("foo/baz")), is(true));
        assertThat(tree.canWrite(path("foo/baz")), is(false));
    }

    public void testPrunedPaths() {
        var tree = accessTree(entitlement("foo", "read", "foo/baz", "read", "foo/bar", "read"), List.of());
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canWrite(path("foo")), is(false));
        assertThat(tree.canRead(path("foo/bar")), is(true));
        assertThat(tree.canWrite(path("foo/bar")), is(false));
        assertThat(tree.canRead(path("foo/baz")), is(true));
        assertThat(tree.canWrite(path("foo/baz")), is(false));
        // also test a non-existent subpath
        assertThat(tree.canRead(path("foo/barf")), is(true));
        assertThat(tree.canWrite(path("foo/barf")), is(false));

        tree = accessTree(entitlement("foo", "read", "foo/bar", "read_write"), List.of());
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canWrite(path("foo")), is(false));
        assertThat(tree.canRead(path("foo/bar")), is(true));
        assertThat(tree.canWrite(path("foo/bar")), is(true));
        assertThat(tree.canRead(path("foo/baz")), is(true));
        assertThat(tree.canWrite(path("foo/baz")), is(false));
    }

    public void testPathAndFileWithSamePrefix() {
        var tree = accessTree(entitlement("foo/bar/", "read", "foo/bar.xml", "read"), List.of());
        assertThat(tree.canRead(path("foo")), is(false));
        assertThat(tree.canRead(path("foo/bar")), is(true));
        assertThat(tree.canRead(path("foo/bar/baz")), is(true));
        assertThat(tree.canRead(path("foo/bar.xml")), is(true));
        assertThat(tree.canRead(path("foo/bar.txt")), is(false));
    }

    public void testReadWithRelativePath() {
        for (var dir : List.of("config", "home")) {
            var tree = accessTree(entitlement(Map.of("relative_path", "foo", "mode", "read", "relative_to", dir)), List.of());
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
        for (var dir : List.of("config", "home")) {
            var tree = accessTree(entitlement(Map.of("relative_path", "foo", "mode", "read_write", "relative_to", dir)), List.of());
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
        var tree = accessTree(entitlement(Map.of("relative_path", "foo", "mode", "read_write", "relative_to", "data")), List.of());
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
        var tree = accessTree(entitlement("foo/../bar", "read"), List.of());
        assertThat(tree.canRead(path("foo/../bar")), is(true));
        assertThat(tree.canRead(path("foo/../bar/")), is(true));
        assertThat(tree.canRead(path("foo")), is(false));
        assertThat(tree.canRead(path("")), is(false));
    }

    public void testNormalizeTrailingSlashes() {
        var tree = accessTree(entitlement("/trailing/slash/", "read", "/no/trailing/slash", "read"), List.of());
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
        var tree = accessTree(entitlement("a/b", "read", "m" + sep + "n", "read"), List.of());

        // Native separators work
        assertThat(tree.canRead(path("a" + sep + "b")), is(true));
        assertThat(tree.canRead(path("m" + sep + "n")), is(true));

        // Forward slashes also work
        assertThat(tree.canRead(path("a/b")), is(true));
        assertThat(tree.canRead(path("m/n")), is(true));
    }

    public void testTempDirAccess() {
        var tree = FileAccessTree.of("test-component", "test-module", FilesEntitlement.EMPTY, TEST_PATH_LOOKUP, List.of());
        assertThat(tree.canRead(TEST_PATH_LOOKUP.tempDir()), is(true));
        assertThat(tree.canWrite(TEST_PATH_LOOKUP.tempDir()), is(true));
    }

    public void testBasicExclusiveAccess() {
        var tree = accessTree(entitlement("foo", "read"), exclusivePaths("test-component", "test-module", "foo"));
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canWrite(path("foo")), is(false));
        tree = accessTree(entitlement("foo", "read_write"), exclusivePaths("test-component", "test-module", "foo"));
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canWrite(path("foo")), is(true));
        tree = accessTree(entitlement("foo", "read"), exclusivePaths("test-component", "diff-module", "foo/bar"));
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canWrite(path("foo")), is(false));
        assertThat(tree.canRead(path("foo/baz")), is(true));
        assertThat(tree.canWrite(path("foo/baz")), is(false));
        assertThat(tree.canRead(path("foo/bar")), is(false));
        assertThat(tree.canWrite(path("foo/bar")), is(false));
        tree = accessTree(
            entitlement("foo", "read", "foo.xml", "read", "foo/bar.xml", "read_write"),
            exclusivePaths("test-component", "diff-module", "foo/bar", "foo/baz", "other")
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
        var iae = assertThrows(
            IllegalArgumentException.class,
            () -> accessTree(entitlement("foo/bar", "read"), exclusivePaths("test-component", "diff-module", "foo"))
        );
        assertThat(iae.getMessage(), is("[test-component] [test-module] cannot use exclusive path [" + path("foo") + "]"));
    }

    FileAccessTree accessTree(FilesEntitlement entitlement, List<ExclusivePath> exclusivePaths) {
        return FileAccessTree.of("test-component", "test-module", entitlement, TEST_PATH_LOOKUP, exclusivePaths);
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

    static List<ExclusivePath> exclusivePaths(String componentName, String moduleName, String... paths) {
        List<ExclusivePath> exclusivePaths = new ArrayList<>();
        for (String path : paths) {
            exclusivePaths.add(new ExclusivePath(componentName, moduleName, path(path).toString()));
        }
        return exclusivePaths;
    }
}
