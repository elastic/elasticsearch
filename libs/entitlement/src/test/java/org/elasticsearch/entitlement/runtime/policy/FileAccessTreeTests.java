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

    @BeforeClass
    public static void setupRoot() {
        root = createTempDir();
    }

    private static Path path(String s) {
        return root.resolve(s);
    }

    private static final PathLookup TEST_PATH_LOOKUP = new PathLookup(
        Path.of("/home"),
        Path.of("/config"),
        new Path[] { Path.of("/data1"), Path.of("/data2") },
        Path.of("/tmp")
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
    }

    public void testReadWithRelativePath() {
        for (var dir : List.of("config", "home")) {
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
        for (var dir : List.of("config", "home")) {
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
        assertThat(tree.canRead(path("foo")), is(false));
        assertThat(tree.canRead(path("")), is(false));
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

    public void testTempDirAccess() {
        Path tempDir = createTempDir();
        var tree = FileAccessTree.of(
            FilesEntitlement.EMPTY,
            new PathLookup(Path.of("/home"), Path.of("/config"), new Path[] { Path.of("/data1"), Path.of("/data2") }, tempDir)
        );
        assertThat(tree.canRead(tempDir), is(true));
        assertThat(tree.canWrite(tempDir), is(true));
    }

    FileAccessTree accessTree(FilesEntitlement entitlement) {
        return FileAccessTree.of(entitlement, TEST_PATH_LOOKUP);
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
}
