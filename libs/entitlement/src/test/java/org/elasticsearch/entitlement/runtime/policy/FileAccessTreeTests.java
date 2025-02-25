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
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.core.PathUtils.getDefaultFileSystem;
import static org.hamcrest.Matchers.is;

@ESTestCase.WithoutSecurityManager
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
        assertThat(tree.canRead(path("foo/../bar/")), is(true));
        assertThat(tree.canRead(path("foo")), is(false));
        assertThat(tree.canRead(path("")), is(false));
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
        var tree = FileAccessTree.of(FilesEntitlement.EMPTY, TEST_PATH_LOOKUP);
        assertThat(tree.canRead(TEST_PATH_LOOKUP.tempDir()), is(true));
        assertThat(tree.canWrite(TEST_PATH_LOOKUP.tempDir()), is(true));
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
