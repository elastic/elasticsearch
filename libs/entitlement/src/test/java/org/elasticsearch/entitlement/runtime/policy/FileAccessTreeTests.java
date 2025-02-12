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
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.core.PathUtils.getDefaultFileSystem;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class FileAccessTreeTests extends ESTestCase {

    static Path root;

    @BeforeClass
    public static void setupRoot() {
        root = createTempDir();
    }

    private static Path path(String s) {
        return root.resolve(s);
    }

    public void testEmpty() {
        var tree = FileAccessTree.of(FilesEntitlement.EMPTY, Mockito.mock(DirectoryResolver.class));
        assertThat(tree.canRead(path("path")), is(false));
        assertThat(tree.canWrite(path("path")), is(false));
    }

    public void testRead() {
        var tree = FileAccessTree.of(entitlement("foo", "read"), Mockito.mock(DirectoryResolver.class));
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canRead(path("foo/subdir")), is(true));
        assertThat(tree.canRead(path("food")), is(false));
        assertThat(tree.canWrite(path("foo")), is(false));
        assertThat(tree.canWrite(path("food")), is(false));

        assertThat(tree.canRead(path("before")), is(false));
        assertThat(tree.canRead(path("later")), is(false));
    }

    public void testWrite() {
        var tree = FileAccessTree.of(entitlement("foo", "read_write"), Mockito.mock(DirectoryResolver.class));
        assertThat(tree.canWrite(path("foo")), is(true));
        assertThat(tree.canWrite(path("foo/subdir")), is(true));
        assertThat(tree.canWrite(path("food")), is(false));
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canRead(path("food")), is(false));

        assertThat(tree.canWrite(path("before")), is(false));
        assertThat(tree.canWrite(path("later")), is(false));
    }

    public void testTwoPaths() {
        var tree = FileAccessTree.of(entitlement("foo", "read", "bar", "read"), Mockito.mock(DirectoryResolver.class));
        assertThat(tree.canRead(path("a")), is(false));
        assertThat(tree.canRead(path("bar")), is(true));
        assertThat(tree.canRead(path("bar/subdir")), is(true));
        assertThat(tree.canRead(path("c")), is(false));
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canRead(path("foo/subdir")), is(true));
        assertThat(tree.canRead(path("z")), is(false));
    }

    public void testReadWriteUnderRead() {
        var tree = FileAccessTree.of(entitlement("foo", "read", "foo/bar", "read_write"), Mockito.mock(DirectoryResolver.class));
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canWrite(path("foo")), is(false));
        assertThat(tree.canRead(path("foo/bar")), is(true));
        assertThat(tree.canWrite(path("foo/bar")), is(true));
    }

    public void testReadWithBaseDir() {
        var resolver = Mockito.mock(DirectoryResolver.class);
        when(resolver.resolveTemp(any(Path.class))).thenReturn(Path.of("/tmp/foo"));
        var tree = FileAccessTree.of(entitlement(Map.of("path", "foo", "mode", "read", "base_dir", "temp")), resolver);
        assertThat(tree.canRead(path("foo")), is(false));

        assertThat(tree.canRead(path("/tmp/foo")), is(true));

        assertThat(tree.canRead(path("/tmp/foo/subdir")), is(true));
        assertThat(tree.canRead(path("/tmp/food")), is(false));
        assertThat(tree.canWrite(path("/tmp/foo")), is(false));

        assertThat(tree.canRead(path("/tmp")), is(false));
        assertThat(tree.canRead(path("/tmp/before")), is(false));
        assertThat(tree.canRead(path("/tmp/later")), is(false));
    }

    public void testWriteWithBaseDir() {
        var resolver = Mockito.mock(DirectoryResolver.class);
        when(resolver.resolveConfig(any(Path.class))).thenReturn(Path.of("/config/foo"));
        var tree = FileAccessTree.of(entitlement(Map.of("path", "foo", "mode", "read_write", "base_dir", "config")), resolver);
        assertThat(tree.canWrite(path("/config/foo")), is(true));
        assertThat(tree.canWrite(path("/config/foo/subdir")), is(true));
        assertThat(tree.canWrite(path("foo")), is(false));
        assertThat(tree.canWrite(path("/config/food")), is(false));
        assertThat(tree.canRead(path("/config/foo")), is(true));
        assertThat(tree.canRead(path("foo")), is(false));

        assertThat(tree.canWrite(path("/config")), is(false));
        assertThat(tree.canWrite(path("/config/before")), is(false));
        assertThat(tree.canWrite(path("/config/later")), is(false));
    }

    public void testMultipleDataBaseDir() {
        var resolver = Mockito.mock(DirectoryResolver.class);
        when(resolver.resolveData(any(Path.class))).thenReturn(Stream.of(Path.of("/data1/foo"), Path.of("/data2/foo")));
        var tree = FileAccessTree.of(entitlement(Map.of("path", "foo", "mode", "read_write", "base_dir", "data")), resolver);
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
        var tree = FileAccessTree.of(entitlement("foo/../bar", "read"), Mockito.mock(DirectoryResolver.class));
        assertThat(tree.canRead(path("foo/../bar")), is(true));
        assertThat(tree.canRead(path("foo")), is(false));
        assertThat(tree.canRead(path("")), is(false));
    }

    public void testForwardSlashes() {
        String sep = getDefaultFileSystem().getSeparator();
        var tree = FileAccessTree.of(entitlement("a/b", "read", "m" + sep + "n", "read"), Mockito.mock(DirectoryResolver.class));

        // Native separators work
        assertThat(tree.canRead(path("a" + sep + "b")), is(true));
        assertThat(tree.canRead(path("m" + sep + "n")), is(true));

        // Forward slashes also work
        assertThat(tree.canRead(path("a/b")), is(true));
        assertThat(tree.canRead(path("m/n")), is(true));
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
