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

    public void testEmpty() {
        var tree = FileAccessTree.of(FilesEntitlement.EMPTY);
        assertThat(tree.canRead(path("path")), is(false));
        assertThat(tree.canWrite(path("path")), is(false));
    }

    public void testRead() {
        var tree = FileAccessTree.of(entitlement("foo", "read"));
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canRead(path("foo/subdir")), is(true));
        assertThat(tree.canRead(path("food")), is(false));
        assertThat(tree.canWrite(path("foo")), is(false));
        assertThat(tree.canWrite(path("food")), is(false));

        assertThat(tree.canRead(path("before")), is(false));
        assertThat(tree.canRead(path("later")), is(false));
    }

    public void testWrite() {
        var tree = FileAccessTree.of(entitlement("foo", "read_write"));
        assertThat(tree.canWrite(path("foo")), is(true));
        assertThat(tree.canWrite(path("foo/subdir")), is(true));
        assertThat(tree.canWrite(path("food")), is(false));
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canRead(path("food")), is(false));

        assertThat(tree.canWrite(path("before")), is(false));
        assertThat(tree.canWrite(path("later")), is(false));
    }

    public void testTwoPaths() {
        var tree = FileAccessTree.of(entitlement("foo", "read", "bar", "read"));
        assertThat(tree.canRead(path("a")), is(false));
        assertThat(tree.canRead(path("bar")), is(true));
        assertThat(tree.canRead(path("bar/subdir")), is(true));
        assertThat(tree.canRead(path("c")), is(false));
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canRead(path("foo/subdir")), is(true));
        assertThat(tree.canRead(path("z")), is(false));
    }

    public void testReadWriteUnderRead() {
        var tree = FileAccessTree.of(entitlement("foo", "read", "foo/bar", "read_write"));
        assertThat(tree.canRead(path("foo")), is(true));
        assertThat(tree.canWrite(path("foo")), is(false));
        assertThat(tree.canRead(path("foo/bar")), is(true));
        assertThat(tree.canWrite(path("foo/bar")), is(true));
    }

    public void testNormalizePath() {
        var tree = FileAccessTree.of(entitlement("foo/../bar", "read"));
        assertThat(tree.canRead(path("foo/../bar")), is(true));
        assertThat(tree.canRead(path("foo")), is(false));
        assertThat(tree.canRead(path("")), is(false));
    }

    public void testForwardSlashes() {
        String sep = getDefaultFileSystem().getSeparator();
        var tree = FileAccessTree.of(entitlement("a/b", "read", "m" + sep + "n", "read"));

        // Native separators work
        assertThat(tree.canRead(path("a" + sep + "b")), is(true));
        assertThat(tree.canRead(path("m" + sep + "n")), is(true));

        // Forward slashes also work
        assertThat(tree.canRead(path("a/b")), is(true));
        assertThat(tree.canRead(path("m/n")), is(true));

        // In case the native separator is a backslash, don't treat that as an escape
        assertThat(tree.canRead(path("m\n")), is(false));
    }

    FilesEntitlement entitlement(String... values) {
        List<Object> filesData = new ArrayList<>();
        for (int i = 0; i < values.length; i += 2) {
            Map<String, String> fileData = new HashMap<>();
            fileData.put("path", path(values[i]).toString());
            fileData.put("mode", values[i + 1]);
            filesData.add(fileData);
        }
        return FilesEntitlement.build(filesData);
    }
}
