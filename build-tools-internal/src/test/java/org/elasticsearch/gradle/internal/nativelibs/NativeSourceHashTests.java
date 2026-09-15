/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class NativeSourceHashTests {

    private static final String TOOLCHAIN = "toolchain:1";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    /**
     * The hash addresses the published artifact, so two checkouts of the same sources must agree on it
     * no matter what order their file trees were walked in.
     */
    @Test
    public void testIndependentOfFileOrder() throws IOException {
        File root = temporaryFolder.newFolder("checkout");
        File first = write(root, "a.cpp", "int a();");
        File second = write(root, "b.cpp", "int b();");

        String forward = NativeSourceHash.compute(root, ordered(first, second), TOOLCHAIN);
        String reversed = NativeSourceHash.compute(root, ordered(second, first), TOOLCHAIN);

        assertEquals(forward, reversed);
    }

    /**
     * A checkout with CRLF line endings compiles to the same binary, so it must resolve the same
     * artifact rather than compiling and publishing a second copy.
     */
    @Test
    public void testIndependentOfLineEndings() throws IOException {
        File unix = temporaryFolder.newFolder("unix");
        File windows = temporaryFolder.newFolder("windows");

        String hashLf = NativeSourceHash.compute(unix, ordered(write(unix, "a.cpp", "int a();\nint b();\n")), TOOLCHAIN);
        String hashCrLf = NativeSourceHash.compute(windows, ordered(write(windows, "a.cpp", "int a();\r\nint b();\r\n")), TOOLCHAIN);

        assertEquals(hashLf, hashCrLf);
    }

    /**
     * Only the CRLF pair is normalised. A lone carriage return is content — {@code '\r'} and
     * {@code ''} are different programs — so it must change the digest.
     */
    @Test
    public void testDistinguishesALoneCarriageReturn() throws IOException {
        File with = temporaryFolder.newFolder("with");
        File without = temporaryFolder.newFolder("without");

        assertNotEquals(
            NativeSourceHash.compute(with, ordered(write(with, "a.cpp", "char c = '\r';")), TOOLCHAIN),
            NativeSourceHash.compute(without, ordered(write(without, "a.cpp", "char c = '';")), TOOLCHAIN)
        );
    }

    /** The digest must not depend on where the repository happens to be checked out. */
    @Test
    public void testIndependentOfCheckoutLocation() throws IOException {
        File here = temporaryFolder.newFolder("here");
        File elsewhere = temporaryFolder.newFolder("some", "deeper", "elsewhere");

        String first = NativeSourceHash.compute(here, ordered(write(here, "a.cpp", "int a();")), TOOLCHAIN);
        String second = NativeSourceHash.compute(elsewhere, ordered(write(elsewhere, "a.cpp", "int a();")), TOOLCHAIN);

        assertEquals(first, second);
    }

    @Test
    public void testChangesWithSourceContent() throws IOException {
        File before = temporaryFolder.newFolder("before");
        File after = temporaryFolder.newFolder("after");

        assertNotEquals(
            NativeSourceHash.compute(before, ordered(write(before, "a.cpp", "int a();")), TOOLCHAIN),
            NativeSourceHash.compute(after, ordered(write(after, "a.cpp", "int a(); // changed")), TOOLCHAIN)
        );
    }

    /**
     * Without this a compiler upgrade would produce different binaries under a version that is already
     * published, and every later build would keep resolving the stale one.
     */
    @Test
    public void testChangesWithToolchain() throws IOException {
        File root = temporaryFolder.newFolder("checkout");
        Set<File> sources = ordered(write(root, "a.cpp", "int a();"));

        assertNotEquals(
            NativeSourceHash.compute(root, sources, "toolchain:1"),
            NativeSourceHash.compute(root, sources, "toolchain:2")
        );
    }

    @Test
    public void testChangesWhenASourceIsAdded() throws IOException {
        File root = temporaryFolder.newFolder("checkout");
        File first = write(root, "a.cpp", "int a();");
        String single = NativeSourceHash.compute(root, ordered(first), TOOLCHAIN);

        File second = write(root, "b.cpp", "int b();");
        String both = NativeSourceHash.compute(root, ordered(first, second), TOOLCHAIN);

        assertNotEquals(single, both);
    }

    /** Renaming a file changes what the build compiles, so it must change the digest. */
    @Test
    public void testChangesWhenASourceIsRenamed() throws IOException {
        File before = temporaryFolder.newFolder("before");
        File after = temporaryFolder.newFolder("after");

        assertNotEquals(
            NativeSourceHash.compute(before, ordered(write(before, "a.cpp", "int a();")), TOOLCHAIN),
            NativeSourceHash.compute(after, ordered(write(after, "renamed.cpp", "int a();")), TOOLCHAIN)
        );
    }

    /**
     * Digesting contents alone gives {@code ["ab", "c"]} and {@code ["a", "bc"]} the same result, so
     * moving a declaration between files would silently resolve the previous artifact.
     */
    @Test
    public void testDistinguishesContentMovedBetweenFiles() throws IOException {
        File split = temporaryFolder.newFolder("split");
        File moved = temporaryFolder.newFolder("moved");

        assertNotEquals(
            NativeSourceHash.compute(split, ordered(write(split, "a.cpp", "ab"), write(split, "b.cpp", "c")), TOOLCHAIN),
            NativeSourceHash.compute(moved, ordered(write(moved, "a.cpp", "a"), write(moved, "b.cpp", "bc")), TOOLCHAIN)
        );
    }

    private static File write(File root, String name, String content) throws IOException {
        File file = new File(root, name);
        Files.createDirectories(file.toPath().getParent());
        Files.writeString(file.toPath(), content);
        return file;
    }

    /** A set that preserves insertion order, so a test can control what order the hash is handed. */
    private static Set<File> ordered(File... files) {
        return new LinkedHashSet<>(List.of(files));
    }
}
