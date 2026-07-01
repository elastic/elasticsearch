/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;

public class LocalFileAccessTests extends ESTestCase {

    // --- Default (disabled) ---

    public void testDefaultDisabledRejectsFileUri() {
        LocalFileAccess access = LocalFileAccess.create(Settings.EMPTY);
        assertFalse("empty allowlist must be disabled", access.enabled());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> access.check(StoragePath.of("file:///etc/passwd")));
        assertThat(e.getMessage(), containsString("esql.datasource.local_allowed_paths"));
    }

    public void testDefaultDisabledRejectsFileUriStringOverload() {
        LocalFileAccess access = LocalFileAccess.create(Settings.EMPTY);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> access.check("file:///etc/passwd"));
        assertThat(e.getMessage(), containsString("esql.datasource.local_allowed_paths"));
    }

    public void testDefaultDisabledAllowsNonFileUri() {
        LocalFileAccess access = LocalFileAccess.create(Settings.EMPTY);
        // Non-file schemes must pass through without any error
        access.check(StoragePath.of("s3://my-bucket/data.parquet"));
        access.check("https://example.com/data.csv");
    }

    public void testNullLocationIsNoop() {
        LocalFileAccess access = LocalFileAccess.create(Settings.EMPTY);
        // null must not throw
        access.check((String) null);
    }

    // --- Allowlist enabled ---

    public void testPathUnderAllowedRootSucceeds() throws IOException {
        Path tmpDir = createTempDir();
        Settings settings = Settings.builder().putList("esql.datasource.local_allowed_paths", tmpDir.toString()).build();
        LocalFileAccess access = LocalFileAccess.create(settings);
        assertTrue(access.enabled());

        Path target = tmpDir.resolve("data.csv");
        Files.createFile(target);
        // Should not throw
        access.check(StoragePath.of("file://" + target.toAbsolutePath()));
    }

    public void testPathOutsideAllowedRootRejected() throws IOException {
        Path allowed = createTempDir();
        Path outside = createTempDir();

        Settings settings = Settings.builder().putList("esql.datasource.local_allowed_paths", allowed.toString()).build();
        LocalFileAccess access = LocalFileAccess.create(settings);

        Path outsideFile = outside.resolve("secret.csv");
        Files.createFile(outsideFile);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> access.check(StoragePath.of("file://" + outsideFile.toAbsolutePath()))
        );
        assertThat(e.getMessage(), containsString("esql.datasource.local_allowed_paths"));
        assertThat(e.getMessage(), containsString(outsideFile.toAbsolutePath().toString()));
    }

    public void testDotDotTraversalEscapeRejected() throws IOException {
        Path allowed = createTempDir();
        Path sibling = createTempDir();

        Settings settings = Settings.builder().putList("esql.datasource.local_allowed_paths", allowed.toString()).build();
        LocalFileAccess access = LocalFileAccess.create(settings);

        // Construct a path that uses .. to escape to the sibling directory
        String siblingFile = sibling.resolve("secret.csv").toAbsolutePath().toString();
        // Build a traversal: allowed/../sibling/secret.csv — after normalization this points outside allowed
        String traversal = allowed.toAbsolutePath() + "/../" + sibling.getFileName() + "/secret.csv";
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> access.check(StoragePath.of("file://" + traversal))
        );
        assertThat(e.getMessage(), containsString("esql.datasource.local_allowed_paths"));
    }

    public void testMultipleAllowedRootsFirstRootMatches() throws IOException {
        Path root1 = createTempDir();
        Path root2 = createTempDir();

        Settings settings = Settings.builder().putList("esql.datasource.local_allowed_paths", root1.toString(), root2.toString()).build();
        LocalFileAccess access = LocalFileAccess.create(settings);

        Path file1 = root1.resolve("a.csv");
        Files.createFile(file1);
        // Should not throw — under root1
        access.check(StoragePath.of("file://" + file1.toAbsolutePath()));
    }

    public void testMultipleAllowedRootsSecondRootMatches() throws IOException {
        Path root1 = createTempDir();
        Path root2 = createTempDir();

        Settings settings = Settings.builder().putList("esql.datasource.local_allowed_paths", root1.toString(), root2.toString()).build();
        LocalFileAccess access = LocalFileAccess.create(settings);

        Path file2 = root2.resolve("b.csv");
        Files.createFile(file2);
        // Should not throw — under root2
        access.check(StoragePath.of("file://" + file2.toAbsolutePath()));
    }

    // --- Glob patterns ---

    public void testGlobUnderAllowedRootSucceeds() {
        Path allowed = createTempDir();
        Settings settings = Settings.builder().putList("esql.datasource.local_allowed_paths", allowed.toString()).build();
        LocalFileAccess access = LocalFileAccess.create(settings);

        // A glob location must validate its non-glob prefix directory, not the raw "*.parquet" path
        // (which would throw InvalidPathException on filesystems that reject glob metacharacters).
        access.check(StoragePath.of("file://" + allowed.toAbsolutePath() + "/*.parquet"));
        access.check(StoragePath.of("file://" + allowed.toAbsolutePath() + "/sub/data-*.csv"));
    }

    public void testGlobWhosePrefixEscapesRootRejected() {
        Path allowed = createTempDir();
        Path sibling = createTempDir();
        Settings settings = Settings.builder().putList("esql.datasource.local_allowed_paths", allowed.toString()).build();
        LocalFileAccess access = LocalFileAccess.create(settings);

        // The glob prefix resolves outside the allowed root via ".." — must be rejected, not crash.
        String traversalGlob = allowed.toAbsolutePath() + "/../" + sibling.getFileName() + "/*.parquet";
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> access.check(StoragePath.of("file://" + traversalGlob))
        );
        assertThat(e.getMessage(), containsString("esql.datasource.local_allowed_paths"));
    }

    public void testGlobRejectedWhenDisabled() {
        LocalFileAccess access = LocalFileAccess.create(Settings.EMPTY);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> access.check(StoragePath.of("file:///some/dir/*.parquet"))
        );
        assertThat(e.getMessage(), containsString("esql.datasource.local_allowed_paths"));
    }

    // --- UNRESTRICTED sentinel ---

    public void testUnrestrictedAllowsAnything() {
        // UNRESTRICTED must bypass all checks — file:// outside any root, arbitrary schemes
        LocalFileAccess.UNRESTRICTED.check(StoragePath.of("file:///etc/passwd"));
        LocalFileAccess.UNRESTRICTED.check("file:///etc/shadow");
        LocalFileAccess.UNRESTRICTED.check((String) null);
    }
}
