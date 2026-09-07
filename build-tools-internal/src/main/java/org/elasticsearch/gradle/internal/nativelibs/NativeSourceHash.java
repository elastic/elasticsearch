/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import org.gradle.api.GradleException;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * The identity of a native library: a digest of its sources and of the toolchain that builds them,
 * used as the version of published compiled artifact.
 *
 * <p>The toolchain is included because a compiler upgrade could produce different binaries.
 */
final class NativeSourceHash {

    private NativeSourceHash() {}

    /**
     * @param sourceRoot directory the sources live under; paths are digested relative to it so the
     *                   result does not depend on where the repository is checked out
     * @param sourceFiles the library's sources
     * @param toolchainKey the build environment container image tag
     */
    static String compute(File sourceRoot, Collection<File> sourceFiles, String toolchainKey) {
        MessageDigest digest = sha256();

        // Sorted as strings rather than with File.compareTo, whose ordering is platform-dependent.
        List<File> ordered = sourceFiles.stream().sorted((a, b) -> a.toString().compareTo(b.toString())).toList();
        for (File file : ordered) {
            // Each file contributes its path plus a fixed-length digest of its content, the way git
            // hashes a tree from its blobs.
            digest.update(utf8(relativePath(sourceRoot, file)));
            digest.update((byte) 0);
            digest.update(sha256().digest(normalizeLineEndings(readAllBytes(file))));
        }

        digest.update(utf8(toolchainKey));
        return HexFormat.of().formatHex(digest.digest());
    }

    /**
     * The path of {@code file} below {@code sourceRoot}, joined with {@code /} so that a Windows
     * checkout digests the same paths as a Linux one.
     */
    private static String relativePath(File sourceRoot, File file) {
        Path root = sourceRoot.toPath().toAbsolutePath().normalize();
        Path relative = root.relativize(file.toPath().toAbsolutePath().normalize());
        return StreamSupport.stream(relative.spliterator(), false).map(Path::toString).collect(Collectors.joining("/"));
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Rewrites CRLF to LF, so that a checkout with Windows line endings has the same identity as one
     * with Unix line endings.
     */
    private static byte[] normalizeLineEndings(byte[] bytes) {
        byte[] normalized = new byte[bytes.length];
        int length = 0;
        for (int i = 0; i < bytes.length; i++) {
            boolean crlf = bytes[i] == '\r' && i + 1 < bytes.length && bytes[i + 1] == '\n';
            if (crlf == false) {
                normalized[length++] = bytes[i];
            }
        }
        return length == bytes.length ? bytes : Arrays.copyOf(normalized, length);
    }

    private static byte[] readAllBytes(File file) {
        try {
            return Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read native source " + file, e);
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new GradleException("SHA-256 is not available", e);
        }
    }
}
