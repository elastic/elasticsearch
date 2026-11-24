/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.docker;

import org.elasticsearch.gradle.Architecture;

import java.io.File;
import java.io.FilterReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Replaces the {@code @es_sha512_<arch>@} tokens in the Docker Hub Dockerfile with the SHA-512 checksums of the Linux
 * distribution tarballs. The Docker Official Images build downloads the published tarballs, and its policy requires
 * that every download is verified. The tarballs are not reproducible between builds, so the checksums have to come
 * from the tarballs that are built (and published) together with the Dockerfile.
 * <p>
 * Gradle creates the filter first and only then sets {@link #setTarballs(Iterable)}, so the content is transformed
 * lazily, on the first read.
 */
public class DockerHubChecksumFilter extends FilterReader {

    private static final Pattern TARBALL_NAME = Pattern.compile("^elasticsearch-.+-linux-(?<arch>[a-z0-9_]+)\\.tar\\.gz$");
    private static final Pattern TOKEN = Pattern.compile("@es_sha512_(?<arch>[a-z0-9_]+)@");

    private Iterable<File> tarballs;
    private Reader transformed;

    public DockerHubChecksumFilter(Reader in) {
        super(in);
    }

    /**
     * The Linux distribution tarballs of all architectures, e.g. {@code elasticsearch-9.5.0-linux-x86_64.tar.gz}.
     */
    public void setTarballs(Iterable<File> tarballs) {
        this.tarballs = tarballs;
    }

    @Override
    public int read() throws IOException {
        return transformed().read();
    }

    @Override
    public int read(char[] buffer, int offset, int length) throws IOException {
        return transformed().read(buffer, offset, length);
    }

    @Override
    public long skip(long n) throws IOException {
        return transformed().skip(n);
    }

    @Override
    public boolean ready() throws IOException {
        return transformed().ready();
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    private Reader transformed() throws IOException {
        if (transformed == null) {
            if (tarballs == null) {
                throw new IllegalStateException("The tarballs to checksum have not been set");
            }
            StringWriter content = new StringWriter();
            in.transferTo(content);
            transformed = new StringReader(replaceTokens(content.toString(), checksums(tarballs)));
        }
        return transformed;
    }

    /**
     * Replaces every checksum token in {@code content}, failing if there is no checksum for a token's architecture, so that
     * a Dockerfile with a missing checksum can never be published.
     */
    static String replaceTokens(String content, Map<String, String> checksumsByArchitecture) {
        Matcher matcher = TOKEN.matcher(content);
        StringBuilder result = new StringBuilder();
        while (matcher.find()) {
            String arch = matcher.group("arch");
            String checksum = checksumsByArchitecture.get(arch);
            if (checksum == null) {
                throw new IllegalStateException(
                    "No Elasticsearch tarball for architecture [" + arch + "], found " + checksumsByArchitecture.keySet()
                );
            }
            matcher.appendReplacement(result, checksum);
        }
        matcher.appendTail(result);
        return result.toString();
    }

    /**
     * Computes the SHA-512 checksums of the tarballs, keyed by architecture classifier, failing unless there is exactly
     * one tarball for each supported architecture.
     */
    static Map<String, String> checksums(Iterable<File> tarballs) {
        Map<String, String> checksums = new TreeMap<>();
        for (File tarball : tarballs) {
            Matcher matcher = TARBALL_NAME.matcher(tarball.getName());
            if (matcher.matches() == false) {
                throw new IllegalArgumentException("Not an Elasticsearch Linux tarball: " + tarball);
            }
            String arch = matcher.group("arch");
            if (checksums.put(arch, sha512(tarball)) != null) {
                throw new IllegalArgumentException("More than one Elasticsearch tarball for architecture [" + arch + "]");
            }
        }
        for (Architecture architecture : Architecture.values()) {
            if (checksums.containsKey(architecture.classifier) == false) {
                throw new IllegalArgumentException(
                    "No Elasticsearch tarball for architecture [" + architecture.classifier + "] in " + tarballs
                );
            }
        }
        return checksums;
    }

    private static String sha512(File file) {
        try (InputStream input = Files.newInputStream(file.toPath())) {
            MessageDigest digest = MessageDigest.getInstance("SHA-512");
            byte[] buffer = new byte[1 << 16];
            int read;
            while ((read = input.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("SHA-512 is always available", e);
        }
    }
}
