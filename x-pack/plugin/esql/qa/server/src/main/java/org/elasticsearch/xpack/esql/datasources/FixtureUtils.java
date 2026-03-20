/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import com.github.luben.zstd.ZstdOutputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.logging.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Enumeration;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.GZIPOutputStream;

public class FixtureUtils {
    /** Resource path for test fixtures */
    public static final String FIXTURES_RESOURCE_PATH = "/iceberg-fixtures";

    /** Compressed extensions generated on the fly; skip loading from disk */
    public static final Set<String> COMPRESSED_EXTENSIONS = Set.of(".gz", ".zst", ".zstd", ".bz2", ".bz");

    /**
     * Iterate over all fixture entries in the iceberg-fixtures resource directory,
     * supporting both filesystem and JAR-packaged resources. Compressed files are skipped.
     */
    public static void forEachFixtureEntry(Class<?> anchor, CheckedBiConsumer<String, byte[], IOException> consumer) throws Exception {
        URL resourceUrl = anchor.getResource(FIXTURES_RESOURCE_PATH);
        if (resourceUrl == null) {
            throw new IllegalStateException("Fixtures resource path not found: " + FIXTURES_RESOURCE_PATH);
        }

        if (resourceUrl.getProtocol().equals("file")) {
            Path fixturesPath = Paths.get(resourceUrl.toURI());
            if (Files.exists(fixturesPath) == false) {
                throw new IllegalStateException("Fixtures path does not exist: " + fixturesPath);
            }
            Files.walkFileTree(fixturesPath, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    String name = file.getFileName().toString();
                    if (COMPRESSED_EXTENSIONS.stream().anyMatch(name::endsWith)) {
                        return FileVisitResult.CONTINUE;
                    }
                    String relativePath = fixturesPath.relativize(file).toString();
                    byte[] content = Files.readAllBytes(file);
                    consumer.accept(relativePath, content);
                    return FileVisitResult.CONTINUE;
                }
            });
        } else if (resourceUrl.getProtocol().equals("jar")) {
            JarURLConnection jarConnection = (JarURLConnection) resourceUrl.openConnection();
            String entryPrefix = jarConnection.getEntryName();
            if (entryPrefix.endsWith("/") == false) {
                entryPrefix = entryPrefix + "/";
            }
            try (JarFile jarFile = jarConnection.getJarFile()) {
                Enumeration<JarEntry> entries = jarFile.entries();
                while (entries.hasMoreElements()) {
                    JarEntry entry = entries.nextElement();
                    String entryName = entry.getName();
                    if (entry.isDirectory() || entryName.startsWith(entryPrefix) == false) {
                        continue;
                    }
                    String relativePath = entryName.substring(entryPrefix.length());
                    if (relativePath.isEmpty()) {
                        continue;
                    }
                    String fileName = relativePath.contains("/") ? relativePath.substring(relativePath.lastIndexOf('/') + 1) : relativePath;
                    if (COMPRESSED_EXTENSIONS.stream().anyMatch(fileName::endsWith)) {
                        continue;
                    }
                    try (InputStream is = jarFile.getInputStream(entry)) {
                        byte[] content = is.readAllBytes();
                        consumer.accept(relativePath, content);
                    }
                }
            }
        } else {
            throw new IllegalStateException("Unsupported resource protocol: " + resourceUrl);
        }
    }

    /**
     * Find the first pipe character that's not inside a quoted string.
     * Used by fixture injectParams methods to locate where to insert WITH clauses.
     */
    static int findFirstPipeAfterExternal(String query) {
        boolean inQuotes = false;
        char quoteChar = 0;

        for (int i = 0; i < query.length(); i++) {
            char c = query.charAt(i);

            if (inQuotes == false && (c == '"' || c == '\'')) {
                inQuotes = true;
                quoteChar = c;
            } else if (inQuotes && c == quoteChar) {
                inQuotes = false;
            } else if (inQuotes == false && c == '|') {
                return i;
            }
        }

        return -1;
    }

    /**
     * Resolve the local filesystem path to the iceberg-fixtures directory, or null if
     * fixtures are packaged inside a JAR.
     */
    public static Path resolveLocalFixturesPath(Logger logger, Class<?> anchor) {
        URL resourceUrl = anchor.getResource(FIXTURES_RESOURCE_PATH);
        if (resourceUrl != null && "file".equals(resourceUrl.getProtocol())) {
            try {
                Path path = Paths.get(resourceUrl.toURI());
                if (Files.exists(path)) {
                    return path;
                }
            } catch (Exception e) {
                logger.warn("Failed to resolve local fixtures path", e);
            }
        }
        return null;
    }

    public static byte[] compress(byte[] input, String suffix) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        return switch (suffix) {
            case ".gz" -> {
                try (GZIPOutputStream out = new GZIPOutputStream(baos)) {
                    out.write(input);
                }
                yield baos.toByteArray();
            }
            case ".zst", ".zstd" -> {
                try (ZstdOutputStream out = new ZstdOutputStream(baos)) {
                    out.write(input);
                }
                yield baos.toByteArray();
            }
            case ".bz2", ".bz" -> {
                try (BZip2CompressorOutputStream out = new BZip2CompressorOutputStream(baos)) {
                    out.write(input);
                }
                yield baos.toByteArray();
            }
            default -> throw new IllegalArgumentException("Unknown compression: " + suffix);
        };
    }

    public static void writeCompressedVariantsToFixturesPath(Path fixturesPath) throws IOException {
        Files.walkFileTree(fixturesPath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                String name = file.getFileName().toString();
                if (name.endsWith(".csv") || name.endsWith(".ndjson")) {
                    byte[] content = Files.readAllBytes(file);
                    Path parent = file.getParent();
                    for (String suffix : COMPRESSED_EXTENSIONS) {
                        byte[] compressed = compress(content, suffix);
                        Files.write(parent.resolve(name + suffix), compressed);
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
