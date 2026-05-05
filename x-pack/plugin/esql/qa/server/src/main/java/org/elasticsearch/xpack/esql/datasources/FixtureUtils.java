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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.GZIPOutputStream;

public class FixtureUtils {
    private static final Logger logger = LogManager.getLogger(FixtureUtils.class);

    /** Resource path for test fixtures */
    public static final String FIXTURES_RESOURCE_PATH = "/iceberg-fixtures";

    /**
     * Same directory as {@link #FIXTURES_RESOURCE_PATH} without a leading slash, for
     * {@link ClassLoader#getResources(String)} (which does not use leading {@code /}).
     * {@link #FIXTURES_RESOURCE_PATH} must begin with {@code /}.
     */
    private static final String FIXTURES_RESOURCE_PATH_FOR_CLASS_LOADER = FIXTURES_RESOURCE_PATH.substring(1);

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
            logger.info("Iterating {} from filesystem [{}] (anchor [{}])", FIXTURES_RESOURCE_PATH, fixturesPath, anchor.getName());
            forEachFixtureEntryFromFilesystem(fixturesPath, consumer);
        } else if (resourceUrl.getProtocol().equals("jar")) {
            logger.info("Iterating {} from JAR [{}] (anchor [{}])", FIXTURES_RESOURCE_PATH, resourceUrl, anchor.getName());
            forEachFixtureEntryFromJar(resourceUrl, consumer);
        } else {
            logger.warn("Unsupported fixtures resource protocol [{}] for [{}]", resourceUrl.getProtocol(), resourceUrl);
            throw new IllegalStateException("Unsupported resource protocol: " + resourceUrl);
        }
    }

    /**
     * Like {@link #forEachFixtureEntry(Class, CheckedBiConsumer)} but merges every root under
     * {@link #FIXTURES_RESOURCE_PATH} returned by {@link ClassLoader#getResources(String)}. Multiple roots
     * are common when one dependency contributes a partial tree (e.g. CSV-only) and the test project adds
     * generated Parquet; {@link Class#getResource(String)} only sees the first match, so fixtures such as
     * {@code standalone/web_logs.parquet} would otherwise never load into HTTP fixtures.
     * <p>
     * Roots are processed in enumeration order; for the same {@code relativePath}, later roots overwrite
     * earlier ones (matching typical classpath precedence for overlapping resources).
     */
    public static void forEachFixtureEntryMergingAllClasspathRoots(
        ClassLoader classLoader,
        CheckedBiConsumer<String, byte[], IOException> consumer
    ) throws Exception {
        List<URL> urls = new ArrayList<>();
        Enumeration<URL> resources = classLoader.getResources(FIXTURES_RESOURCE_PATH_FOR_CLASS_LOADER);
        while (resources.hasMoreElements()) {
            urls.add(resources.nextElement());
        }
        if (urls.isEmpty()) {
            throw new IllegalStateException("Fixtures resource path not found: " + FIXTURES_RESOURCE_PATH);
        }
        logger.info("Merging {} {} classpath root(s)", urls.size(), FIXTURES_RESOURCE_PATH_FOR_CLASS_LOADER);
        for (URL resourceUrl : urls) {
            logger.info("Loading fixtures from [{}]", resourceUrl);
            if (resourceUrl.getProtocol().equals("file")) {
                Path fixturesPath = Paths.get(resourceUrl.toURI());
                if (Files.exists(fixturesPath) == false) {
                    throw new IllegalStateException("Fixtures path does not exist: " + fixturesPath);
                }
                forEachFixtureEntryFromFilesystem(fixturesPath, consumer);
            } else if (resourceUrl.getProtocol().equals("jar")) {
                forEachFixtureEntryFromJar(resourceUrl, consumer);
            } else {
                logger.warn("Unsupported fixtures resource protocol [{}] for [{}]", resourceUrl.getProtocol(), resourceUrl);
                throw new IllegalStateException("Unsupported resource protocol: " + resourceUrl);
            }
        }
    }

    /**
     * Iterate fixture files under a resolved {@code iceberg-fixtures} directory on the filesystem.
     */
    static void forEachFixtureEntryFromFilesystem(Path fixturesPath, CheckedBiConsumer<String, byte[], IOException> consumer)
        throws IOException {
        Files.walkFileTree(fixturesPath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                String name = file.getFileName().toString();
                if (COMPRESSED_EXTENSIONS.stream().anyMatch(name::endsWith)) {
                    return FileVisitResult.CONTINUE;
                }
                String relativePath = fixturesPath.relativize(file).toString().replace('\\', '/');
                logger.debug("Fixture [{}] from filesystem path [{}]", relativePath, file.toAbsolutePath());
                byte[] content = Files.readAllBytes(file);
                consumer.accept(relativePath, content);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * Iterate fixture entries packaged under {@code iceberg-fixtures} inside a JAR.
     *
     * @param jarResourceUrl {@code jar:} URL for the resource directory (from {@link Class#getResource(String)})
     */
    static void forEachFixtureEntryFromJar(URL jarResourceUrl, CheckedBiConsumer<String, byte[], IOException> consumer) throws IOException {
        JarURLConnection jarConnection = (JarURLConnection) jarResourceUrl.openConnection();
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
                logger.debug("Fixture [{}] from JAR entry [{}] in [{}]", relativePath, entryName, jarFile.getName());
                try (InputStream is = jarFile.getInputStream(entry)) {
                    byte[] content = is.readAllBytes();
                    consumer.accept(relativePath, content);
                }
            }
        }
    }

    /**
     * Inject the given comma-separated map entries into the EXTERNAL clause of {@code externalPart}.
     * <ul>
     *   <li>If {@code externalPart} already ends with {@code WITH { ... }}, the new entries are
     *       merged into the existing map immediately after the opening {@code &#123;}, i.e. they
     *       appear <em>before</em> the entries already present. The fixture-injected credentials
     *       therefore come first; if a spec author intentionally writes a key that the fixture also
     *       provides (e.g. {@code endpoint}), the spec value will win in maps that take
     *       last-wins semantics, but please avoid relying on this — the precedence is a
     *       consequence of the merge order rather than a contract.</li>
     *   <li>Otherwise a fresh {@code WITH &#123; ... &#125;} is appended.</li>
     *   <li>If both {@code entries} is empty and there is no existing {@code WITH} clause, the
     *       input is returned unchanged (no synthetic empty {@code WITH &#123; &#125;} is emitted).</li>
     * </ul>
     * The entries string must be valid map content (e.g. {@code "endpoint": "x", "key": "y"}) — it
     * is inserted verbatim.
     */
    public static String injectWithEntries(String externalPart, String entries) {
        int withBrace = findOpenBraceOfTrailingWith(externalPart);
        if (withBrace < 0) {
            return entries.isEmpty() ? externalPart : externalPart + " WITH { " + entries + " }";
        }
        if (entries.isEmpty()) {
            return externalPart;
        }
        // Strip a leading space from the existing map body since we always emit ", " as the separator,
        // otherwise input like "{ "k": v }" would yield two spaces between the merged entries.
        String tail = externalPart.substring(withBrace + 1);
        if (tail.startsWith(" ")) {
            tail = tail.substring(1);
        }
        return externalPart.substring(0, withBrace + 1) + " " + entries + ", " + tail;
    }

    /**
     * Locate the position of the {@code &#123;} that opens a trailing {@code WITH &#123; ... &#125;}
     * clause in an EXTERNAL fragment, or {@code -1} if no such clause is present. The detection is
     * quote-aware so a literal {@code WITH} inside the source path string is ignored.
     *
     * <p>Note: escape sequences (e.g. {@code \"}) inside quoted strings are not handled — a
     * backslash-escaped quote will toggle the quote state. This is acceptable for test fixture
     * code where paths never contain escaped quotes.
     */
    private static int findOpenBraceOfTrailingWith(String externalPart) {
        int len = externalPart.length();
        boolean inQuotes = false;
        char quoteChar = 0;
        int lastUnquotedWith = -1;

        for (int i = 0; i + 4 <= len; i++) {
            char c = externalPart.charAt(i);
            if (inQuotes == false && (c == '"' || c == '\'')) {
                inQuotes = true;
                quoteChar = c;
            } else if (inQuotes && c == quoteChar) {
                inQuotes = false;
            } else if (inQuotes == false
                && (c == 'W' || c == 'w')
                && externalPart.regionMatches(true, i, "WITH", 0, 4)
                && (i == 0 || isIdentPart(externalPart.charAt(i - 1)) == false)
                && (i + 4 == len || isIdentPart(externalPart.charAt(i + 4)) == false)) {
                    lastUnquotedWith = i;
                }
        }

        if (lastUnquotedWith < 0) {
            return -1;
        }
        for (int i = lastUnquotedWith + 4; i < len; i++) {
            char c = externalPart.charAt(i);
            if (c == '{') {
                return i;
            }
            if (Character.isWhitespace(c) == false) {
                return -1;
            }
        }
        return -1;
    }

    private static boolean isIdentPart(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
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

    /**
     * Directory path for Elasticsearch {@code path.repo} when tests need a narrow filesystem root for
     * {@code shared_repo}-backed file entitlements (e.g. esql-datasource-http). Using {@code java.io.tmpdir}
     * or {@code /tmp} as the repo root can imply read access under the test distribution's {@code modules}
     * directory and fail {@code FilesEntitlementsValidation} at node bootstrap.
     * <p>
     * If {@code iceberg-fixtures} resolves to an on-disk path for the given anchor class, that path is
     * returned. Otherwise a unique temporary directory is created once per anchor class per JVM via
     * {@link Files#createTempDirectory(String, java.nio.file.attribute.FileAttribute[])}.
     */
    public static String pathRepoRootForIcebergFixtures(Class<?> anchor) {
        Path local = resolveLocalFixturesPath(logger, anchor);
        if (local != null) {
            return local.toAbsolutePath().toString();
        }
        return pathRepoFallback.get(anchor).toAbsolutePath().toString();
    }

    private static final ClassValue<Path> pathRepoFallback = new ClassValue<>() {
        @Override
        protected Path computeValue(Class<?> type) {
            try {
                return Files.createTempDirectory("esql-path-repo-" + type.getSimpleName() + "-");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    };

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
