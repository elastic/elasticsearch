/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.conventions;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.ArchiveOperations;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;
import java.util.Locale;
import java.util.stream.Stream;

import javax.inject.Inject;

/**
 * Repackages the Maven Central compliant aggregation zip produced by
 * `com.gradleup.nmcp.aggregation` into the layout the DRA snapshot repo
 * (`snapshots.elastic.co/&lt;buildId&gt;/maven/`) expects.
 *
 * <p>The upstream zip is intentionally Central Portal shaped: for snapshot
 * builds it emits Maven-timestamped filenames like
 * {@code foo-9.6.0-20260824.075015-1.jar} inside {@code 9.6.0-SNAPSHOT/}.
 * The DRA snapshot repo historically publishes {@code -SNAPSHOT.jar}
 * literals (produced by release-manager), and consumers depend on that shape.
 *
 * <p>For snapshot versions this task:
 * <ol>
 *   <li>Sync-extracts the aggregation zip into an output directory, renaming
 *       {@code -<yyyyMMdd.HHmmss>-<n>} segments to {@code -SNAPSHOT}.
 *       Per-file checksum sidecars (.md5/.sha1/.sha*) hash the file bytes so
 *       renaming them alongside their jar/pom is byte-safe.</li>
 *   <li>Emits a minimal {@code maven-metadata.xml} per version directory
 *       (with {@code <snapshot><localCopy>true</localCopy></snapshot>}), plus
 *       checksum sidecars, so Gradle/Maven consumers can resolve
 *       {@code <version>-SNAPSHOT} against the literal filenames.</li>
 * </ol>
 *
 * <p>For non-snapshot (release) versions the extract is a plain sync and no
 * metadata is generated — the release side of DRA doesn't need it.
 *
 * <p>See <a href="https://github.com/elastic/elasticsearch-team/issues/4297">
 * elasticsearch-team#4297</a>.
 */
public abstract class PrepareDraSnapshotMavenAggregation extends DefaultTask {

    // Match the timestamp + build-number segment that maven-publish emits for
    // snapshot deploys, e.g. `-20260824.075015-1`. The trailing `\d+` is
    // digit-only so classifier suffixes like `-sources` / `-javadoc` are
    // preserved by the rename.
    private static final String TIMESTAMP_REGEX = "-\\d{8}\\.\\d{6}-\\d+";

    @InputFile
    public abstract RegularFileProperty getSourceZip();

    @Input
    public abstract Property<String> getVersion();

    @OutputDirectory
    public abstract DirectoryProperty getOutputDir();

    @Inject
    protected abstract ArchiveOperations getArchiveOperations();

    @Inject
    protected abstract FileSystemOperations getFileSystemOperations();

    @TaskAction
    public void prepare() throws IOException {
        String version = getVersion().get();
        boolean snapshot = version.endsWith("-SNAPSHOT");
        File outDir = getOutputDir().get().getAsFile();

        getFileSystemOperations().sync(spec -> {
            spec.from(getArchiveOperations().zipTree(getSourceZip()));
            spec.into(outDir);
            if (snapshot) {
                spec.rename(TIMESTAMP_REGEX, "-SNAPSHOT");
            }
        });

        if (snapshot == false) {
            return;
        }

        String lastUpdated = ZonedDateTime.now(ZoneOffset.UTC)
            .format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        Path root = outDir.toPath();
        try (Stream<Path> stream = Files.walk(root)) {
            stream.filter(Files::isDirectory)
                .filter(PrepareDraSnapshotMavenAggregation::isVersionDirectory)
                .forEach(versionDir -> writeSnapshotMetadata(root, versionDir, lastUpdated));
        }
    }

    private static boolean isVersionDirectory(Path dir) {
        // A version directory is `<groupPath>/<artifactId>/<version>/` and by
        // convention contains at least one `.pom`. Using the pom presence as
        // the marker avoids parsing filenames.
        try (Stream<Path> s = Files.list(dir)) {
            return s.anyMatch(p -> p.getFileName().toString().endsWith(".pom"));
        } catch (IOException e) {
            return false;
        }
    }

    private static void writeSnapshotMetadata(Path root, Path versionDir, String lastUpdated) {
        Path artifactDir = versionDir.getParent();
        Path groupDir = artifactDir.getParent();

        String version = versionDir.getFileName().toString();
        String artifactId = artifactDir.getFileName().toString();

        StringBuilder groupBuilder = new StringBuilder();
        for (Path segment : root.relativize(groupDir)) {
            if (groupBuilder.length() > 0) {
                groupBuilder.append('.');
            }
            groupBuilder.append(segment.toString());
        }
        String groupId = groupBuilder.toString();

        // Minimal `maven-metadata.xml` for a non-uniqueVersion snapshot: with
        // `<localCopy>true</localCopy>` Gradle/Maven resolves the artifact
        // straight against the `-SNAPSHOT` literal filename instead of
        // rewriting `<value>` into a timestamped variant.
        String xml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <metadata>
              <groupId>%s</groupId>
              <artifactId>%s</artifactId>
              <version>%s</version>
              <versioning>
                <snapshot>
                  <localCopy>true</localCopy>
                </snapshot>
                <lastUpdated>%s</lastUpdated>
              </versioning>
            </metadata>
            """.formatted(groupId, artifactId, version, lastUpdated);

        try {
            Path metadata = versionDir.resolve("maven-metadata.xml");
            Files.writeString(metadata, xml, StandardCharsets.UTF_8);
            writeChecksumSidecars(metadata);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void writeChecksumSidecars(Path file) throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        // Match the sidecar set nmcp already emits for the other artifacts in
        // the zip; keeping them symmetric avoids surprise on the S3 side.
        for (String algorithm : new String[] { "MD5", "SHA-1", "SHA-256", "SHA-512" }) {
            try {
                MessageDigest digest = MessageDigest.getInstance(algorithm);
                String hex = HexFormat.of().formatHex(digest.digest(bytes));
                String extension = "." + algorithm.toLowerCase(Locale.ROOT).replace("-", "");
                Files.writeString(file.resolveSibling(file.getFileName() + extension), hex, StandardCharsets.UTF_8);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("Missing required digest " + algorithm, e);
            }
        }
    }
}
