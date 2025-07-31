/*
* Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
* or more contributor license agreements. Licensed under the "Elastic License
* 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
* Public License v 1"; you may not use this file except in compliance with, at
* your election, the "Elastic License 2.0", the "GNU Affero General Public
* License v3.0 only", or the "Server Side Public License, v 1".
*/

package org.elasticsearch.gradle.internal.transport;

import com.google.common.collect.Comparators;

import org.elasticsearch.gradle.Version;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.attributes.Attribute;
import org.gradle.api.attributes.AttributeContainer;
import org.gradle.api.file.Directory;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.gradle.api.artifacts.type.ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE;

class TransportVersionUtils {
    static final Attribute<Boolean> TRANSPORT_VERSION_REFERENCES_ATTRIBUTE = Attribute.of("transport-version-references", Boolean.class);

    static final String LATEST_DIR = "latest";
    static final String DEFINED_DIR = "defined";

    private static final String CSV_SUFFIX = ".csv";

    record TransportVersionDefinition(String name, List<Integer> ids) {
        String path(Path resourcesDir) {
            return resourcesDir.resolve(DEFINED_DIR).resolve(name + CSV_SUFFIX).toString();
        }
    }

    record TransportVersionLatest(MajorMinor version, String name, int id) {
        String path(Path resourcesDir) {
            return resourcesDir.resolve(LATEST_DIR).resolve(version.toString() + CSV_SUFFIX).toString();
        }
    }

    record TransportVersionReference(String name, String location) {
        @Override
        public @NotNull String toString() {
            return name + " " + location;
        }
    }

    static int getLatestId(Path resourcesDir, String majorMinor) throws IOException {
        return readLatestFile(resourcesDir, MajorMinor.of(majorMinor)).id();
    }

    static TransportVersionLatest readLatestFile(Path resourcesDir, MajorMinor version) throws IOException {
        Path filePath = resourcesDir.resolve(LATEST_DIR).resolve(version.toString() + CSV_SUFFIX);
        String[] parts = Files.readString(filePath, StandardCharsets.UTF_8).split(",");
        assert parts.length == 2;
        return new TransportVersionLatest(version, parts[0], Integer.parseInt(parts[1]));
    }

    static TransportVersionDefinition readDefinitionFile(Path resourcesDir, String name) {
        validateNameFormat(name);
        var filePath = resourcesDir.resolve(DEFINED_DIR).resolve(name + CSV_SUFFIX);
        if (Files.isRegularFile(filePath) == false) {
            System.out.println("Potato file was not found at " + filePath);
            return null;
        }
        try {
            String[] parts = Files.readString(filePath, StandardCharsets.UTF_8).split(",");
            List<Integer> ids = Arrays.stream(parts).map(rawId -> Integer.parseInt(rawId.strip())).toList();

            if (ids.isEmpty()) {
                throw new IllegalStateException("Invalid transport version data file [" + filePath + "], no ids");
            }
            if (Comparators.isInOrder(ids, Comparator.reverseOrder()) == false) {
                throw new IllegalStateException("Invalid transport version data file [" + filePath + "], ids are not in sorted");
            }
            return new TransportVersionDefinition(name, ids);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read definition file", e);
        }
    }

    static Stream<TransportVersionDefinition> readAllDefinitionFiles(Path resourcesDir) throws IOException {
        var definitionsStream = Files.list(resourcesDir.resolve(DEFINED_DIR));
        return definitionsStream.map(path -> {
            String fileName = path.getFileName().toString();
            assert fileName.endsWith(CSV_SUFFIX);
            String name = fileName.substring(0, fileName.length() - 4);
            System.out.println("Potato path.getparent" + path.getParent());
            return readDefinitionFile(resourcesDir, name);
        });
    }

    static void writeDefinitionFile(Path resourcesDir, String name, List<Integer> ids) throws IOException {
        validateNameFormat(name);
        assert ids != null && ids.isEmpty() == false : "Ids must be non-empty";
        Files.writeString(
            resourcesDir.resolve(DEFINED_DIR).resolve(name + CSV_SUFFIX),
            ids.stream().map(String::valueOf).collect(Collectors.joining(",")) + "\n",
            StandardCharsets.UTF_8
        );
    }

    static void updateLatestFile(Path resourcesDir, String majorMinor, String name, int id) throws IOException {
        validateNameFormat(name);
        var path = resourcesDir.resolve(LATEST_DIR).resolve(majorMinor + CSV_SUFFIX);
        assert Files.isRegularFile(path) : "\"Latest\" file was not found at" + path + ", but is required: ";
        Files.writeString(path, name + "," + id + "\n", StandardCharsets.UTF_8);
    }

    static void validateNameFormat(String name) {
        if (Pattern.compile("^\\w+$").matcher(name).matches() == false) {
            throw new GradleException("The TransportVersion name must only contain underscores and alphanumeric characters.");
        }
    }

    static List<TransportVersionReference> readReferencesFile(Path file) throws IOException {
        assert file.endsWith(".txt");
        List<TransportVersionReference> results = new ArrayList<>();
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            String[] parts = line.split(" ", 2);
            if (parts.length != 2) {
                throw new IOException("Invalid transport version data file [" + file + "]: " + line);
            }
            results.add(new TransportVersionReference(parts[0], parts[1]));
        }
        return results;
    }

    static Directory getTransportDefinitionsDirectory(Project project) {
        return getTransportResourcesDirectory(project).dir("defined");
    }

    static Directory getTransportResourcesDirectory(Project project) {
        var projectName = project.findProperty("org.elasticsearch.transport.definitionsProject");
        if (projectName == null) {
            projectName = ":server";
        }
        Directory serverDir = project.getRootProject().project(projectName.toString()).getLayout().getProjectDirectory();
        return serverDir.dir("src/main/resources/transport");
    }

    static void addTransportVersionReferencesAttribute(AttributeContainer attributes) {
        attributes.attribute(ARTIFACT_TYPE_ATTRIBUTE, "txt");
        attributes.attribute(TransportVersionUtils.TRANSPORT_VERSION_REFERENCES_ATTRIBUTE, true);
    }

    /**
     * Specifies which part of the TransportVersion id to bump. The TV format:
     * <p>
     * MM_NNN_S_PP
     * <p>
     * M - The major version of Elasticsearch
     * NNN - The server version part
     * S - The subsidiary version part. It should always be 0 here, it is only used in subsidiary repositories.
     * PP - The patch version part
     */
    public enum IdIncrement {
        MAJOR(1_000_0_00, 2),
        SERVER(1_0_00, 3),
        SUBSIDIARY(1_00, 1),
        PATCH(1, 2);

        private final int value;
        private final int max;

        IdIncrement(int value, int numDigits) {
            this.value = value;
            this.max = (int) Math.pow(10, numDigits);
        }

        public int bumpTransportVersion(int tvIDToBump) {
            int zeroesCleared = (tvIDToBump / value) * value;
            int newId = zeroesCleared + value;
            if ((newId / value) % max == 0) {
                throw new IllegalStateException(
                    "Insufficient" + name() + " version section in TransportVersion: " + tvIDToBump + ", Cannot bump."
                );
            }
            return newId;
        }
    }

    public record MajorMinor(int major, int minor) {
        public static MajorMinor of(String majorMinor) {
            String[] versionParts = majorMinor.split("\\.");
            assert versionParts.length == 2;
            return new MajorMinor(Integer.parseInt(versionParts[0]), Integer.parseInt(versionParts[1]));
        }

        public static MajorMinor of(Version version) {
            return new MajorMinor(version.getMajor(), version.getMinor());
        }

        @Override
        public @NotNull String toString() {
            return major + "." + minor;
        }
    }

    public static int getPriorLatestId(Path dataDir, String majorMinor) throws IOException {
        var version = MajorMinor.of(majorMinor);
        if (version.minor() > 0) {
            return getLatestId(dataDir, version.major + "." + (version.minor - 1));
        }
        try (var pathStream = Files.list(Objects.requireNonNull(dataDir.resolve(LATEST_DIR)))) {
            var highestMinorOfPrevMajor = pathStream.flatMap(path -> {
                var fileMajorMinor = path.getFileName().toString().replace(CSV_SUFFIX, "");
                var fileVersion = MajorMinor.of(fileMajorMinor);
                return fileVersion.major == version.major - 1 ? Stream.of(fileVersion.minor) : Stream.empty();
            }).sorted().toList().getLast();
            return getLatestId(dataDir, (version.major - 1) + "." + highestMinorOfPrevMajor);
        }
    }

}
