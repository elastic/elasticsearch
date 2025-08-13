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

import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.attributes.Attribute;
import org.gradle.api.attributes.AttributeContainer;
import org.gradle.api.file.Directory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.IdComponents.MAJOR;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.IdComponents.PATCH;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.IdComponents.SERVER;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.IdComponents.SUBSIDIARY;
import static org.gradle.api.artifacts.type.ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE;

class TransportVersionUtils {
    static final Attribute<Boolean> TRANSPORT_VERSION_REFERENCES_ATTRIBUTE = Attribute.of("transport-version-references", Boolean.class);

    static final String LATEST_DIR = "latest";
    static final String DEFINED_DIR = "defined";
    static final String SERVERLESS_BRANCH = "SERVERLESS";

    private static final String CSV_SUFFIX = ".csv";

    record TransportVersionReference(String name, String location) {
        @Override
        public String toString() {
            return name + "," + location;
        }
    }

    record TransportVersionDefinition(String name, List<TransportVersionId> ids) {
        TransportVersionDefinition {
            validateNameFormat(name);
            if (Comparators.isInOrder(ids, Comparator.reverseOrder()) == false) {
                throw new IllegalArgumentException("Ids are not in decreasing order");
            }
        }

        public static TransportVersionDefinition fromString(String filename, String contents) {
            assert filename.endsWith(CSV_SUFFIX);
            String fileWithoutPath = Path.of(filename).getFileName().toString();
            String name = fileWithoutPath.substring(0, fileWithoutPath.length() - CSV_SUFFIX.length());
            List<TransportVersionId> ids = new ArrayList<>();

            if (contents.isEmpty() == false) {
                for (String rawId : contents.split(",")) {
                    try {
                        ids.add(TransportVersionId.fromString(rawId));
                    } catch (NumberFormatException e) {
                        throw new IllegalStateException("Failed to parse id " + rawId + " in " + filename, e);
                    }
                }
            }

            return new TransportVersionDefinition(name, ids);
        }
    }

    record TransportVersionLatest(String branch, String name, TransportVersionId id) {
        TransportVersionLatest {
            validateNameFormat(name);
            validateBranchFormat(branch);
        }

        public static TransportVersionLatest fromString(String filename, String contents) {
            assert filename.endsWith(".csv");
            String fileWithoutPath = Path.of(filename).getFileName().toString();
            String branch = fileWithoutPath.substring(0, fileWithoutPath.length() - CSV_SUFFIX.length());

            String[] parts = contents.split(",");
            if (parts.length != 2) {
                throw new IllegalStateException("Invalid transport version latest file [" + filename + "]: " + contents);
            }

            return new TransportVersionLatest(branch, parts[0], TransportVersionId.fromString(parts[1]));
        }
    }

    /**
     * The TV format:
     * <p>
     * MM_NNN_S_PP
     * <p>
     * M - The major version of Elasticsearch
     * NNN - The server version part
     * S - The subsidiary version part. It should always be 0 here, it is only used in subsidiary repositories.
     * PP - The patch version part
     */
    public enum IdComponents {
        MAJOR(1_000_0_00, 2),
        SERVER(1_0_00, 3),
        SUBSIDIARY(1_00, 1),
        PATCH(1, 2);

        private final int value;
        private final int max;

        IdComponents(int value, int numDigits) {
            this.value = value;
            this.max = (int) Math.pow(10, numDigits);
        }
    }

    record TransportVersionId(int complete, int major, int server, int subsidiary, int patch) implements Comparable<TransportVersionId> {
        public static TransportVersionId fromInt(int complete) {
            int patch = complete % PATCH.max;
            int subsidiary = (complete / SUBSIDIARY.value) % SUBSIDIARY.max;
            int server = (complete / SERVER.value) % SERVER.max;
            int major = complete / MAJOR.value;

            return new TransportVersionId(complete, major, server, subsidiary, patch);
        }

        public TransportVersionId bumpComponent(IdComponents idComponents) {
            int zeroesCleared = (complete / idComponents.value) * idComponents.value;
            int newId = zeroesCleared + idComponents.value;
            if ((newId / idComponents.value) % idComponents.max == 0) {
                throw new IllegalStateException(
                    "Insufficient" + idComponents.name() + " version section in TransportVersion: " + complete + ", Cannot bump."
                );
            }
            return fromInt(newId);
        }

        public static TransportVersionId fromString(String filename) {
            return fromInt(Integer.parseInt(filename));
        }

        @Override
        public int compareTo(TransportVersionId o) {
            return Integer.compare(complete, o.complete);
        }

        @Override
        public String toString() {
            return Integer.toString(complete);
        }

        public int base() {
            return (complete / 1000) * 1000;
        }
    }

    static Path definitionFilePath(Directory resourcesDirectory, String name) {
        return getDefinitionsDirectory(resourcesDirectory).getAsFile().toPath().resolve(name + CSV_SUFFIX);
    }

    static Path latestFilePath(Directory resourcesDir, String name) {
        return getLatestDirectory(resourcesDir).getAsFile().toPath().resolve(name + CSV_SUFFIX);
    }

    static TransportVersionDefinition readDefinitionFile(Path file) throws IOException {
        String contents = Files.readString(file, StandardCharsets.UTF_8).strip();
        return TransportVersionDefinition.fromString(file.getFileName().toString(), contents);
    }

    static void writeDefinitionFile(Path resourcesDir, TransportVersionDefinition definition) throws IOException {
        Files.writeString(
            resourcesDir.resolve(DEFINED_DIR).resolve(definition.name() + CSV_SUFFIX),
            definition.ids().stream().map(id -> String.valueOf(id.complete())).collect(Collectors.joining(",")) + "\n",
            StandardCharsets.UTF_8
        );
    }

    static TransportVersionLatest readLatestFile(Path file) throws IOException {
        String contents = Files.readString(file, StandardCharsets.UTF_8).strip();
        return TransportVersionLatest.fromString(file.getFileName().toString(), contents);
    }

    static TransportVersionLatest readLatestFile(Path resourcesDir, String branchName) throws IOException {
        Path latestFilePath = resourcesDir.resolve(LATEST_DIR).resolve(branchName + CSV_SUFFIX);
        return readLatestFile(latestFilePath);
    }

    static void writeLatestFile(Path resourcesDir, TransportVersionLatest latest) throws IOException {
        var path = resourcesDir.resolve(LATEST_DIR).resolve(latest.branch + CSV_SUFFIX);
        Files.writeString(path, latest.name() + "," + latest.id().complete + "\n", StandardCharsets.UTF_8);
    }

    static void validateNameFormat(String name) {
        if (Pattern.compile("^\\w+$").matcher(name).matches() == false) {
            throw new GradleException("The TransportVersion name must only contain underscores and alphanumeric characters: " + name);
        }
    }

    static void validateBranchFormat(String branchName) {
        if (Pattern.compile("^(\\d+\\.\\d+)|SERVERLESS$").matcher(branchName).matches() == false) {
            throw new GradleException("The branch name must be of the form \"int.int\" or \"SERVERLESS\"");
        }
    }

    static List<TransportVersionReference> readReferencesFile(Path file) throws IOException {
        assert file.toString().endsWith(".txt");
        List<TransportVersionReference> results = new ArrayList<>();
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            String[] parts = line.split(",", 2);
            if (parts.length != 2) {
                throw new IOException("Invalid transport version data file [" + file + "]: " + line);
            }
            results.add(new TransportVersionReference(parts[0], parts[1]));
        }
        return results;
    }

    static Directory getDefinitionsDirectory(Directory resourcesDirectory) {
        return resourcesDirectory.dir(DEFINED_DIR);
    }

    static Directory getLatestDirectory(Directory resourcesDirectory) {
        return resourcesDirectory.dir(LATEST_DIR);
    }

    static Directory getResourcesDirectory(Project project) {
        var projectName = project.findProperty("org.elasticsearch.transport.definitionsProject");
        if (projectName == null) {
            projectName = ":server";
        }
        Directory projectDir = project.project(projectName.toString()).getLayout().getProjectDirectory();
        return projectDir.dir("src/main/resources/transport");
    }

    static void addTransportVersionReferencesAttribute(AttributeContainer attributes) {
        attributes.attribute(ARTIFACT_TYPE_ATTRIBUTE, "txt");
        attributes.attribute(TransportVersionUtils.TRANSPORT_VERSION_REFERENCES_ATTRIBUTE, true);
    }
}
