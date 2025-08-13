/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.gradle.api.Project;
import org.gradle.api.attributes.Attribute;
import org.gradle.api.attributes.AttributeContainer;
import org.gradle.api.file.Directory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.gradle.api.artifacts.type.ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE;

class TransportVersionUtils {

    static final Attribute<Boolean> TRANSPORT_VERSION_REFERENCES_ATTRIBUTE = Attribute.of("transport-version-references", Boolean.class);

    record TransportVersionReference(String name, String location) {
        @Override
        public String toString() {
            return name + "," + location;
        }
    }

    record TransportVersionDefinition(String name, List<TransportVersionId> ids) {
        public static TransportVersionDefinition fromString(String filename, String contents) {
            assert filename.endsWith(".csv");
            String name = filename.substring(0, filename.length() - 4);
            List<TransportVersionId> ids = new ArrayList<>();

            if (contents.isEmpty() == false) {
                for (String rawId : contents.split(",")) {
                    try {
                        ids.add(parseId(rawId));
                    } catch (NumberFormatException e) {
                        throw new IllegalStateException("Failed to parse id " + rawId + " in " + filename, e);
                    }
                }
            }

            return new TransportVersionDefinition(name, ids);
        }
    }

    record TransportVersionLatest(String branch, String name, TransportVersionId id) {
        public static TransportVersionLatest fromString(String filename, String contents) {
            assert filename.endsWith(".csv");
            String branch = filename.substring(0, filename.length() - 4);

            String[] parts = contents.split(",");
            if (parts.length != 2) {
                throw new IllegalStateException("Invalid transport version latest file [" + filename + "]: " + contents);
            }

            return new TransportVersionLatest(branch, parts[0], parseId(parts[1]));
        }
    }

    record TransportVersionId(int complete, int major, int server, int subsidiary, int patch) implements Comparable<TransportVersionId> {

        static TransportVersionId fromString(String s) {
            int complete = Integer.parseInt(s);
            int patch = complete % 100;
            int subsidiary = (complete / 100) % 10;
            int server = (complete / 1000) % 1000;
            int major = complete / 1000000;
            return new TransportVersionId(complete, major, server, subsidiary, patch);
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
        return getDefinitionsDirectory(resourcesDirectory).getAsFile().toPath().resolve(name + ".csv");
    }

    static Path latestFilePath(Directory resourcesDirectory, String name) {
        return getLatestDirectory(resourcesDirectory).getAsFile().toPath().resolve(name + ".csv");
    }

    static TransportVersionDefinition readDefinitionFile(Path file) throws IOException {
        String contents = Files.readString(file, StandardCharsets.UTF_8).strip();
        return TransportVersionDefinition.fromString(file.getFileName().toString(), contents);
    }

    static TransportVersionLatest readLatestFile(Path file) throws IOException {
        String contents = Files.readString(file, StandardCharsets.UTF_8).strip();
        return TransportVersionLatest.fromString(file.getFileName().toString(), contents);
    }

    static List<TransportVersionReference> readReferencesFile(Path file) throws IOException {
        assert file.endsWith(".txt");
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

    private static TransportVersionId parseId(String rawId) {
        int complete = Integer.parseInt(rawId);
        int patch = complete % 100;
        int subsidiary = (complete / 100) % 10;
        int server = (complete / 1000) % 1000;
        int major = complete / 1000000;
        return new TransportVersionId(complete, major, server, subsidiary, patch);
    }

    static Directory getDefinitionsDirectory(Directory resourcesDirectory) {
        return resourcesDirectory.dir("defined");
    }

    static Directory getLatestDirectory(Directory resourcesDirectory) {
        return resourcesDirectory.dir("latest");
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
