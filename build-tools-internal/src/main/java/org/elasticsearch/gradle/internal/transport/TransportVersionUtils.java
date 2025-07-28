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

import static org.gradle.api.artifacts.type.ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE;

class TransportVersionUtils {
    static final Attribute<Boolean> TRANSPORT_VERSION_REFERENCES_ATTRIBUTE = Attribute.of("transport-version-references", Boolean.class);

    private static final String LATEST_DIR = "latest";
    private static final String DEFINED_DIR = "defined";

    record TransportVersionDefinition(String name, List<Integer> ids) {}

    record TransportVersionReference(String name, String location) {
        @Override
        public String toString() {
            return name + " " + location;
        }
    }

    static TransportVersionDefinition getLatestFile(Path dataDir, String majorMinor) throws IOException {
        return readDefinitionFile(dataDir.resolve(LATEST_DIR).resolve(majorMinor + ".csv"), true);
    }

    static TransportVersionDefinition getDefinedFile(Path dataDir, String name) throws IOException {
        validateNameFormat(name);
        var filePath = dataDir.resolve(DEFINED_DIR).resolve(name);
        if (Files.isRegularFile(filePath) == false) {
            return null;
        }
        return readDefinitionFile(filePath, false);
    }

    static void writeDefinitionFile(Path dataDir, String name, List<Integer> ids) throws IOException {
        validateNameFormat(name);
        assert ids != null && ids.isEmpty() == false : "Ids must be non-empty";
        Files.writeString(
            dataDir.resolve(DEFINED_DIR).resolve(name + ".csv"),
            ids.stream().map(String::valueOf).collect(Collectors.joining(",")) + "\n",
            StandardCharsets.UTF_8
        );
    }

    static void updateLatestFile(Path dataDir, String majorMinor, String name, int id) throws IOException {
        validateNameFormat(name);
        var path = dataDir.resolve(LATEST_DIR).resolve(majorMinor + ".csv");
        assert Files.isRegularFile(path) : "\"Latest\" file was not found at" + path + ", but is required: ";
        Files.writeString(path, name + "," + id + "\n", StandardCharsets.UTF_8);
    }

    static void validateNameFormat(String name) {
        if (Pattern.compile("^\\w+$").matcher(name).matches() == false) {
            throw new GradleException("The TransportVersion name must only contain underscores and alphanumeric characters.");
        }
    }

    static TransportVersionDefinition readDefinitionFile(Path file, boolean nameInFile) throws IOException {
        assert file.endsWith(".csv");
        String rawName = file.getFileName().toString();

        String[] parts = Files.readString(file, StandardCharsets.UTF_8).split(",");
        String name = nameInFile ? parts[0] : rawName.substring(0, rawName.length() - 4);
        List<Integer> ids = new ArrayList<>();
        for (int i = nameInFile ? 1 : 0; i < parts.length; ++i) {
            try {
                ids.add(Integer.parseInt(parts[i]));
            } catch (NumberFormatException nfe) {
                throw new IllegalStateException("Invalid transport version file format [" + file + "], id could not be parsed", nfe);
            }
        }

        if (Comparators.isInOrder(ids, Comparator.reverseOrder()) == false) {
            throw new IOException("Invalid transport version data file [" + file + "], ids are not in sorted");
        }
        return new TransportVersionDefinition(name, ids);
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

    static Directory getDefinitionsDirectory(Project project) {
        Directory serverDir = project.getRootProject().project(":server").getLayout().getProjectDirectory();
        return serverDir.dir("src/main/resources/transport/defined");
    }

    static void addTransportVersionReferencesAttribute(AttributeContainer attributes) {
        attributes.attribute(ARTIFACT_TYPE_ATTRIBUTE, "txt");
        attributes.attribute(TransportVersionUtils.TRANSPORT_VERSION_REFERENCES_ATTRIBUTE, true);
    }
}
