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

import static org.gradle.api.artifacts.type.ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE;

class TransportVersionUtils {

    static final Attribute<Boolean> TRANSPORT_VERSION_REFERENCES_ATTRIBUTE = Attribute.of("transport-version-references", Boolean.class);

    record TransportVersionConstant(String name, List<Integer> ids) {}

    record TransportVersionReference(String name, String location) {
        @Override
        public String toString() {
            return name + " " + location;
        }
    }

    static TransportVersionConstant readDefinitionFile(Path file) throws IOException {
        assert file.endsWith(".csv");
        String rawName = file.getFileName().toString();
        String name = rawName.substring(0, rawName.length() - 4);
        List<Integer> ids = new ArrayList<>();

        for (String rawId : Files.readString(file, StandardCharsets.UTF_8).split(",")) {
            try {
                ids.add(Integer.parseInt(rawId.strip()));
            } catch (NumberFormatException e) {
                throw new IOException("Failed to parse id " + rawId + " in " + file, e);
            }
        }

        if (Comparators.isInOrder(ids, Comparator.reverseOrder()) == false) {
            throw new IOException("invalid transport version data file [" + file + "], ids are not in sorted");
        }
        return new TransportVersionConstant(name, ids);
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
        var projectName = project.findProperty("org.elasticsearch.transport.definitionsProject");
        if (projectName == null) {
            projectName = ":server";
        }
        Directory projectDir = project.project(projectName.toString()).getLayout().getProjectDirectory();
        return projectDir.dir("src/main/resources/transport/defined");
    }

    static void addTransportVersionReferencesAttribute(AttributeContainer attributes) {
        attributes.attribute(ARTIFACT_TYPE_ATTRIBUTE, "txt");
        attributes.attribute(TransportVersionUtils.TRANSPORT_VERSION_REFERENCES_ATTRIBUTE, true);
    }

}
