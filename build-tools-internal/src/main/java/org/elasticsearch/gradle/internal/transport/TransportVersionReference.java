/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.gradle.api.attributes.Attribute;
import org.gradle.api.attributes.AttributeContainer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.gradle.api.artifacts.type.ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE;

record TransportVersionReference(String name, String location) {

    private static final Attribute<Boolean> REFERENCES_ATTRIBUTE = Attribute.of("transport-version-references", Boolean.class);

    static List<TransportVersionReference> listFromFile(Path file) throws IOException {
        assert file.toString().endsWith(".csv") : file + " does not end in .csv";
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

    static void addArtifactAttribute(AttributeContainer attributes) {
        attributes.attribute(ARTIFACT_TYPE_ATTRIBUTE, "csv");
        attributes.attribute(REFERENCES_ATTRIBUTE, true);
    }

    static Set<String> collectNames(Iterable<File> referencesFiles) throws IOException {
        Set<String> names = new HashSet<>();
        for (var referencesFile : referencesFiles) {
            listFromFile(referencesFile.toPath()).stream().map(TransportVersionReference::name).forEach(names::add);
        }
        return names;
    }

    @Override
    public String toString() {
        return name + "," + location;
    }
}
