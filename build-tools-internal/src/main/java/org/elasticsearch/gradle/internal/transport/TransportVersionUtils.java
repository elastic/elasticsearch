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
import org.gradle.api.file.Directory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

class TransportVersionUtils {

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
}
