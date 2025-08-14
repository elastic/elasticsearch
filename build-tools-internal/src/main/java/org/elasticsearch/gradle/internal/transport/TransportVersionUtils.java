/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class TransportVersionUtils {
    static final String LATEST_DIR = "latest";
    static final String DEFINED_DIR = "defined";

    private static final String CSV_SUFFIX = ".csv";

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

    static TransportVersionLatest readLatestFile(Path resourcesDir, String latestName) throws IOException {
        Path latestFilePath = resourcesDir.resolve(LATEST_DIR).resolve(latestName + CSV_SUFFIX);
        return readLatestFile(latestFilePath);
    }

    static void writeLatestFile(Path resourcesDir, TransportVersionLatest latest) throws IOException {
        var path = resourcesDir.resolve(LATEST_DIR).resolve(latest.releaseBranch() + CSV_SUFFIX);
        Files.writeString(path, latest.name() + "," + latest.id().complete() + "\n", StandardCharsets.UTF_8);
    }

    static void validateNameFormat(String name) {
        if (Pattern.compile("^\\w+$").matcher(name).matches() == false) {
            throw new GradleException("The TransportVersion name must only contain underscores and alphanumeric characters: " + name);
        }
    }

    static void validateBranchFormat(String branchName) {
        if (Pattern.compile("^(\\d+\\.\\d+)|SERVERLESS$").matcher(branchName).matches() == false) {
            throw new GradleException("The releaseBranch name must be of the form \"int.int\" or \"SERVERLESS\"");
        }
    }

    static Directory getDefinitionsDirectory(Directory resourcesDirectory) {
        return resourcesDirectory.dir(DEFINED_DIR);
    }

    static Directory getLatestDirectory(Directory resourcesDirectory) {
        return resourcesDirectory.dir(LATEST_DIR);
    }

    static String getResourcePath(String resourcesProjectPath, String subPath) {
        return resourcesProjectPath + "/src/main/resources/transport/" + subPath;
    }

    static Directory getResourcesDirectory(Project project) {
        var projectName = project.findProperty("org.elasticsearch.transport.definitionsProject");
        if (projectName == null) {
            projectName = ":server";
        }
        Directory projectDir = project.project(projectName.toString()).getLayout().getProjectDirectory();
        return projectDir.dir("src/main/resources/transport");
    }

    static String getNameFromCsv(String filepath) {
        return filepath.substring(filepath.lastIndexOf(File.pathSeparator) + 1, filepath.length() - 4 /* .csv */);
    }
}
