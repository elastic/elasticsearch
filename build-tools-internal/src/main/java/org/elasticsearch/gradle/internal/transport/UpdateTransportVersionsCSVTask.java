/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.elasticsearch.gradle.Version;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.services.ServiceReference;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public abstract class UpdateTransportVersionsCSVTask extends DefaultTask {

    @ServiceReference("transportVersionResources")
    abstract Property<TransportVersionResourcesService> getResourceService();

    @InputFile
    public abstract RegularFileProperty getTransportVersionsFile();

    @Input
    @Option(option = "stack-version", description = "The Elasticsearch version to record in TransportVersions.csv")
    public abstract Property<String> getStackVersion();

    @TaskAction
    public void run() throws IOException {
        Version stackVersion = Version.fromString(getStackVersion().get());
        String upperBoundName = getUpperBoundName(stackVersion);
        TransportVersionResourcesService resources = getResourceService().get();
        TransportVersionUpperBound upperBound = resources.getUpperBoundFromGitBase(upperBoundName);

        if (upperBound == null) {
            throw new RuntimeException("Missing upper bound " + upperBoundName + " for stack version " + stackVersion);
        }

        int expectedTransportVersionId = upperBound.definitionId().complete();

        // Check if this version is already in the CSV file (idempotency check)
        Integer existingTransportVersionId = getExistingTransportVersionId(stackVersion);
        if (existingTransportVersionId != null) {
            if (existingTransportVersionId != expectedTransportVersionId) {
                throw new RuntimeException(
                    "Version "
                        + stackVersion
                        + " already exists in TransportVersions.csv with transport version ID "
                        + existingTransportVersionId
                        + ", but expected "
                        + expectedTransportVersionId
                );
            }
            getLogger().lifecycle(
                "Version {} already exists in TransportVersions.csv with correct transport version ID, skipping",
                stackVersion
            );
            return;
        }

        addTransportVersionRecord(stackVersion, expectedTransportVersionId);
    }

    private Integer getExistingTransportVersionId(Version stackVersion) throws IOException {
        var csvFile = getTransportVersionsFile().getAsFile().get().toPath();
        String versionPrefix = stackVersion.toString() + ",";

        return Files.readAllLines(csvFile)
            .stream()
            .map(String::trim)
            .filter(line -> line.startsWith(versionPrefix))
            .findFirst()
            .map(line -> {
                String[] parts = line.split(",");
                if (parts.length >= 2) {
                    return Integer.parseInt(parts[1]);
                }
                return null;
            })
            .orElse(null);
    }

    private void addTransportVersionRecord(Version stackVersion, int transportVersionId) throws IOException {
        String newEntry = stackVersion + "," + transportVersionId + "\n";
        Files.writeString(getTransportVersionsFile().getAsFile().get().toPath(), newEntry, StandardOpenOption.APPEND);
    }

    private String getUpperBoundName(Version version) {
        return version.getMajor() + "." + version.getMinor();
    }
}
