/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import com.google.common.collect.Streams;
import org.elasticsearch.gradle.VersionProperties;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.updateLatestFile;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.writeDefinitionFile;

/**
 * This task generates TransportVersionSetData data files that contain information about transport versions. These files
 * are added to the server project's resource directory at `server/src/main/resources/org/elasticsearch/transport/`.
 * They have the following format:
 * <pre>
 * Filename: my-transport-version-set.json  // Must be the same as the name of the transport version set.
 * {
 *   "name": "my-transport-version-set", // The name of the transport version set used for reference in the code.
 *   "ids": [
 *     9109000,  // The transport version introduced to the main branch.
 *     8841059   // The transport version backported to a previous release branch.
 *   ]
 * }
 * </pre>
 */
public abstract class GenerateTransportVersionDataTask extends DefaultTask {

    /**
     * Specifies the directory in which contains all TransportVersionSet data files.
     *
     * @return
     */
    @InputDirectory
    public abstract RegularFileProperty getDataFileDirectory();

    /**
     * Used to set the name of the TransportVersionSet for which a data file will be generated.
     */
    @Input
    public abstract Property<String> getTVName();

    /**
     * Used to set the `major.minor` release version for which the specific TransportVersion ID will be generated.
     * E.g.: "9.2", "8.18", etc.
     */
    @Input
    public abstract Property<String> getMinorVersionForTV();

    @TaskAction
    public void generateTransportVersionData() throws IOException {
        final Path tvDataDir = Objects.requireNonNull(getDataFileDirectory().getAsFile().get()).toPath();
        final var tvName = Objects.requireNonNull(getTVName().get());
        final var forMinorVersion = Objects.requireNonNull(getMinorVersionForTV().get());

        // Get the latest transport version data for the specified minor version.
        final var latestTV = TransportVersionUtils.getLatestFile(tvDataDir, forMinorVersion);

        // Create the new version
        final var mainReleaseVersion = VersionProperties.getElasticsearchVersion();
        final var areWeOnMain = forMinorVersion.equals(mainReleaseVersion);
        int newVersion = bumpVersionNumber(
            latestTV.ids().getFirst(),
            areWeOnMain ? PartToBump.SERVER : PartToBump.PATCH
        );

        // Load the tvSetData for the specified name, if it exists
        final var tvSetDataFromFile = TransportVersionUtils.getDefinedFile(tvDataDir, tvName);
        final var tvSetFileExists = tvSetDataFromFile != null;

        // Write the definition file.
        final var ids = tvSetFileExists
            ? Streams.concat(tvSetDataFromFile.ids().stream(), Stream.of(newVersion)).sorted().toList().reversed()
            : List.of(newVersion);
        writeDefinitionFile(tvDataDir, tvName, ids);

        // Update the LATEST file.
        updateLatestFile(tvDataDir, forMinorVersion, tvName, newVersion);
    }

    public enum PartToBump {
        SERVER,
        SUBSIDIARY,
        PATCH
    }

    // TODO Do I need to remove the patch when updating the server portion? NO, but probably need some additional checks
    private static int bumpVersionNumber(
        int tvIDToBump,
        PartToBump partToBump
    ) {

        /* The TV format:
         *
         * MM_NNN_S_PP
         *
         * M - The major version of Elasticsearch
         * NNN - The server version part
         * S - The subsidiary version part. It should always be 0 here, it is only used in subsidiary repositories.
         * PP - The patch version part
         */
        return switch (partToBump) {
            case SERVER -> {
                var newId = tvIDToBump + 1000;
                if ((newId / 1000) % 100 == 0) {
                    throw new IllegalStateException("Insufficient server version section in TransportVersion: " + tvIDToBump
                        + ", Cannot bump.");
                }
                yield tvIDToBump + 1000;
            }
            case SUBSIDIARY -> {
                var newId = tvIDToBump + 100;
                if ((newId / 100) == 0) {
                    throw new IllegalStateException("Insufficient subsidiary version section in TransportVersion: " + tvIDToBump
                        + ", Cannot bump.");
                }
                yield newId;
            }
            case PATCH -> {
                var newId = tvIDToBump + 1;
                if (newId % 10 == 0) {
                    throw new IllegalStateException("Insufficient patch version section in TransportVersion: " + tvIDToBump
                        + ", Cannot bump.");
                }
                yield newId;
            }
        };
    }
}
