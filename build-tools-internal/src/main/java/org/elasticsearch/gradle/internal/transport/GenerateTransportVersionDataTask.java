/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.IdIncrement;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.getDefinedFile;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.getLatestId;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.getPriorLatestId;
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
    @Optional
    @InputDirectory
    public abstract RegularFileProperty getDataFileDirectory();

    /**
     * Used to set the name of the TransportVersionSet for which a data file will be generated.
     */
    @Optional
    @Input
    public abstract Property<String> getTVName();

    /**
     * Used to set the `major.minor` release version for which the specific TransportVersion ID will be generated.
     * E.g.: "9.2", "8.18", etc.
     */
    @Optional
    @Input
    public abstract ListProperty<String> getMinorVersionsForTV();

    @Optional
    @Input
    public abstract Property<Function<String, IdIncrement>> getIdIncrementSupplier();

    @TaskAction
    public void generateTransportVersionData() throws IOException {
        final Path tvDataDir = Objects.requireNonNull(getDataFileDirectory().getAsFile().get()).toPath();
        final var tvName = Objects.requireNonNull(getTVName().get());
        final var forMinorVersions = Objects.requireNonNull(getMinorVersionsForTV().get());
        final var idIncrementSupplier = Objects.requireNonNull(getIdIncrementSupplier().get());

        // TODO
        // - do we need to also validate that the minorVersions don't contain duplicates here? How do we enforce idempotency if we don't?
        // - is there an order we need to apply? ( I don't think so)
        // - Do we need to run this iteratively for backport construction, rather than accepting a list like this? (I don't think so)
        // - also parse args if run alone
        // - check that duplicate tags don't come in too

        // Load the tvSetData for the specified name, if it exists
        final var tvDefinition = getDefinedFile(tvDataDir, tvName);
        boolean tvDefinitionExists = tvDefinition != null;
        final List<Integer> definitionIds = tvDefinitionExists ? tvDefinition.ids() : List.of();

        var seenIds = new HashSet<Integer>();
        var ids = new ArrayList<>(definitionIds);
        for (var forMinorVersion : forMinorVersions) {
            // Get the latest transport version data for the specified minor version.
            final int latestTV = getLatestId(tvDataDir, forMinorVersion);

            // Create the new version
            final int newVersion = idIncrementSupplier.apply(forMinorVersion).bumpVersionNumber(latestTV);

            // Check that we don't already have an ID for this minor version
            var priorLatestID = getPriorLatestId(tvDataDir, forMinorVersion);
            if (containsValueInRange(priorLatestID, newVersion, ids)) {
                // TODO: Should we log something here?
                continue;
            }

            // Update the LATEST file.
            updateLatestFile(tvDataDir, forMinorVersion, tvName, newVersion);
        }

        writeDefinitionFile(tvDataDir, tvName, ids.stream().sorted(Comparator.reverseOrder()).toList());
    }

    private boolean containsValueInRange(int lowerExclusive, int upperInclusive, List<Integer> ids) {
        for (var id : ids) {
            if (lowerExclusive < id && id <= upperInclusive) {
                return true;
            }
        }
        return false;
    }
}
