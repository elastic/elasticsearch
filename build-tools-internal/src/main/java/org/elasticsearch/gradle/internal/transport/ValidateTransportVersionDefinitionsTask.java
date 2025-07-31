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

import org.elasticsearch.gradle.internal.transport.TransportVersionUtils.TransportVersionReference;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.readAllDefinitionFiles;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.readReferencesFile;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.validateNameFormat;

/**
 * Validates that each defined transport version definition file is referenced by at least one project.
 */
@CacheableTask
public abstract class ValidateTransportVersionDefinitionsTask extends DefaultTask {

    @InputDirectory
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract DirectoryProperty getTransportResourcesDirectory();

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getReferencesFiles();

    @TaskAction
    public void validateTransportVersions() throws IOException {
        Path resourcesDir = getTransportResourcesDirectory().getAsFile().get().toPath();

        Set<String> allTvNames = new HashSet<>();
        for (var tvReferencesFile : getReferencesFiles()) {
            readReferencesFile(tvReferencesFile.toPath()).stream().map(TransportVersionReference::name).forEach(allTvNames::add);
        }

        // TODO validate that all files:
        // - have only have a single ID per release version
        // - [x] have TVs in order
        // - [x] have a name in the correct format
        // - have the correct data format
        // - [x] Don't have duplicate IDs across any files
        // - no duplicate names? Should be impossible due to filename conflicts

        HashSet<Integer> seenIds = new HashSet<>();
        try (var allDefinitions = readAllDefinitionFiles(resourcesDir)) {
            allDefinitions.forEach(definition -> {
                // Validate that all definitions are referenced in the codebase:
                if (allTvNames.contains(definition.name()) == false) {
                    throw new IllegalStateException(
                        "Transport version definition file " + definition.path(resourcesDir) + " is not referenced in the codebase."
                    );
                }

                // Validate that all Ids are in decending order:
                if (Comparators.isInOrder(definition.ids(), Comparator.reverseOrder()) == false) {
                    throw new IllegalStateException(
                        "Transport version definition file " + definition.path(resourcesDir) + " does not have ordered ids"
                    );
                }

                // Validate that the name is in the correct format:
                validateNameFormat(definition.name());

                // Validate that there are no duplicate ids across any files:
                for (var id : definition.ids()) {
                    if (seenIds.contains(id)) {
                        throw new IllegalStateException(
                            "Transport version definition file "
                                + definition.path(resourcesDir)
                                + " contains an id also present in another file"
                        );
                    }
                    seenIds.add(id);
                }
            });
        }
    }

}
