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
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.DEFINED_DIR;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.readDefinitionFile;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.readReferencesFile;

/**
 * Validates that each defined transport version definition file is referenced by at least one project.
 */
public abstract class ValidateTransportVersionDefinitionsTask extends DefaultTask {

    @InputDirectory
    public abstract DirectoryProperty getTVDataDirectory();

    @InputFiles
    public abstract ConfigurableFileCollection getReferencesFiles();

    @TaskAction
    public void validateTransportVersions() throws IOException {
        Path dataDir = getTVDataDirectory().getAsFile().get().toPath();
        Path definitionsDir = dataDir.resolve(DEFINED_DIR);

        Set<String> allTvNames = new HashSet<>();
        for (var tvReferencesFile : getReferencesFiles()) {
            readReferencesFile(tvReferencesFile.toPath()).stream().map(TransportVersionReference::name).forEach(allTvNames::add);
        }

        // TODO validate that all files:
        // - have only have a single ID per release version
        // - have TVs in order
        // - have the correct name
        // - have the correct data format
        // - Don't have duplicate IDs across any files
        // - no duplicate names? Should be impossible due to filename conflicts

        try (var definitionsStream = Files.list(definitionsDir)) {
            for (var definitionFile : definitionsStream.toList()) {
                // Validate that all definitions are referenced in the code.
                var tv = readDefinitionFile(definitionFile, false);
                if (allTvNames.contains(tv.name()) == false) {
                    throw new IllegalStateException(
                        "Transport version definition " + tv.name() + " in file " + definitionFile + "is not referenced in the code."
                    );
                }

                // Validate that all Ids are in decending order:
                if (Comparators.isInOrder(tv.ids(), Comparator.reverseOrder()) == false) {
                    throw new IllegalStateException("Transport version definition file " + definitionFile + " does not have ordered ids");
                }

            }
        }
    }

}
