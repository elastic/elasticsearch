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
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Predicate;

/**
 * Validates that each transport version named reference has a constant definition.
 */
@CacheableTask
public abstract class ValidateTransportVersionReferencesTask extends DefaultTask {

    @InputDirectory
    @Optional
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract DirectoryProperty getDefinitionsDirectory();

    @InputFile
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract RegularFileProperty getReferencesFile();

    @TaskAction
    public void validateTransportVersions() throws IOException {
        final Predicate<String> referenceChecker;
        if (getDefinitionsDirectory().isPresent()) {
            Path definitionsDir = getDefinitionsDirectory().getAsFile().get().toPath();
            referenceChecker = (name) -> Files.exists(definitionsDir.resolve(name + ".csv"));
        } else {
            referenceChecker = (name) -> false;
        }
        Path namesFile = getReferencesFile().get().getAsFile().toPath();

        for (var tvReference : TransportVersionReference.listFromFile(namesFile)) {
            if (referenceChecker.test(tvReference.name()) == false) {
                throw new RuntimeException(
                    "TransportVersion.fromName(\""
                        + tvReference.name()
                        + "\") was used at "
                        + tvReference.location()
                        + ", but lacks a"
                        + " transport version definition. This can be generated with the <TODO> task" // todo
                );
            }
        }
    }
}
