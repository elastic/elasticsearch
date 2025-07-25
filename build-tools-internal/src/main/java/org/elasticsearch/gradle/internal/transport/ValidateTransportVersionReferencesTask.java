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
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Validates that each transport version named reference has a constant definition.
 */
public abstract class ValidateTransportVersionReferencesTask extends DefaultTask {

    @InputDirectory
    public abstract DirectoryProperty getDefinitionsDirectory();

    @InputFile
    public abstract RegularFileProperty getReferencesFile();

    @TaskAction
    public void validateTransportVersions() throws IOException {
        Path constantsDir = getDefinitionsDirectory().getAsFile().get().toPath();
        Path namesFile = getReferencesFile().get().getAsFile().toPath();

        for (var tvReference : TransportVersionUtils.readReferencesFile(namesFile)) {
            Path constantFile = constantsDir.resolve(tvReference.name() + ".csv");
            if (Files.exists(constantFile) == false) {
                throw new RuntimeException(
                    "TransportVersion.fromName(\""
                        + tvReference.name()
                        + "\") was used at "
                        + tvReference.location()
                        + ", but lacks a"
                        + " transport version constant definition. This can be generated with the <TODO> task" // todo
                );
            }
        }
    }
}
