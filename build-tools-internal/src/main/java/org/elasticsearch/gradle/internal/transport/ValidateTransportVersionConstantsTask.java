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
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

/**
 * Validates that each defined transport version constant is referenced by at least one project.
 */
public abstract class ValidateTransportVersionConstantsTask extends DefaultTask {

    @InputDirectory
    public abstract DirectoryProperty getConstantsDirectory();

    @InputFiles
    public abstract ConfigurableFileCollection getNamesFiles();

    @TaskAction
    public void validateTransportVersions() throws IOException {
        Path constantsDir = getConstantsDirectory().getAsFile().get().toPath();

        Set<String> allTvNames = new HashSet<>();
        for (var tvNamesFile : getNamesFiles()) {
            allTvNames.addAll(Files.readAllLines(tvNamesFile.toPath(), StandardCharsets.UTF_8));
        }

        try (var constantsStream = Files.list(constantsDir)) {
            for (var constantsFile : constantsStream.toList()) {
                var tv = TransportVersionUtils.readDataFile(constantsFile);
                if (allTvNames.contains(tv.name()) == false) {
                    throw new IllegalStateException("Transport version constant " + tv.name() + " is not referenced");
                }
            }
        }
    }
}
