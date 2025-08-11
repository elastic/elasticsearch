/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.elasticsearch.gradle.internal.transport.TransportVersionUtils.TransportVersionReference;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.readDefinitionFile;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.readReferencesFile;

/**
 * Validates that each defined transport version constant is referenced by at least one project.
 */
@CacheableTask
public abstract class ValidateTransportVersionDefinitionsTask extends DefaultTask {

    @InputDirectory
    @Optional
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract DirectoryProperty getDefinitionsDirectory();

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getReferencesFiles();

    @TaskAction
    public void validateTransportVersions() throws IOException {
        Path constantsDir = getDefinitionsDirectory().getAsFile().get().toPath();

        Set<String> allTvNames = new HashSet<>();
        for (var tvReferencesFile : getReferencesFiles()) {
            readReferencesFile(tvReferencesFile.toPath()).stream().map(TransportVersionReference::name).forEach(allTvNames::add);
        }

        try (var constantsStream = Files.list(constantsDir)) {
            for (var constantsFile : constantsStream.toList()) {
                var tv = readDefinitionFile(constantsFile);
                if (allTvNames.contains(tv.name()) == false) {
                    throw new IllegalStateException("Transport version constant " + tv.name() + " is not referenced");
                }
            }
        }
    }
}
