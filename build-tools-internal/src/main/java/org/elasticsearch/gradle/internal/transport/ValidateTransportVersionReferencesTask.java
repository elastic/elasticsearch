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
import org.gradle.api.provider.Property;
import org.gradle.api.services.ServiceReference;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.VerificationException;
import org.gradle.api.tasks.VerificationTask;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Validates that each transport version reference has a referable definition.
 */
@CacheableTask
public abstract class ValidateTransportVersionReferencesTask extends DefaultTask implements VerificationTask {

    @ServiceReference("transportVersionResources")
    abstract Property<TransportVersionResourcesService> getTransportResources();

    @InputDirectory
    @Optional
    @PathSensitive(PathSensitivity.RELATIVE)
    public Path getDefinitionsDir() {
        return getTransportResources().get().getDefinitionsDir();
    }

    @InputFile
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract RegularFileProperty getReferencesFile();

    @TaskAction
    public void validateTransportVersions() throws IOException {
        Path namesFile = getReferencesFile().get().getAsFile().toPath();
        TransportVersionResourcesService resources = getTransportResources().get();

        for (var tvReference : TransportVersionReference.listFromFile(namesFile)) {
            if (resources.referableDefinitionExists(tvReference.name()) == false) {
                throw new VerificationException(
                    "TransportVersion.fromName(\""
                        + tvReference.name()
                        + "\") was used at "
                        + tvReference.location()
                        + ", but lacks a transport version definition. "
                        + "If this is a new transport version, run './gradle generateTransportVersion'."
                );
            }
        }
    }
}
