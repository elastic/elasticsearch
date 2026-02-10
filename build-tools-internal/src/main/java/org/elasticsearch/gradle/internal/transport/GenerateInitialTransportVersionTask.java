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
import java.util.List;

public abstract class GenerateInitialTransportVersionTask extends DefaultTask {

    @ServiceReference("transportVersionResources")
    abstract Property<TransportVersionResourcesService> getResourceService();

    @InputFile
    public abstract RegularFileProperty getTransportVersionsFile();

    @Input
    @Option(option = "release-version", description = "The current Elasticsearch version being released")
    public abstract Property<String> getReleaseVersion();

    @Input
    @Option(option = "next-version", description = "The next Elasticsearch release version this transport version will be associated with")
    public abstract Property<String> getNextVersion();

    @Input
    abstract Property<Version> getCurrentVersion();

    @TaskAction
    public void run() throws IOException {
        Version releaseVersion = Version.fromString(getReleaseVersion().get());
        Version nextVersion = Version.fromString(getNextVersion().get());
        String baseUpperBoundName = getUpperBoundName(releaseVersion);
        TransportVersionResourcesService resources = getResourceService().get();
        TransportVersionUpperBound baseUpperBound = resources.getUpperBoundFromGitBase(baseUpperBoundName);
        String initialDefinitionName = "initial_" + nextVersion;
        TransportVersionDefinition existingDefinition = resources.getUnreferableDefinitionFromGitBase(initialDefinitionName);

        // This task runs on main and release branches. In release branches we will generate the exact same
        // upper bound result because we always look at the base branch (ie upstream/main).
        if (existingDefinition == null) {
            if (baseUpperBound == null) {
                throw new RuntimeException("Missing upper bound " + baseUpperBoundName + " for release version " + releaseVersion);
            }

            // Record the current transport version for the release version before creating the new one
            int currentTransportVersionId = baseUpperBound.definitionId().complete();
            addTransportVersionRecord(releaseVersion, currentTransportVersionId);

            // minors increment by 1000 to create a unique base, patches increment by 1 as other patches do
            int increment = nextVersion.getRevision() == 0 ? 1000 : 1;
            var id = TransportVersionId.fromInt(currentTransportVersionId + increment);
            var definition = new TransportVersionDefinition(initialDefinitionName, List.of(id), false);
            resources.writeDefinition(definition);
            String upperBoundName = getUpperBoundName(nextVersion);
            var newUpperBound = new TransportVersionUpperBound(upperBoundName, initialDefinitionName, id);
            resources.writeUpperBound(newUpperBound);

            if (nextVersion.getRevision() == 0) {
                Version currentVersion = getCurrentVersion().get();
                String currentUpperBoundName = getUpperBoundName(currentVersion);
                var currentUpperBound = new TransportVersionUpperBound(currentUpperBoundName, initialDefinitionName, id);
                resources.writeUpperBound(currentUpperBound);
            }
        }
    }

    private void addTransportVersionRecord(Version releaseVersion, int transportVersionId) throws IOException {
        String newEntry = releaseVersion + "," + transportVersionId + "\n";
        Files.writeString(getTransportVersionsFile().getAsFile().get().toPath(), newEntry, StandardOpenOption.APPEND);
    }

    private String getUpperBoundName(Version version) {
        return version.getMajor() + "." + version.getMinor();
    }
}
