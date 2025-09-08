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
import org.elasticsearch.gradle.internal.BwcVersions;
import org.gradle.api.DefaultTask;
import org.gradle.api.provider.Property;
import org.gradle.api.services.ServiceReference;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public abstract class GenerateInitialTransportVersionTask extends DefaultTask {

    @ServiceReference("transportVersionResources")
    abstract Property<TransportVersionResourcesService> getResourceService();

    @Input
    @Option(option = "release-version", description = "The Elasticsearch release version this transport version will be associated with")
    public abstract Property<String> getReleaseVersion();

    @Input
    abstract Property<BwcVersions> getBwcVersions();

    @TaskAction
    public void run() throws IOException {
        String releaseVersion = getReleaseVersion().get();
        String upperBoundName = getUpperBoundName(releaseVersion);
        TransportVersionResourcesService resources = getResourceService().get();
        TransportVersionUpperBound upstreamUpperBound = resources.getUpperBoundFromUpstream(upperBoundName);
        String initialDefinitionName = "initial_" + releaseVersion;
        TransportVersionDefinition existingDefinition = resources.getUnreferableDefinitionFromUpstream(initialDefinitionName);

        if (existingDefinition != null) {
            // this initial version has already been created upstream
            return;
        }

        TransportVersionId id;
        if (upstreamUpperBound == null) {
            if (releaseVersion.endsWith(".0") == false) {
                throw new RuntimeException("Upper bound file " + upperBoundName + " does not exist for patch version " + releaseVersion);
            }
            // The upper bound doesn't exist yet, so initialize the id based on the previous upper bound.
            // Here we assume the previous upper bound release version is unreleased, and the new
            // upper bound is not yet in bwc versions (so it will not be found)
            Version targetVersion = Version.fromString(releaseVersion);
            List<Version> unreleasedVersions = getBwcVersions().get().getUnreleased();
            int ndx = Collections.binarySearch(unreleasedVersions, targetVersion);
            Version previousVersion = unreleasedVersions.get(-ndx - 2); // element before the insertion point
            String previousUpperBoundName = previousVersion.getMajor() + "." + previousVersion.getMinor();
            TransportVersionUpperBound previousUpperBound = resources.getUpperBoundFromUpstream(previousUpperBoundName);
            id = TransportVersionId.fromInt(previousUpperBound.definitionId().complete() + 1000);
        } else {
            id = TransportVersionId.fromInt(upstreamUpperBound.definitionId().complete() + 1);
        }

        var definition = new TransportVersionDefinition(initialDefinitionName, List.of(id));
        resources.writeUnreferableDefinition(definition);
        var newUpperBound = new TransportVersionUpperBound(upperBoundName, initialDefinitionName, id);
        resources.writeUpperBound(newUpperBound);
    }

    private String getUpperBoundName(String releaseVersion) {
        int firstDotIndex = releaseVersion.indexOf('.');
        int secondDotIndex = releaseVersion.indexOf('.', firstDotIndex + 1);
        return releaseVersion.substring(0, secondDotIndex);
    }
}
