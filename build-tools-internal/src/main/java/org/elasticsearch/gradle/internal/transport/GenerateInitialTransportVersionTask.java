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
import org.gradle.api.provider.Property;
import org.gradle.api.services.ServiceReference;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;

import java.io.IOException;
import java.util.List;

public abstract class GenerateInitialTransportVersionTask extends DefaultTask {

    @ServiceReference("transportVersionResources")
    abstract Property<TransportVersionResourcesService> getResourceService();

    @Input
    @Option(option = "stack-version", description = "The Elasticsearch version to generate an initial transport version for")
    public abstract Property<String> getStackVersion();

    @Input
    abstract Property<Version> getCurrentVersion();

    @TaskAction
    public void run() throws IOException {
        Version stackVersion = Version.fromString(getStackVersion().get());
        String upperBoundName = getUpperBoundName(stackVersion);
        TransportVersionResourcesService resources = getResourceService().get();
        TransportVersionUpperBound baseUpperBound = resources.getUpperBoundFromGitBase(upperBoundName);
        String initialDefinitionName = "initial_" + stackVersion;
        TransportVersionDefinition existingDefinition = resources.getUnreferableDefinitionFromGitBase(initialDefinitionName);

        // This task runs on main and release branches. In release branches we will generate the exact same
        // upper bound result because we always look at the base branch (ie upstream/main).
        if (existingDefinition == null) {
            if (baseUpperBound == null) {
                throw new RuntimeException("Missing upper bound " + upperBoundName + " for release version " + stackVersion);
            }

            // minors increment by 1000 to create a unique base, patches increment by 1 as other patches do
            int increment = stackVersion.getRevision() == 0 ? 1000 : 1;
            var id = TransportVersionId.fromInt(baseUpperBound.definitionId().complete() + increment);
            var definition = new TransportVersionDefinition(initialDefinitionName, List.of(id), false);
            resources.writeDefinition(definition);
            var newUpperBound = new TransportVersionUpperBound(upperBoundName, initialDefinitionName, id);
            resources.writeUpperBound(newUpperBound);

            Version currentVersion = getCurrentVersion().get();
            String currentUpperBoundName = getUpperBoundName(currentVersion);
            if (stackVersion.getRevision() == 0) {
                // a minor creates a new base, which the current branch must also point at
                resources.writeUpperBound(new TransportVersionUpperBound(currentUpperBoundName, initialDefinitionName, id));
            } else if (currentUpperBoundName.equals(upperBoundName) == false) {
                // A patch adds an id to an existing base. If the current branch's upper bound still points into that
                // same base, that patch id is now the latest id for the base, which the current branch cannot adopt:
                // ids in the current upper bound must be base ids so that generating a transport version here keeps
                // creating a new base. Reserve the next base for the current branch instead, held by a placeholder
                // definition. This is the case when no transport version has been added to the current branch since
                // the minor being patched was branched, ie the current upper bound is that minor's initial version.
                TransportVersionUpperBound currentUpperBound = resources.getUpperBoundFromGitBase(currentUpperBoundName);
                if (currentUpperBound != null && currentUpperBound.definitionId().base() == id.base()) {
                    var reservedId = TransportVersionId.fromInt(id.base() + 1000);
                    String reservedName = "placeholder_" + currentVersion;
                    resources.writeDefinition(new TransportVersionDefinition(reservedName, List.of(reservedId), false));
                    resources.writeUpperBound(new TransportVersionUpperBound(currentUpperBoundName, reservedName, reservedId));
                }
            }
        }
    }

    private String getUpperBoundName(Version version) {
        return version.getMajor() + "." + version.getMinor();
    }
}
