/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.DefaultTask;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.FileCollectionDependency;
import org.gradle.api.artifacts.component.ModuleComponentIdentifier;
import org.gradle.api.file.FileCollection;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.TaskAction;
import org.gradle.internal.deprecation.DeprecatableConfiguration;

import java.util.Collection;
import java.util.stream.Collectors;

import javax.inject.Inject;

public abstract class ResolveAllDependencies extends DefaultTask {

public class ResolveAllDependencies extends DefaultTask {

    private final ObjectFactory objectFactory;
    private final ProviderFactory providerFactory;

    Collection<Configuration> configs;

    @Inject
    public ResolveAllDependencies(ObjectFactory objectFactory, ProviderFactory providerFactory) {
        this.objectFactory = objectFactory;
        this.providerFactory = providerFactory;
    }

    @InputFiles
    public FileCollection getResolvedArtifacts() {
        return objectFactory.fileCollection().from(configs.stream().filter(ResolveAllDependencies::canBeResolved).map(c -> {
            // Make a copy of the configuration, omitting file collection dependencies to avoid building project artifacts
            Configuration copy = c.copyRecursive(d -> d instanceof FileCollectionDependency == false);
            copy.setCanBeConsumed(false);
            return copy;
        })
            // Include only module dependencies, ignoring things like project dependencies so we don't unnecessarily build stuff
            .map(c -> c.getIncoming().artifactView(v -> v.lenient(true).componentFilter(i -> i instanceof ModuleComponentIdentifier)))
            .map(artifactView -> providerFactory.provider(artifactView::getFiles))
            .collect(Collectors.toList()));
    }

    @TaskAction
    void resolveAll() {
        // do nothing, dependencies are resolved when snapshotting task inputs
    }

    private static boolean canBeResolved(Configuration configuration) {
        if (configuration.isCanBeResolved() == false) {
            return false;
        }
        if (configuration instanceof org.gradle.internal.deprecation.DeprecatableConfiguration) {
            var deprecatableConfiguration = (DeprecatableConfiguration) configuration;
            if (deprecatableConfiguration.canSafelyBeResolved() == false) {
                return false;
            }
        }

        return true;
    }
}
