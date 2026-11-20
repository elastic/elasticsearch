/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.elasticsearch.gradle.transform.FilteringJarTransform;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.tasks.AbstractCopyTask;

import javax.inject.Inject;

/**
 * Exposes typed distribution-layout copy-spec builders via {@code esDistributionLayout}.
 */
public class DistributionLayoutPlugin implements Plugin<Project> {

    private final FileSystemOperations fileSystemOperations;

    @Inject
    public DistributionLayoutPlugin(FileSystemOperations fileSystemOperations) {
        this.fileSystemOperations = fileSystemOperations;
    }

    @Override
    public void apply(Project project) {
        ObjectFactory objects = project.getObjects();
        registerEntitlementBridgeTransform(project);

        DistributionLayoutExtension extension = project.getExtensions()
            .create(
                "esDistributionLayout",
                DistributionLayoutExtension.class,
                project,
                fileSystemOperations,
                objects,
                project.getProviders()
            );

        project.getTasks().withType(AbstractCopyTask.class).configureEach(copyTask -> {
            Task task = copyTask;
            // Shared project-wide registry for step 4: every copy task tracks all realized expansion maps.
            // This can invalidate unrelated distribution tasks but is strictly more correct than missing inputs.
            // TODO(step 7): precise per-task token expansion inputs.
            task.getInputs().property("esDistributionLayout.tokenExpansions", extension.getRequestedTokenExpansions());
        });
    }

    private static void registerEntitlementBridgeTransform(Project project) {
        FilteringJarTransform.registerTransform(project.getDependencies(), spec -> {
            spec.exclude("module-info.class");
            spec.exclude("META-INF/versions/**");
        });
    }
}
