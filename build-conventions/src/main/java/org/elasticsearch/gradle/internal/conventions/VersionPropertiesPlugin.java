/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.conventions;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.initialization.IncludedBuild;
import org.gradle.api.invocation.Gradle;
import org.gradle.api.provider.Provider;

import java.io.File;

public class VersionPropertiesPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        File workspaceDir = locateElasticsearchWorkspace(project.getGradle());

        // Register the service if not done yet
        File infoPath = new File(workspaceDir, "build-tools-internal");
        Provider<VersionPropertiesBuildService> serviceProvider = project.getGradle().getSharedServices()
                .registerIfAbsent("versions", VersionPropertiesBuildService.class, spec -> {
            spec.getParameters().getInfoPath().set(infoPath);
        });
        project.getExtensions().add("versions", serviceProvider.get().getProperties());
    }

    private static File locateElasticsearchWorkspace(Gradle project) {
        if (project.getParent() == null) {
            // See if any of these included builds is the Elasticsearch project
            for (IncludedBuild includedBuild : project.getIncludedBuilds()) {
                File versionProperties = new File(includedBuild.getProjectDir(), "build-tools-internal/version.properties");
                if (versionProperties.exists()) {
                    return includedBuild.getProjectDir();
                }
            }

            // Otherwise assume this project is the root elasticsearch workspace
            return project.getRootProject().getRootDir();
        } else {
            // We're an included build, so keep looking
            return locateElasticsearchWorkspace(project.getParent());
        }
    }
}
