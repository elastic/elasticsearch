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
import org.gradle.api.provider.Provider;
import org.gradle.api.initialization.IncludedBuild;
import org.gradle.api.invocation.Gradle;

import java.io.File;

public class VersionPropertiesPlugin implements Plugin<Project> {

    public static File locateElasticsearchWorkspace(Gradle gradle) {
        if (gradle.getRootProject().getName().startsWith("build-tools")) {
            File buildToolsParent = gradle.getRootProject().getRootDir().getParentFile();
            if (versionFileExists(buildToolsParent)) {
                return buildToolsParent;
            }
            return buildToolsParent;
        }
        if (gradle.getParent() == null) {
            // See if any of these included builds is the Elasticsearch gradle
            for (IncludedBuild includedBuild : gradle.getIncludedBuilds()) {
                if (versionFileExists(includedBuild.getProjectDir())) {
                    return includedBuild.getProjectDir();
                }
            }

            // Otherwise assume this gradle is the root elasticsearch workspace
            return gradle.getRootProject().getRootDir();
        } else {
            // We're an included build, so keep looking
            return locateElasticsearchWorkspace(gradle.getParent());
        }
    }

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

    private static boolean versionFileExists(File rootDir) {
        return new File(rootDir, "build-tools-internal/version.properties").exists();
    }

}
