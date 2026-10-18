/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.util;

import org.elasticsearch.gradle.internal.info.BuildParameterExtension;
import org.elasticsearch.gradle.internal.info.BuildParameterService;
import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.provider.Property;
import org.gradle.api.services.BuildServiceRegistration;

public class ParamsUtils {

    private static final String BUILD_PARAMS_SERVICE_NAME = "buildParams";

    /**
     * Applies {@link GlobalBuildInfoPlugin} only when {@code project} is the root project.
     * <p>
     * Subprojects must not apply plugins to the root project as that is a cross-project access forbidden
     * by Isolated Projects. They rely on the root build script applying {@code elasticsearch.global-build-info}
     * instead; Gradle configures a parent project before its children, so the build parameters service is
     * registered by the time a subproject plugin calls {@link #loadBuildParams(Project)}.
     */
    public static void applyGlobalBuildInfoIfRoot(Project project) {
        // The root project of a build always has the path ":"; comparing paths avoids navigating the project hierarchy
        if (Project.PATH_SEPARATOR.equals(project.getPath())) {
            project.getPluginManager().apply(GlobalBuildInfoPlugin.class);
        }
    }

    public static Property<BuildParameterExtension> loadBuildParams(Project project) {
        // Only findByName is permitted on the registrations container when Isolated Projects is enabled
        @SuppressWarnings("unchecked")
        BuildServiceRegistration<BuildParameterService, BuildParameterService.Params> buildParamsRegistrations = (BuildServiceRegistration<
            BuildParameterService,
            BuildParameterService.Params>) project.getGradle().getSharedServices().getRegistrations().findByName(BUILD_PARAMS_SERVICE_NAME);
        if (buildParamsRegistrations == null) {
            throw new GradleException(
                "Build service '"
                    + BUILD_PARAMS_SERVICE_NAME
                    + "' is not registered. Apply the 'elasticsearch.global-build-info' plugin to the root project of this build."
            );
        }
        return buildParamsRegistrations.getParameters().getBuildParams();
    }

}
