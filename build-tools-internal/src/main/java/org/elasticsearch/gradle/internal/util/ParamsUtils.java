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
import org.gradle.api.Project;
import org.gradle.api.provider.Property;
import org.gradle.api.services.BuildServiceRegistration;

public class ParamsUtils {

    public static Property<BuildParameterExtension> loadBuildParams(Project project) {
        BuildServiceRegistration<BuildParameterService, BuildParameterService.Params> buildParamsRegistrations = (BuildServiceRegistration<
            BuildParameterService,
            BuildParameterService.Params>) project.getGradle().getSharedServices().getRegistrations().getByName("buildParams");
        Property<BuildParameterExtension> buildParams = buildParamsRegistrations.getParameters().getBuildParams();
        return buildParams;
    }

}
