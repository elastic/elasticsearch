/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.dependencies;

import org.gradle.api.NamedDomainObjectProvider;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.plugins.JavaPlugin;

public class CompileOnlyResolvePlugin implements Plugin<Project> {
    public static final String RESOLVEABLE_COMPILE_ONLY_CONFIGURATION_NAME = "resolveableCompileOnly";

    @Override
    public void apply(Project project) {
        project.getConfigurations().all(configuration -> {
            if (configuration.getName().equals(JavaPlugin.COMPILE_ONLY_CONFIGURATION_NAME)) {
                NamedDomainObjectProvider<Configuration> resolvableCompileOnly = project.getConfigurations()
                    .register(RESOLVEABLE_COMPILE_ONLY_CONFIGURATION_NAME);
                resolvableCompileOnly.configure((c) -> {
                    c.setCanBeResolved(true);
                    c.setCanBeConsumed(false);
                    c.extendsFrom(configuration);
                });
            }
        });
    }
}
