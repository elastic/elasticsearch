/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.dependencies.CompileOnlyResolvePlugin;
import org.elasticsearch.gradle.internal.precommit.DependencyLicensesTask;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.attributes.Category;
import org.gradle.api.attributes.Usage;
import org.gradle.api.plugins.JavaPlugin;

public class DependenciesInfoPlugin implements Plugin<Project> {

    public static String USAGE_ATTRIBUTE = "DependenciesInfo";

    @Override
    public void apply(final Project project) {
        project.getPlugins().apply(CompileOnlyResolvePlugin.class);
        var depsInfo = project.getTasks().register("dependenciesInfo", DependenciesInfoTask.class);

        depsInfo.configure(t -> {
            t.setRuntimeConfiguration(project.getConfigurations().getByName(JavaPlugin.RUNTIME_CLASSPATH_CONFIGURATION_NAME));
            t.setCompileOnlyConfiguration(
                project.getConfigurations().getByName(CompileOnlyResolvePlugin.RESOLVEABLE_COMPILE_ONLY_CONFIGURATION_NAME)
            );
            t.getConventionMapping().map("mappings", () -> {
                var depLic = project.getTasks().named("dependencyLicenses", DependencyLicensesTask.class);
                return depLic.get().getMappings();
            });
        });
        Configuration dependenciesInfoFilesConfiguration = project.getConfigurations().create("dependenciesInfoFiles");
        dependenciesInfoFilesConfiguration.setCanBeResolved(false);
        dependenciesInfoFilesConfiguration.setCanBeConsumed(true);
        dependenciesInfoFilesConfiguration.attributes(
            attributes -> attributes.attribute(
                Category.CATEGORY_ATTRIBUTE,
                project.getObjects().named(Category.class, Category.DOCUMENTATION)
            )
        );

        dependenciesInfoFilesConfiguration.attributes(
            attributes -> attributes.attribute(
                Usage.USAGE_ATTRIBUTE,
                project.getObjects().named(Usage.class, USAGE_ATTRIBUTE)
            )
        );
        project.getArtifacts().add("dependenciesInfoFiles", depsInfo);

    }

}
