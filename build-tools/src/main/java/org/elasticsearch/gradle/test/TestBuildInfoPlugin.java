/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.test;

import org.elasticsearch.gradle.dependencies.CompileOnlyResolvePlugin;
import org.elasticsearch.gradle.plugin.GenerateTestBuildInfoTask;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.language.jvm.tasks.ProcessResources;

import javax.inject.Inject;

public class TestBuildInfoPlugin implements Plugin<Project> {

    protected final ProviderFactory providerFactory;

    @Inject
    public TestBuildInfoPlugin(ProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
    }

    @Override
    public void apply(Project project) {
        var testBuildInfoTask = project.getTasks().register("generateTestBuildInfo", GenerateTestBuildInfoTask.class, task -> {
            var sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
            task.getCodeLocations()
                .set(
                    project.getConfigurations()
                        .getByName("runtimeClasspath")
                        .minus(project.getConfigurations().getByName(CompileOnlyResolvePlugin.RESOLVEABLE_COMPILE_ONLY_CONFIGURATION_NAME))
                        .plus(sourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME).getOutput().getClassesDirs())
                );
            Provider<Directory> directory = project.getLayout().getBuildDirectory().dir("generated-build-info");
            task.getOutputDirectory().set(directory);
        });

        project.getTasks().withType(ProcessResources.class).named("processResources").configure(task -> {
            task.into("META-INF", copy -> copy.from(testBuildInfoTask));
        });
    }
}
