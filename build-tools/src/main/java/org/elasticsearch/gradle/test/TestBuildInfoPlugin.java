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
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.FileCollection;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.testing.Test;
import org.gradle.language.jvm.tasks.ProcessResources;

import java.util.List;

import javax.inject.Inject;

/**
 * This plugin configures the {@link GenerateTestBuildInfoTask} task
 * with customizations for component name and output file name coming
 * from the source using the plugin (server or ES plugin).
 */
public class TestBuildInfoPlugin implements Plugin<Project> {

    protected final ProviderFactory providerFactory;

    @Inject
    public TestBuildInfoPlugin(ProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
    }

    @Override
    public void apply(Project project) {
        var testBuildInfoTask = project.getTasks().register("generateTestBuildInfo", GenerateTestBuildInfoTask.class, task -> {
            FileCollection codeLocations = project.getConfigurations().getByName("runtimeClasspath");
            Configuration compileOnly = project.getConfigurations()
                .findByName(CompileOnlyResolvePlugin.RESOLVEABLE_COMPILE_ONLY_CONFIGURATION_NAME);
            if (compileOnly != null) {
                codeLocations = codeLocations.minus(compileOnly);
            }
            var sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
            codeLocations = codeLocations.plus(sourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME).getOutput().getClassesDirs());
            task.getCodeLocations().set(codeLocations);
        });

        project.getTasks().withType(ProcessResources.class).named("processResources").configure(task -> {
            task.into("META-INF", copy -> copy.from(testBuildInfoTask));
        });

        project.getTasks().withType(Test.class).configureEach(test -> {
            if (List.of("test", "internalClusterTest").contains(test.getName())) {
                System.err.println("PATDOYLE - es.entitlement.enableForTests on " + project.getName() + test.getName());
                test.systemProperty("es.entitlement.enableForTests", "true");
            }
        });
    }
}
