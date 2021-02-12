/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.SourceSetContainer;

/**
 * Ideally, this plugin is intended to be temporary and in the long run we want to move
 * forward to port our test fixtures to use the gradle test fixtures plugin.
 * */
public class InternalTestArtifactPlugin implements Plugin<Project> {
    @Override
    public void apply(Project project) {
        JavaPluginExtension javaPluginExtension = project.getExtensions().getByType(JavaPluginExtension.class);
        javaPluginExtension.registerFeature("testArtifacts", featureSpec -> {
            featureSpec.usingSourceSet(project.getExtensions().getByType(SourceSetContainer.class).getByName("test"));
            featureSpec.capability("org.elasticsearch.gradle", project.getName() + "-test-artifacts", "1.0");
            featureSpec.disablePublication();
        });
        Configuration testApiElements = project.getConfigurations().getByName("testApiElements");
        testApiElements.extendsFrom(project.getConfigurations().getByName("testCompileClasspath"));
        DependencyHandler dependencies = project.getDependencies();
        project.getPlugins().withType(JavaPlugin.class, javaPlugin -> {
            Dependency projectDependency = dependencies.create(project);
            dependencies.add("testApiElements", projectDependency);
            dependencies.add("testRuntimeElements", projectDependency);
        });
    }
}
