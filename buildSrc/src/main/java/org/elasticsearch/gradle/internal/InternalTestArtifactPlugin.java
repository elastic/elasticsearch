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
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.bundling.Jar;

/**
 * Ideally, yhis plugin is intended to be temporary and in the long run we want to move
 * forward to port our test fixtures to use the gradle test fixtures plugin.
 * */
public class InternalTestArtifactPlugin implements Plugin<Project> {
    @Override
    public void apply(Project project) {
        Configuration testArtifactsConfiguration = project.getConfigurations().create("testArtifacts");
        testArtifactsConfiguration.extendsFrom(project.getConfigurations().getByName("testImplementation"));

        var testJar = project.getTasks().register("testJar", Jar.class, jar -> {
            jar.getArchiveAppendix().set("test");
            SourceSet testSourceSet = project.getExtensions().getByType(SourceSetContainer.class).getByName("test");
            jar.from(testSourceSet.getOutput());
        });

        project.getArtifacts().add("testArtifacts", testJar);
        project.getPlugins()
            .withType(JavaPlugin.class, javaPlugin -> project.getArtifacts().add("testArtifacts", project.getTasks().named("jar")));
    }
}
