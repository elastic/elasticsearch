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
import org.gradle.api.plugins.BasePluginConvention;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.jvm.tasks.Jar;

import javax.inject.Inject;

/**
 * Ideally, this plugin is intended to be temporary and in the long run we want to move
 * forward to port our test fixtures to use the gradle test fixtures plugin.
 * */
public class InternalTestArtifactPlugin implements Plugin<Project> {

    private final ProviderFactory providerFactory;

    @Inject
    public InternalTestArtifactPlugin(ProviderFactory providers) {
        this.providerFactory = providers;
    }

    @Override
    public void apply(Project project) {
        JavaPluginExtension javaPluginExtension = project.getExtensions().getByType(JavaPluginExtension.class);
        javaPluginExtension.registerFeature("testArtifacts", featureSpec -> {
            featureSpec.usingSourceSet(project.getExtensions().getByType(SourceSetContainer.class).getByName("test"));
            featureSpec.capability("org.elasticsearch.gradle", project.getName() + "-test-artifacts", "1.0");
            // This feature is only used internally in the
            // elasticsearch build so we do not need any publication.
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
        // PolicyUtil doesn't handle classifier notation well probably.
        // Instead of fixing PoliceUtil we stick to the pattern of changing
        // the basename here to indicate its a test artifacts jar.
        BasePluginConvention convention = (BasePluginConvention) project.getConvention().getPlugins().get("base");
        project.getTasks().named("testJar", Jar.class).configure(jar -> {
            jar.getArchiveBaseName().convention(providerFactory.provider(() -> convention.getArchivesBaseName() + "-test-artifacts"));
            jar.getArchiveClassifier().set("");
        });
    }
}
