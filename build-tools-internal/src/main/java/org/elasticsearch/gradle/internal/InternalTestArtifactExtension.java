/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.Project;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.plugins.BasePluginExtension;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.SourceSet;
import org.gradle.jvm.tasks.Jar;

public class InternalTestArtifactExtension {
    private final Project project;
    private final ProviderFactory providerFactory;

    public InternalTestArtifactExtension(Project project, ProviderFactory providerFactory) {
        this.project = project;
        this.providerFactory = providerFactory;
    }

    public void registerTestArtifactFromSourceSet(SourceSet sourceSet) {
        String name = sourceSet.getName();
        JavaPluginExtension javaPluginExtension = project.getExtensions().getByType(JavaPluginExtension.class);
        javaPluginExtension.registerFeature(name + "Artifacts", featureSpec -> {
            featureSpec.usingSourceSet(sourceSet);
            featureSpec.capability("org.elasticsearch.gradle", project.getName() + "-" + name + "-artifacts", "1.0");
            // This feature is only used internally in the
            // elasticsearch build so we do not need any publication.
            featureSpec.disablePublication();
        });

        DependencyHandler dependencies = project.getDependencies();
        project.getPlugins().withType(JavaPlugin.class, javaPlugin -> {
            Dependency projectDependency = dependencies.create(project);
            dependencies.add(sourceSet.getApiElementsConfigurationName(), projectDependency);
            dependencies.add(sourceSet.getRuntimeElementsConfigurationName(), projectDependency);
        });
        // PolicyUtil doesn't handle classifier notation well probably.
        // Instead of fixing PoliceUtil we stick to the pattern of changing
        // the basename here to indicate its a test artifacts jar.
        BasePluginExtension pluginExtension = project.getExtensions().getByType(BasePluginExtension.class);
        project.getTasks().named(name + "Jar", Jar.class).configure(jar -> {
            jar.getArchiveBaseName()
                .convention(providerFactory.provider(() -> pluginExtension.getArchivesName().get() + "-" + name + "-artifacts"));
            jar.getArchiveClassifier().set("");
        });
    }
}
