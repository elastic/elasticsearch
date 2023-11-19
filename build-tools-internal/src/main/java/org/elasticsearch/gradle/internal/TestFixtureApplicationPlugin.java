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
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.attributes.Usage;
import org.gradle.api.distribution.DistributionContainer;
import org.gradle.api.distribution.plugins.DistributionPlugin;
import org.gradle.api.file.CopySpec;
import org.gradle.api.plugins.ApplicationPlugin;
import org.gradle.api.tasks.Sync;

import java.io.File;

public class TestFixtureApplicationPlugin implements Plugin<Project> {
    public static final String DISTRIBUTION_USAGE_ATTRIBUTE = "distribution";
    final String EXPLODED_BUNDLE_CONFIG = "installedFixtureDistro";

    private Project project;

    @Override
    public void apply(Project project) {
        this.project = project;
        project.getPluginManager().apply(ApplicationPlugin.class);
        configureDefaultDistributionContent(project);
        configureInstallDistributionVariant(project);
    }

    private void configureInstallDistributionVariant(Project project) {
        var installedFixtureDistro = project.getConfigurations().create(EXPLODED_BUNDLE_CONFIG);
        installedFixtureDistro.setCanBeResolved(false);
        installedFixtureDistro.setCanBeConsumed(true);
        installedFixtureDistro.getAttributes()
            .attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE);
        installedFixtureDistro.getAttributes()
            .attribute(Usage.USAGE_ATTRIBUTE, project.getObjects().named(Usage.class, DISTRIBUTION_USAGE_ATTRIBUTE));
        project.getArtifacts()
            .add(EXPLODED_BUNDLE_CONFIG, project.getTasks().withType(Sync.class).named(DistributionPlugin.TASK_INSTALL_NAME));
    }

    private void configureDefaultDistributionContent(Project project) {
        var distributions = project.getExtensions().getByType(DistributionContainer.class);
        var mainDistribution = distributions.getByName("main");
        mainDistribution.contents(this::configureContents);
    }

    private void configureContents(CopySpec copySpec) {
        File dockerfile = project.file("Dockerfile");
        if (dockerfile.exists()) {
            copySpec.from(project.file("Dockerfile"));
        }
    }
}
