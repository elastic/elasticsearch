/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.transform.UnzipTransform;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.Directory;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.Sync;

import static org.elasticsearch.gradle.internal.conventions.GUtils.capitalize;
import static org.elasticsearch.gradle.util.GradleUtils.getJavaSourceSets;
import static org.gradle.api.artifacts.type.ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE;
import static org.gradle.api.artifacts.type.ArtifactTypeDefinition.DIRECTORY_TYPE;
import static org.gradle.api.artifacts.type.ArtifactTypeDefinition.JAR_TYPE;

public class EmbeddedProviderExtension {

    private final Project project;

    public EmbeddedProviderExtension(Project project) {
        this.project = project;
    }

    void impl(String implName, Project implProject) {
        String projectName = implProject.getName();
        String capitalName = capitalize(projectName);

        Configuration implConfig = project.getConfigurations().detachedConfiguration(project.getDependencies().create(implProject));
        implConfig.attributes(attrs -> {
            attrs.attribute(ARTIFACT_TYPE_ATTRIBUTE, DIRECTORY_TYPE);
            attrs.attribute(EmbeddedProviderPlugin.IMPL_ATTR, true);
        });

        String manifestTaskName = "generate" + capitalName + "ProviderManifest";
        Provider<Directory> generatedResourcesDir = project.getLayout().getBuildDirectory().dir("generated-resources");
        var generateProviderManifest = project.getTasks().register(manifestTaskName, GenerateProviderManifest.class);
        generateProviderManifest.configure(t -> {
            t.getManifestFile().set(generatedResourcesDir.get().file("LISTING.TXT"));
            t.getProviderImplClasspath().from(implConfig);
        });

        String implTaskName = "generate" + capitalName + "ProviderImpl";
        Provider<Directory> implsDir = generatedResourcesDir.map(d -> d.dir("impls"));
        var generateProviderImpl = project.getTasks().register(implTaskName, Sync.class);
        generateProviderImpl.configure(t -> {
            t.setDestinationDir(implsDir.get().dir(projectName).getAsFile());
            t.into("IMPL-JARS/" + implName, copySpec -> {
                copySpec.from(implConfig);
                copySpec.from(generateProviderManifest);
            });
        });

        var mainSourceSet = getJavaSourceSets(project).findByName(SourceSet.MAIN_SOURCE_SET_NAME);
        mainSourceSet.getOutput().dir(generateProviderImpl);
    }
}
