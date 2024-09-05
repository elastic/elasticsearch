/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.Directory;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.Sync;
import org.gradle.api.tasks.TaskProvider;

import static org.elasticsearch.gradle.internal.conventions.GUtils.capitalize;
import static org.elasticsearch.gradle.util.GradleUtils.getJavaSourceSets;
import static org.gradle.api.artifacts.type.ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE;
import static org.gradle.api.artifacts.type.ArtifactTypeDefinition.DIRECTORY_TYPE;

public class EmbeddedProviderExtension {

    private final Project project;
    private final TaskProvider<Task> metaTask;

    public EmbeddedProviderExtension(Project project, TaskProvider<Task> metaTask) {
        this.project = project;
        this.metaTask = metaTask;
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
        Provider<Directory> generatedResourcesRoot = project.getLayout().getBuildDirectory().dir("generated-resources");
        var generateProviderManifest = project.getTasks().register(manifestTaskName, GenerateProviderManifest.class);
        generateProviderManifest.configure(t -> {
            t.getManifestFile().set(generatedResourcesRoot.map(d -> d.dir(manifestTaskName).file("LISTING.TXT")));
            t.getProviderImplClasspath().from(implConfig);
        });
        String implTaskName = "generate" + capitalName + "ProviderImpl";
        var generateProviderImpl = project.getTasks().register(implTaskName, Sync.class);
        generateProviderImpl.configure(t -> {
            t.into(generatedResourcesRoot.map(d -> d.dir(implTaskName)));
            t.into("IMPL-JARS/" + implName, spec -> {
                spec.from(implConfig);
                spec.from(generateProviderManifest);
            });
        });
        metaTask.configure(t -> { t.dependsOn(generateProviderImpl); });

        var mainSourceSet = getJavaSourceSets(project).findByName(SourceSet.MAIN_SOURCE_SET_NAME);
        mainSourceSet.getOutput().dir(generateProviderImpl);
    }
}
