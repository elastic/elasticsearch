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
import org.gradle.api.attributes.Attribute;
import org.gradle.api.file.Directory;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.Sync;

import static org.elasticsearch.gradle.internal.conventions.GUtils.capitalize;
import static org.elasticsearch.gradle.util.GradleUtils.getJavaSourceSets;
import static org.gradle.api.artifacts.type.ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE;
import static org.gradle.api.artifacts.type.ArtifactTypeDefinition.DIRECTORY_TYPE;
import static org.gradle.api.artifacts.type.ArtifactTypeDefinition.JAR_TYPE;

public class EmbeddedProviderExtension {

    private static final Attribute<Boolean> IMPL_ATTR = Attribute.of("is.impl", Boolean.class);

    private final Project project;

    public EmbeddedProviderExtension(Project project) {
        this.project = project;
    }

    void impl(String implName, Project implProject) {
        String projectName = implProject.getName();
        String capitalName = capitalize(projectName);

        String configName = "embedded" + capitalName;
        Configuration implConfig = project.getConfigurations().create(configName);
        implConfig.attributes(attrs -> {
            attrs.attribute(ARTIFACT_TYPE_ATTRIBUTE, DIRECTORY_TYPE);
            attrs.attribute(IMPL_ATTR, true);
        });

        var deps = project.getDependencies();
        deps.registerTransform(UnzipTransform.class,
            transformSpec -> {
                transformSpec.getFrom().attribute(ARTIFACT_TYPE_ATTRIBUTE, JAR_TYPE).attribute(IMPL_ATTR, true);
                transformSpec.getTo().attribute(ARTIFACT_TYPE_ATTRIBUTE, DIRECTORY_TYPE).attribute(IMPL_ATTR, true);
                transformSpec.parameters(parameters -> parameters.getIncludeArtifactName().set(true));
            });
        deps.add(configName, implProject);

        String manifestTaskName = "generate" + capitalName + "ProviderManifest";
        Directory generatedResourcesDir = project.getLayout().getBuildDirectory().dir("generated-resources").get();
        var generateProviderManifest = project.getTasks().register(manifestTaskName, GenerateProviderManifest.class);
        generateProviderManifest.configure(t -> {
            t.getManifestFile().set(generatedResourcesDir.file("LISTING.TXT"));
            t.getProviderImplClasspath().from(implConfig);
        });

        String implTaskName = "generate" + capitalName + "ProviderImpl";
        Directory implsDir = generatedResourcesDir.dir("impls");
        var generateProviderImpl = project.getTasks().register(implTaskName, Sync.class);
        generateProviderImpl.configure(t -> {
            t.setDestinationDir(implsDir.dir(projectName).getAsFile());
            t.into("IMPL-JARS/" + implName, copySpec -> {
                copySpec.from(implConfig);
                copySpec.from(generateProviderManifest);
            });
        });

        var mainSourceSet = getJavaSourceSets(project).findByName(SourceSet.MAIN_SOURCE_SET_NAME);
        mainSourceSet.getOutput().dir(generateProviderImpl);
    }


}
