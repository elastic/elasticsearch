/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.transform.UnzipTransform;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.attributes.Attribute;
import org.gradle.api.tasks.TaskProvider;

import static org.gradle.api.artifacts.type.ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE;
import static org.gradle.api.artifacts.type.ArtifactTypeDefinition.DIRECTORY_TYPE;
import static org.gradle.api.artifacts.type.ArtifactTypeDefinition.JAR_TYPE;

public class EmbeddedProviderPlugin implements Plugin<Project> {
    static final Attribute<Boolean> IMPL_ATTR = Attribute.of("is.impl", Boolean.class);

    @Override
    public void apply(Project project) {

        project.getDependencies().registerTransform(UnzipTransform.class, transformSpec -> {
            transformSpec.getFrom().attribute(ARTIFACT_TYPE_ATTRIBUTE, JAR_TYPE).attribute(IMPL_ATTR, true);
            transformSpec.getTo().attribute(ARTIFACT_TYPE_ATTRIBUTE, DIRECTORY_TYPE).attribute(IMPL_ATTR, true);
            transformSpec.parameters(parameters -> parameters.getIncludeArtifactName().set(true));
        });

        TaskProvider<Task> metaTask = project.getTasks().register("generateProviderImpls");
        project.getExtensions().create("embeddedProviders", EmbeddedProviderExtension.class, project, metaTask);
    }
}
