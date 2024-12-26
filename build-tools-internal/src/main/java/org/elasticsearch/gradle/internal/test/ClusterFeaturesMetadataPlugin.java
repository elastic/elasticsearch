/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.dependencies.CompileOnlyResolvePlugin;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;

import java.util.Map;

/**
 * Extracts cluster feature metadata into a machine-readable format for use in backward compatibility testing.
 */
public class ClusterFeaturesMetadataPlugin implements Plugin<Project> {
    public static final String CLUSTER_FEATURES_JSON = "cluster-features.json";
    public static final String FEATURES_METADATA_TYPE = "features-metadata-json";
    public static final String FEATURES_METADATA_CONFIGURATION = "featuresMetadata";

    @Override
    public void apply(Project project) {
        Configuration featureMetadataExtractorConfig = project.getConfigurations().create("featuresMetadataExtractor", c -> {
            // Don't bother adding this dependency if the project doesn't exist which simplifies testing
            if (project.findProject(":test:metadata-extractor") != null) {
                c.defaultDependencies(d -> d.add(project.getDependencies().project(Map.of("path", ":test:metadata-extractor"))));
            }
        });

        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        SourceSet mainSourceSet = sourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME);

        TaskProvider<ClusterFeaturesMetadataTask> generateTask = project.getTasks()
            .register("generateClusterFeaturesMetadata", ClusterFeaturesMetadataTask.class, task -> {
                task.setClasspath(
                    featureMetadataExtractorConfig.plus(mainSourceSet.getRuntimeClasspath())
                        .plus(project.getConfigurations().getByName(CompileOnlyResolvePlugin.RESOLVEABLE_COMPILE_ONLY_CONFIGURATION_NAME))
                );
                task.getOutputFile().convention(project.getLayout().getBuildDirectory().file(CLUSTER_FEATURES_JSON));
            });

        Configuration featuresMetadataArtifactConfig = project.getConfigurations().create(FEATURES_METADATA_CONFIGURATION, c -> {
            c.setCanBeResolved(false);
            c.setCanBeConsumed(true);
            c.attributes(a -> { a.attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, FEATURES_METADATA_TYPE); });
        });

        project.getArtifacts().add(featuresMetadataArtifactConfig.getName(), generateTask);
    }
}
