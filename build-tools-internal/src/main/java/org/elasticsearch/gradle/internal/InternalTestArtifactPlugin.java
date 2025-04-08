/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;

/**
 * Ideally, this plugin is intended to be temporary and in the long run we want to move
 * forward to port our test fixtures to use the gradle test fixtures plugin.
 * */
public class InternalTestArtifactPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getPlugins().apply(InternalTestArtifactBasePlugin.class);
        InternalTestArtifactExtension testArtifactExtension = project.getExtensions().getByType(InternalTestArtifactExtension.class);
        project.getExtensions().getByType(SourceSetContainer.class).configureEach(sourceSet -> {
            if (sourceSet.getName().equals(SourceSet.MAIN_SOURCE_SET_NAME) == false) {
                testArtifactExtension.registerTestArtifactFromSourceSet(sourceSet);
            }
        });
    }
}
