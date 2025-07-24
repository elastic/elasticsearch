/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.SourceSet;

public class LocateTransportVersionsPlugin implements Plugin<Project> {
    public static final String TRANSPORT_VERSION_NAMES_FILE = "generated-transport-info/transport-version-set-names.txt";

    @Override
    public void apply(Project project) {

        final var checkTransportVersion = project.getTasks().register("locateTransportVersions", LocateTransportVersionsTask.class, t -> {
            t.setGroup("Transport Versions");
            t.setDescription("Collects all TransportVersion names used throughout the project");
            SourceSet mainSourceSet = GradleUtils.getJavaSourceSets(project).findByName(SourceSet.MAIN_SOURCE_SET_NAME);
            FileCollection clasDirs = mainSourceSet.getRuntimeClasspath();
            t.getClassDirs().set(clasDirs);
            t.getOutputFile().set(project.getLayout().getBuildDirectory().file(TRANSPORT_VERSION_NAMES_FILE));
        });

        var config = project.getConfigurations().create("locateTransportVersionsConfig");
        project.getArtifacts().add(config.getName(), checkTransportVersion);
    }
}
