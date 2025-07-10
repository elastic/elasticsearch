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

public class CheckTransportVersionPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        // TODO figure out what the classpath needs to be to be able to scan the server classes
        // Does this need to be a lib (to limit scanning by making this a jar, to exclude gradle)? Ask Mark
        // "/Users/john.verwolf/code/elasticsearch/build-tools-internal/build/classes/java/main"

        final var checkTransportVersion = project.getTasks().register("checkTransportVersion", CheckTransportVersionTask.class, t -> {
            SourceSet mainSourceSet = GradleUtils.getJavaSourceSets(project).findByName(SourceSet.MAIN_SOURCE_SET_NAME);
            FileCollection compiledPluginClasses = mainSourceSet.getOutput().getClassesDirs();
            t.getClassDirs().set(compiledPluginClasses);

            t.getOutputFile().set(project.getLayout().getBuildDirectory().file("generated-transport-info/transport-version-set-names.txt"));
        });
    }
}
