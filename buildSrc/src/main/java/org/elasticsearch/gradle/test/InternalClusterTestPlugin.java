/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.test;

import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.SourceSet;

public class InternalClusterTestPlugin implements Plugin<Project> {

    public static final String SOURCE_SET_NAME = "internalClusterTest";

    @Override
    public void apply(Project project) {
        GradleUtils.addTestSourceSet(project, SOURCE_SET_NAME);

        // TODO: fix usages of IT tests depending on Tests methods so this extension is not necessary
        GradleUtils.extendSourceSet(project, SourceSet.TEST_SOURCE_SET_NAME, SOURCE_SET_NAME);

        // add alias task that is easier to type
        project.getTasks().register("icTest").configure(alias -> alias.dependsOn(SOURCE_SET_NAME));
    }
}
