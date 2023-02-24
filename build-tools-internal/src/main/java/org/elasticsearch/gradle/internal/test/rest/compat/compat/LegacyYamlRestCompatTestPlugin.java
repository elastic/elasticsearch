/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.compat.compat;

import org.elasticsearch.gradle.internal.test.rest.LegacyYamlRestTestPlugin;
import org.elasticsearch.gradle.internal.test.rest.RestTestUtil;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.internal.file.FileOperations;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;

import javax.inject.Inject;

/**
 * Apply this plugin to run the YAML based REST tests from a prior major version against this version's cluster.
 *
 * @deprecated use {@link YamlRestCompatTestPlugin}
 */
@Deprecated
public class LegacyYamlRestCompatTestPlugin extends AbstractYamlRestCompatTestPlugin {
    @Inject
    public LegacyYamlRestCompatTestPlugin(ProjectLayout projectLayout, FileOperations fileOperations) {
        super(projectLayout, fileOperations);
    }

    @Override
    public TaskProvider<? extends Test> registerTestTask(Project project, SourceSet sourceSet) {
        return RestTestUtil.registerTestTask(project, sourceSet, sourceSet.getTaskName(null, "test"));
    }

    @Override
    public Class<? extends Plugin<Project>> getBasePlugin() {
        return LegacyYamlRestTestPlugin.class;
    }
}
