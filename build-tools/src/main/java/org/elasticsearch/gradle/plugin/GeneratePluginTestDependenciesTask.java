/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.plugin;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;

import javax.inject.Inject;

public abstract class GeneratePluginTestDependenciesTask extends DefaultTask {

    public static final String DESCRIPTION = "generates plugin test dependencies file";
    private static final Logger LOGGER = Logging.getLogger(GeneratePluginTestDependenciesTask.class);

    @Inject
    public GeneratePluginTestDependenciesTask(ProjectLayout projectLayout) {
        setDescription(DESCRIPTION);
    }

    @TaskAction
    public void generatePropertiesFile() throws IOException {
        LOGGER.lifecycle("HELLO!");
    }
}
