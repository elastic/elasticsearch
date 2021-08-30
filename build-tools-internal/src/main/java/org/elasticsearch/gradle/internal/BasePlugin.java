/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.VersionProperties;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

public class BasePlugin implements Plugin<Project> {
    @Override
    public void apply(Project project) {
        configureProjectProperties(project);

    }

    private void configureProjectProperties(Project project) {
        project.setGroup("org.elasticsearch");
        project.setVersion(VersionProperties.getElasticsearch());
        project.setDescription("Elasticsearch subproject " + project.getPath());
    }
}
