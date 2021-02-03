/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.release;

import org.gradle.api.Plugin;
import org.gradle.api.Project;

public class ReleaseToolsPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getTasks().register("generateReleaseNotes", GenerateReleaseNotesTask.class).configure(action -> {
            action.setGroup("Documentation");
            action.setDescription("Generates release notes from changelog files held in this checkout");
        });

        project.getTasks().register("validateChangelogs", ValidateChangelogsTask.class).configure(action -> {
            action.setGroup("Documentation");
            action.setDescription("Validates that all the changelog YAML files are well-formed");
        });
    }
}
