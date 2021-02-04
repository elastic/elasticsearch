/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.release;

import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.util.PatternSet;

public class ReleaseToolsPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        final Version version = VersionProperties.getElasticsearchVersion();

        project.getTasks().register("generateReleaseNotes", GenerateReleaseNotesTask.class).configure(action -> {
            action.setGroup("Documentation");
            action.setDescription("Generates release notes from changelog files held in this checkout");

            action.setChangelogs(project.getLayout()
                .getProjectDirectory()
                .dir("docs/changelog")
                .getAsFileTree()
                .matching(new PatternSet().include("**/*.yml", "**/*.yaml"))
                .getFiles());

            action.setReleaseNotesFile(
                project.getLayout()
                    .getProjectDirectory()
                    .file(String.format("docs/reference/release-notes/%d.%d.asciidoc", version.getMajor(), version.getMinor()))
            );

            action.setReleaseHighlightsFile(
                project.getLayout().getProjectDirectory().file("docs/reference/release-notes/highlights.asciidoc")
            );

            action.setBreakingChangesFile(
                project.getLayout()
                    .getProjectDirectory()
                    .file(String.format("docs/reference/migration/migrate_%d_%d.asciidoc", version.getMajor(), version.getMinor()))
            );
        });

        project.getTasks().register("validateChangelogs", ValidateChangelogsTask.class).configure(action -> {
            action.setGroup("Documentation");
            action.setDescription("Validates that all the changelog YAML files are well-formed");

            action.setChangelogs(project.getLayout()
                .getProjectDirectory()
                .dir("docs/changelog")
                .getAsFileTree()
                .matching(new PatternSet().include("**/*.yml", "**/*.yaml"))
                .getFiles());
        });
    }
}
