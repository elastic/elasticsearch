/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitTaskPlugin;
import org.elasticsearch.gradle.internal.precommit.ValidateYamlAgainstSchemaTask;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;
import org.gradle.api.file.FileTree;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.util.PatternSet;

import java.io.File;
import javax.inject.Inject;

/**
 * This plugin defines tasks related to releasing Elasticsearch.
 */
public class ReleaseToolsPlugin implements Plugin<Project> {

    private static final String RESOURCES = "build-tools-internal/src/main/resources/";

    private final ProjectLayout projectLayout;

    @Inject
    public ReleaseToolsPlugin(ProjectLayout projectLayout) {
        this.projectLayout = projectLayout;
    }

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(PrecommitTaskPlugin.class);
        final Directory projectDirectory = projectLayout.getProjectDirectory();

        final Version version = VersionProperties.getElasticsearchVersion();

        final FileTree yamlFiles = projectDirectory.dir("docs/changelog")
            .getAsFileTree()
            .matching(new PatternSet().include("**/*.yml", "**/*.yaml"));

        final Provider<ValidateYamlAgainstSchemaTask> validateChangelogsAgainstYamlTask = project.getTasks()
            .register("validateChangelogsAgainstSchema", ValidateYamlAgainstSchemaTask.class, task -> {
                task.setGroup("Documentation");
                task.setDescription("Validate that the changelog YAML files comply with the changelog schema");
                task.setInputFiles(yamlFiles);
                task.setJsonSchema(new File(project.getRootDir(), RESOURCES + "changelog-schema.json"));
                task.setReport(new File(project.getBuildDir(), "reports/validateYaml.txt"));
            });

        final TaskProvider<ValidateChangelogEntryTask> validateChangelogsTask = project.getTasks()
            .register("validateChangelogs", ValidateChangelogEntryTask.class, task -> {
                task.setGroup("Documentation");
                task.setDescription("Validate that all changelog YAML files are well-formed");
                task.setChangelogs(yamlFiles);
                task.dependsOn(validateChangelogsAgainstYamlTask);
            });

        project.getTasks().register("generateReleaseNotes", GenerateReleaseNotesTask.class).configure(task -> {
            task.setGroup("Documentation");
            task.setDescription("Generates release notes from changelog files held in this checkout");
            task.setChangelogs(yamlFiles);

            task.setReleaseNotesIndexTemplate(projectDirectory.file(RESOURCES + "templates/release-notes-index.asciidoc"));
            task.setReleaseNotesIndexFile(projectDirectory.file("docs/reference/release-notes.asciidoc"));

            task.setReleaseNotesTemplate(projectDirectory.file(RESOURCES + "templates/release-notes.asciidoc"));
            task.setReleaseNotesFile(
                projectDirectory.file(String.format("docs/reference/release-notes/%s.asciidoc", VersionProperties.getElasticsearch()))
            );

            task.setReleaseHighlightsTemplate(projectDirectory.file(RESOURCES + "templates/release-highlights.asciidoc"));
            task.setReleaseHighlightsFile(projectDirectory.file("docs/reference/release-notes/highlights.asciidoc"));

            task.setBreakingChangesTemplate(projectDirectory.file(RESOURCES + "templates/breaking-changes.asciidoc"));
            task.setBreakingChangesFile(
                projectDirectory.file(
                    String.format("docs/reference/migration/migrate_%d_%d.asciidoc", version.getMajor(), version.getMinor())
                )
            );

            task.dependsOn(validateChangelogsTask);
        });

        project.getTasks().register("compileReleaseNotes", CompileReleaseNotesTask.class).configure(task -> {
            final FileTree releaseNotesFiles = projectDirectory.dir("docs/reference/release-notes")
                .getAsFileTree()
                .matching(new PatternSet().include(version.getMajor() + ".*.*.asciidoc"));

            task.setInputFiles(releaseNotesFiles);
            task.setOutputFile(
                projectDirectory.file("docs/reference/release-notes/%d.%d.asciidoc".formatted(version.getMajor(), version.getMinor()))
            );
        });

        project.getTasks().named("precommit").configure(task -> task.dependsOn(validateChangelogsTask));
    }
}
