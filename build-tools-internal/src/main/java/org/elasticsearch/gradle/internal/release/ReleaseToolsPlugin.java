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
import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;
import org.gradle.api.file.FileTree;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.util.PatternSet;

import java.io.File;
import java.util.function.Function;

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

        project.getTasks()
            .register("updateVersions", UpdateVersionsTask.class, t -> project.getTasks().named("spotlessApply").get().mustRunAfter(t));

        project.getTasks().register("extractCurrentVersions", ExtractCurrentVersionsTask.class);
        project.getTasks().register("tagVersions", TagVersionsTask.class);

        final FileTree yamlFiles = projectDirectory.dir("docs/changelog")
            .getAsFileTree()
            .matching(new PatternSet().include("**/*.yml", "**/*.yaml"));

        final Provider<ValidateYamlAgainstSchemaTask> validateChangelogsTask = project.getTasks()
            .register("validateChangelogs", ValidateYamlAgainstSchemaTask.class, task -> {
                task.setGroup("Documentation");
                task.setDescription("Validate that the changelog YAML files comply with the changelog schema");
                task.setInputFiles(yamlFiles);
                task.setJsonSchema(new File(project.getRootDir(), RESOURCES + "changelog-schema.json"));
                task.setReport(new File(project.getBuildDir(), "reports/validateYaml.txt"));
            });

        final Function<Boolean, Action<GenerateReleaseNotesTask>> configureGenerateTask = shouldConfigureYamlFiles -> task -> {
            task.setGroup("Documentation");
            if (shouldConfigureYamlFiles) {
                task.setChangelogs(yamlFiles);
                task.setDescription("Generates release notes from changelog files held in this checkout");
            } else {
                task.setDescription("Generates stub release notes e.g. after feature freeze");
            }

            task.setReleaseNotesIndexTemplate(projectDirectory.file(RESOURCES + "templates/release-notes-index.asciidoc"));
            task.setReleaseNotesIndexFile(projectDirectory.file("docs/reference/release-notes.asciidoc"));

            task.setReleaseNotesTemplate(projectDirectory.file(RESOURCES + "templates/release-notes.asciidoc"));
            task.setReleaseNotesFile(
                projectDirectory.file(
                    String.format(
                        "docs/reference/release-notes/%d.%d.%d.asciidoc",
                        version.getMajor(),
                        version.getMinor(),
                        version.getRevision()
                    )
                )
            );

            task.setReleaseHighlightsTemplate(projectDirectory.file(RESOURCES + "templates/release-highlights.asciidoc"));
            task.setReleaseHighlightsFile(projectDirectory.file("docs/reference/release-notes/highlights.asciidoc"));

            task.setBreakingChangesTemplate(projectDirectory.file(RESOURCES + "templates/breaking-changes.asciidoc"));
            task.setBreakingChangesMigrationFile(
                projectDirectory.file(
                    String.format("docs/reference/migration/migrate_%d_%d.asciidoc", version.getMajor(), version.getMinor())
                )
            );
            task.setMigrationIndexTemplate(projectDirectory.file(RESOURCES + "templates/migration-index.asciidoc"));
            task.setMigrationIndexFile(projectDirectory.file("docs/reference/migration/index.asciidoc"));

            task.dependsOn(validateChangelogsTask);
        };

        project.getTasks().register("generateReleaseNotes", GenerateReleaseNotesTask.class).configure(configureGenerateTask.apply(true));
        project.getTasks()
            .register("generateStubReleaseNotes", GenerateReleaseNotesTask.class)
            .configure(configureGenerateTask.apply(false));

        project.getTasks().register("pruneChangelogs", PruneChangelogsTask.class).configure(task -> {
            task.setGroup("Documentation");
            task.setDescription("Removes changelog files that have been used in a previous release");
            task.setChangelogs(yamlFiles);
        });

        project.getTasks().named("precommit").configure(task -> task.dependsOn(validateChangelogsTask));
    }
}
