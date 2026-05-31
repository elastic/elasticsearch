/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.release;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

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
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;

import javax.inject.Inject;

/**
 * This plugin defines tasks related to releasing Elasticsearch.
 */
public class ReleaseToolsPlugin implements Plugin<Project> {

    private static final String RESOURCES = "build-tools-internal/src/main/resources/";
    private static final String EXTERNAL_SOURCES_CONFIG = "external-changelog-sources.yml";

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
        project.getTasks().register("updateBranchesJson", UpdateBranchesJsonTask.class);

        final Directory changeLogDirectory = projectDirectory.dir("docs/changelog");
        final Directory changeLogBundlesDirectory = projectDirectory.dir("docs/release-notes/changelog-bundles");
        final FileTree yamlFiles = changeLogDirectory.getAsFileTree().matching(new PatternSet().include("**/*.yml", "**/*.yaml"));

        final Provider<ValidateYamlAgainstSchemaTask> validateChangelogsTask = project.getTasks()
            .register("validateChangelogs", ValidateYamlAgainstSchemaTask.class, task -> {
                task.setGroup("Documentation");
                task.setDescription("Validate that the changelog YAML files comply with the changelog schema");
                task.setInputFiles(yamlFiles);
                task.setJsonSchema(new File(project.getRootDir(), RESOURCES + "changelog-schema.json"));
                task.setReport(new File(project.getBuildDir(), "reports/validateYaml.txt"));
            });

        final Action<BundleChangelogsTask> configureBundleTask = task -> {
            task.setGroup("Documentation");
            task.setDescription("Generates release notes from changelog files held in this checkout and external repos");
            task.setChangelogs(yamlFiles);
            task.setChangelogDirectory(changeLogDirectory);
            task.setChangelogBundlesDirectory(changeLogBundlesDirectory);
            task.setBundleFile(projectDirectory.file("docs/release-notes/changelogs-" + version.toString() + ".yml"));
            task.setExternalSources(loadExternalSources());
            task.getOutputs().upToDateWhen(o -> false);
        };

        final Action<GenerateReleaseNotesTask> configureGenerateTask = task -> {
            task.setGroup("Documentation");
            task.setDescription("Generates release notes for all versions/branches using changelog bundles in this checkout");

            task.setReleaseNotesTemplate(projectDirectory.file(RESOURCES + "templates/index.md"));
            task.setReleaseNotesFile(projectDirectory.file("docs/release-notes/index.md"));

            task.setReleaseHighlightsTemplate(projectDirectory.file(RESOURCES + "templates/release-highlights.asciidoc"));
            task.setReleaseHighlightsFile(projectDirectory.file("docs/reference/release-notes/highlights.asciidoc"));

            task.setBreakingChangesTemplate(projectDirectory.file(RESOURCES + "templates/breaking-changes.md"));
            task.setBreakingChangesFile(projectDirectory.file("docs/release-notes/breaking-changes.md"));

            task.setDeprecationsTemplate(projectDirectory.file(RESOURCES + "templates/deprecations.md"));
            task.setDeprecationsFile(projectDirectory.file("docs/release-notes/deprecations.md"));

            task.setChangelogBundleDirectory(changeLogBundlesDirectory);

            task.getOutputs().upToDateWhen(o -> false);

            task.dependsOn(validateChangelogsTask);
        };

        project.getTasks().register("bundleChangelogs", BundleChangelogsTask.class).configure(configureBundleTask);
        project.getTasks().register("generateReleaseNotes", GenerateReleaseNotesTask.class).configure(configureGenerateTask);

        project.getTasks().register("pruneChangelogs", PruneChangelogsTask.class).configure(task -> {
            task.setGroup("Documentation");
            task.setDescription("Removes changelog files that have been used in a previous release");
            task.setChangelogs(yamlFiles);
        });

        project.getTasks().named("precommit").configure(task -> task.dependsOn(validateChangelogsTask));
    }

    private static List<BundleChangelogsTask.ExternalChangelogSource> loadExternalSources() {
        try (InputStream is = ReleaseToolsPlugin.class.getClassLoader().getResourceAsStream(EXTERNAL_SOURCES_CONFIG)) {
            if (is == null) {
                return List.of();
            }
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            return mapper.readValue(is, new TypeReference<>() {});
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load " + EXTERNAL_SOURCES_CONFIG, e);
        }
    }
}
