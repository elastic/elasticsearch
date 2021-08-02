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
import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.RegularFile;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Orchestrates the steps required to generate or update various release notes files.
 */
public class GenerateReleaseNotesTask extends DefaultTask {
    private static final Logger LOGGER = Logging.getLogger(GenerateReleaseNotesTask.class);

    private final ConfigurableFileCollection changelogs;

    private final RegularFileProperty releaseNotesIndexTemplate;
    private final RegularFileProperty releaseNotesTemplate;
    private final RegularFileProperty releaseHighlightsTemplate;
    private final RegularFileProperty breakingChangesTemplate;

    private final RegularFileProperty releaseNotesIndexFile;
    private final RegularFileProperty releaseNotesFile;
    private final RegularFileProperty releaseHighlightsFile;
    private final RegularFileProperty breakingChangesFile;

    @Inject
    public GenerateReleaseNotesTask(ObjectFactory objectFactory) {
        changelogs = objectFactory.fileCollection();

        releaseNotesIndexTemplate = objectFactory.fileProperty();
        releaseNotesTemplate = objectFactory.fileProperty();
        releaseHighlightsTemplate = objectFactory.fileProperty();
        breakingChangesTemplate = objectFactory.fileProperty();

        releaseNotesIndexFile = objectFactory.fileProperty();
        releaseNotesFile = objectFactory.fileProperty();
        releaseHighlightsFile = objectFactory.fileProperty();
        breakingChangesFile = objectFactory.fileProperty();
    }

    @TaskAction
    public void executeTask() throws IOException {
        LOGGER.info("Finding changelog files...");

        final Version checkoutVersion = VersionProperties.getElasticsearchVersion();

        final List<ChangelogEntry> entries = this.changelogs.getFiles()
            .stream()
            .map(ChangelogEntry::parse)
            .filter(
                // Only process changelogs that are included in this minor version series of ES.
                // If this change was released in an earlier major or minor version of Elasticsearch, do not
                // include it in the notes. An earlier patch version is OK, the release notes include changes
                // for every patch release in a minor series.
                log -> {
                    final List<Version> versionsForChangelogFile = log.getVersions()
                        .stream()
                        .map(v -> Version.fromString(v, Version.Mode.RELAXED))
                        .collect(Collectors.toList());

                    final Predicate<Version> includedInSameMinor = v -> v.getMajor() == checkoutVersion.getMajor()
                        && v.getMinor() == checkoutVersion.getMinor();

                    final Predicate<Version> includedInEarlierMajorOrMinor = v -> v.getMajor() < checkoutVersion.getMajor()
                        || (v.getMajor() == checkoutVersion.getMajor() && v.getMinor() < checkoutVersion.getMinor());

                    boolean includedInThisMinor = versionsForChangelogFile.stream().anyMatch(includedInSameMinor);

                    if (includedInThisMinor) {
                        return versionsForChangelogFile.stream().noneMatch(includedInEarlierMajorOrMinor);
                    } else {
                        return false;
                    }
                }
            )
            .collect(Collectors.toList());

        LOGGER.info("Updating release notes index...");
        ReleaseNotesIndexUpdater.update(this.releaseNotesIndexTemplate.get().getAsFile(), this.releaseNotesIndexFile.get().getAsFile());

        LOGGER.info("Generating release notes...");
        ReleaseNotesGenerator.update(this.releaseNotesTemplate.get().getAsFile(), this.releaseNotesFile.get().getAsFile(), entries);

        LOGGER.info("Generating release highlights...");
        ReleaseHighlightsGenerator.update(this.releaseHighlightsTemplate.get().getAsFile(), this.releaseHighlightsFile.get().getAsFile(), entries);

        LOGGER.info("Generating breaking changes / deprecations notes...");
        BreakingChangesGenerator.update(this.breakingChangesTemplate.get().getAsFile(), this.breakingChangesFile.get().getAsFile(), entries);
    }

    @InputFiles
    public FileCollection getChangelogs() {
        return changelogs;
    }

    public void setChangelogs(FileCollection files) {
        this.changelogs.setFrom(files);
    }

    @InputFile
    public RegularFileProperty getReleaseNotesIndexTemplate() {
        return releaseNotesIndexTemplate;
    }

    public void setReleaseNotesIndexTemplate(RegularFile file) {
        this.releaseNotesIndexTemplate.set(file);
    }

    @InputFile
    public RegularFileProperty getReleaseNotesTemplate() {
        return releaseNotesTemplate;
    }

    public void setReleaseNotesTemplate(RegularFile file) {
        this.releaseNotesTemplate.set(file);
    }

    @InputFile
    public RegularFileProperty getReleaseHighlightsTemplate() {
        return releaseHighlightsTemplate;
    }

    public void setReleaseHighlightsTemplate(RegularFile file) {
        this.releaseHighlightsTemplate.set(file);
    }

    @InputFile
    public RegularFileProperty getBreakingChangesTemplate() {
        return breakingChangesTemplate;
    }

    public void setBreakingChangesTemplate(RegularFile file) {
        this.breakingChangesTemplate.set(file);
    }

    @OutputFile
    public RegularFileProperty getReleaseNotesIndexFile() {
        return releaseNotesIndexFile;
    }

    public void setReleaseNotesIndexFile(RegularFile file) {
        this.releaseNotesIndexFile.set(file);
    }

    @OutputFile
    public RegularFileProperty getReleaseNotesFile() {
        return releaseNotesFile;
    }

    public void setReleaseNotesFile(RegularFile file) {
        this.releaseNotesFile.set(file);
    }

    @OutputFile
    public RegularFileProperty getReleaseHighlightsFile() {
        return releaseHighlightsFile;
    }

    public void setReleaseHighlightsFile(RegularFile file) {
        this.releaseHighlightsFile.set(file);
    }

    @OutputFile
    public RegularFileProperty getBreakingChangesFile() {
        return breakingChangesFile;
    }

    public void setBreakingChangesFile(RegularFile file) {
        this.breakingChangesFile.set(file);
    }
}
