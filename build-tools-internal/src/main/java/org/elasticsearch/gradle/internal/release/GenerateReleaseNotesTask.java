/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

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
import org.gradle.internal.logging.text.StyledTextOutput;
import org.gradle.internal.logging.text.StyledTextOutputFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;

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

    private final StyledTextOutput errorOutput;

    @Inject
    public GenerateReleaseNotesTask(ObjectFactory objectFactory, StyledTextOutputFactory styledTextOutputFactory) {
        changelogs = objectFactory.fileCollection();

        releaseNotesIndexTemplate = objectFactory.fileProperty();
        releaseNotesTemplate = objectFactory.fileProperty();
        releaseHighlightsTemplate = objectFactory.fileProperty();
        breakingChangesTemplate = objectFactory.fileProperty();

        releaseNotesIndexFile = objectFactory.fileProperty();
        releaseNotesFile = objectFactory.fileProperty();
        releaseHighlightsFile = objectFactory.fileProperty();
        breakingChangesFile = objectFactory.fileProperty();

        // It's questionable to be using Gradle internals for printing in color, but there's no official API for it,
        // and we need to draw some things to the user's attention.
        errorOutput = styledTextOutputFactory.create("release-notes");
    }

    @TaskAction
    public void executeTask() throws IOException {
        LOGGER.info("Finding changelog files...");

        final List<ChangelogEntry> entries = this.changelogs.getFiles().stream().map(ChangelogEntry::parse).collect(Collectors.toList());

        LOGGER.info("Updating release notes index...");
        ReleaseNotesIndexUpdater.update(this.releaseNotesIndexTemplate.get().getAsFile(), this.releaseNotesIndexFile.get().getAsFile());

        LOGGER.info("Generating release notes...");
        ReleaseNotesGenerator.update(this.releaseNotesTemplate.get().getAsFile(), this.releaseNotesFile.get().getAsFile(), entries);

        if (VersionProperties.getElasticsearchVersion().getRevision() > 0) {
            if (entries.stream().anyMatch(e -> e.getHighlight() != null)) {
                String message = ("WARNING: There are YAML files with release highlights, but %s is not the "
                    + "first version in the minor series. If this is actually correct, please update %s manually.%n").formatted(
                    VersionProperties.getElasticsearchVersion(),
                    this.breakingChangesFile.get().getAsFile()
                );
                this.errorOutput.style(StyledTextOutput.Style.Failure).text(message);
            }
        } else {
            LOGGER.info("Generating release highlights...");
            ReleaseHighlightsGenerator.update(
                this.releaseHighlightsTemplate.get().getAsFile(),
                this.releaseHighlightsFile.get().getAsFile(),
                entries
            );
        }

        if (VersionProperties.getElasticsearchVersion().getRevision() > 0) {
            if (entries.stream().anyMatch(e -> e.getBreaking() != null || e.getDeprecation() != null)) {
                String message = ("WARNING: There are YAML files with breaking changes or deprecations, but %s is not the "
                    + "first version in the minor series. If this is actually correct, please update %s manually.%n").formatted(
                        VersionProperties.getElasticsearchVersion(),
                        this.breakingChangesFile.get().getAsFile()
                    );
                this.errorOutput.style(StyledTextOutput.Style.Failure).text(message);
            }
        } else {
            LOGGER.info("Generating breaking changes / deprecations notes...");
            BreakingChangesGenerator.update(
                this.breakingChangesTemplate.get().getAsFile(),
                this.breakingChangesFile.get().getAsFile(),
                entries
            );
        }
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
