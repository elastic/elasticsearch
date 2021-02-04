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
import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.RegularFile;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class GenerateReleaseNotesTask extends DefaultTask {
    private static final Logger LOGGER = Logging.getLogger(GenerateReleaseNotesTask.class);

    private final ConfigurableFileCollection changelogs = getProject().getObjects().fileCollection();
    private final RegularFileProperty releaseNotesFile = getProject().getObjects().fileProperty();
    private final RegularFileProperty releaseHighlightsFile = getProject().getObjects().fileProperty();
    private final RegularFileProperty breakingChangesFile = getProject().getObjects().fileProperty();

    @InputFiles
    public FileCollection getChangelogs() {
        return changelogs;
    }

    @OutputFile
    public RegularFileProperty getReleaseNotesFile() {
        return releaseNotesFile;
    }

    @OutputFile
    public RegularFileProperty getReleaseHighlightsFile() {
        return releaseHighlightsFile;
    }

    @OutputFile
    public RegularFileProperty getBreakingChangesFile() {
        return breakingChangesFile;
    }

    @TaskAction
    public void executeTask() throws IOException {
        LOGGER.info("Finding changelog files...");

        final Version checkoutVersion = VersionProperties.getElasticsearchVersion();

        final List<ChangelogEntry> entries = this.changelogs.getFiles()
            .stream()
            .map(ChangelogEntry::parseChangelog)
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
                        return false == versionsForChangelogFile.stream().anyMatch(includedInEarlierMajorOrMinor);
                    } else {
                        return false;
                    }
                }
            )
            .collect(Collectors.toList());

        try (ReleaseNotesGenerator generator = new ReleaseNotesGenerator(this.releaseNotesFile.get().getAsFile())) {
            generator.generate(entries);
        }

        try (ReleaseHighlightsGenerator generator = new ReleaseHighlightsGenerator(this.releaseHighlightsFile.get().getAsFile())) {
            generator.generate(entries);
        }

        try (BreakingChangesGenerator generator = new BreakingChangesGenerator(this.breakingChangesFile.get().getAsFile())) {
            generator.generate(entries);
        }
    }

    public void setChangelogs(Set<File> files) {
        this.changelogs.setFrom(files);
    }

    public void setReleaseNotesFile(RegularFile file) {
        this.releaseNotesFile.set(file);
    }

    public void setReleaseHighlightsFile(RegularFile file) {
        this.releaseHighlightsFile.set(file);
    }

    public void setBreakingChangesFile(RegularFile file) {
        this.breakingChangesFile.set(file);
    }
}
