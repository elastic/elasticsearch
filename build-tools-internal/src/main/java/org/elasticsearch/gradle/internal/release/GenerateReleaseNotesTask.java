/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.release;

import com.google.common.annotations.VisibleForTesting;

import org.elasticsearch.gradle.VersionProperties;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.Directory;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.RegularFile;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

/**
 * Orchestrates the steps required to generate or update various release notes files.
 */
public class GenerateReleaseNotesTask extends DefaultTask {
    private static final Logger LOGGER = Logging.getLogger(GenerateReleaseNotesTask.class);

    private final RegularFileProperty releaseNotesTemplate;
    private final RegularFileProperty releaseHighlightsTemplate;
    private final RegularFileProperty breakingChangesTemplate;
    private final RegularFileProperty deprecationsTemplate;

    private final RegularFileProperty releaseNotesFile;
    private final RegularFileProperty releaseHighlightsFile;
    private final RegularFileProperty breakingChangesFile;
    private final RegularFileProperty deprecationsFile;

    private final DirectoryProperty changelogBundleDirectory;

    private final GitWrapper gitWrapper;

    @Inject
    public GenerateReleaseNotesTask(ObjectFactory objectFactory, ExecOperations execOperations) {
        releaseNotesTemplate = objectFactory.fileProperty();
        releaseHighlightsTemplate = objectFactory.fileProperty();
        breakingChangesTemplate = objectFactory.fileProperty();
        deprecationsTemplate = objectFactory.fileProperty();

        releaseNotesFile = objectFactory.fileProperty();
        releaseHighlightsFile = objectFactory.fileProperty();
        breakingChangesFile = objectFactory.fileProperty();
        deprecationsFile = objectFactory.fileProperty();

        changelogBundleDirectory = objectFactory.directoryProperty();

        gitWrapper = new GitWrapper(execOperations);
    }

    @TaskAction
    public void executeTask() throws IOException {
        final String currentVersion = VersionProperties.getElasticsearch();

        if (needsGitTags(currentVersion)) {
            findAndUpdateUpstreamRemote(gitWrapper);
        }

        LOGGER.info("Finding changelog bundles...");

        List<ChangelogBundle> allBundles = this.changelogBundleDirectory.getAsFileTree()
            .getFiles()
            .stream()
            .map(ChangelogBundle::parse)
            .toList();

        var bundles = getSortedBundlesWithUniqueChangelogs(allBundles);

        LOGGER.info("Generating release notes...");
        ReleaseNotesGenerator.update(this.releaseNotesTemplate.get().getAsFile(), this.releaseNotesFile.get().getAsFile(), bundles);
        ReleaseNotesGenerator.update(this.breakingChangesTemplate.get().getAsFile(), this.breakingChangesFile.get().getAsFile(), bundles);
        ReleaseNotesGenerator.update(this.deprecationsTemplate.get().getAsFile(), this.deprecationsFile.get().getAsFile(), bundles);
    }

    @VisibleForTesting
    static List<ChangelogBundle> getSortedBundlesWithUniqueChangelogs(List<ChangelogBundle> bundles) {
        List<ChangelogBundle> sorted = bundles.stream()
            .sorted(Comparator.comparing(ChangelogBundle::released).reversed().thenComparing(ChangelogBundle::generated))
            .toList();

        // Ensure that each changelog/PR only shows up once, in its earliest release
        var uniquePrs = new HashSet<Integer>();
        List<ChangelogBundle> modifiedBundles = new ArrayList<>();
        for (int i = sorted.size() - 1; i >= 0; i--) {
            var bundle = sorted.get(i);
            if (bundle.released() == false) {
                List<ChangelogEntry> entries = bundle.changelogs().stream().filter(c -> false == uniquePrs.contains(c.getPr())).toList();
                modifiedBundles.add(bundle.withChangelogs(entries));
            } else {
                modifiedBundles.add(bundle);
            }
            uniquePrs.addAll(bundle.changelogs().stream().map(ChangelogEntry::getPr).toList());
        }

        return modifiedBundles;
    }

    /**
     * Ensure the upstream git remote is up-to-date. The upstream is whatever git remote references `elastic/elasticsearch`.
     * @param gitWrapper used to call `git`
     */
    private static void findAndUpdateUpstreamRemote(GitWrapper gitWrapper) {
        LOGGER.info("Finding upstream git remote");
        // We need to ensure the tags are up-to-date. Find the correct remote to use
        String upstream = gitWrapper.listRemotes()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().contains("elastic/elasticsearch"))
            .findFirst()
            .map(Map.Entry::getKey)
            .orElseThrow(
                () -> new GradleException(
                    "I need to ensure the git tags are up-to-date, but I couldn't find a git remote for [elastic/elasticsearch]"
                )
            );

        LOGGER.info("Updating remote [{}]", upstream);
        // Now update the remote, and make sure we update the tags too
        gitWrapper.updateRemote(upstream);

        LOGGER.info("Updating tags from [{}]", upstream);
        gitWrapper.updateTags(upstream);
    }

    /**
     * This methods checks the supplied version and answers {@code false} if the fetching of git
     * tags can be skipped, or {@code true} otherwise.
     * <p>
     * The first version in a minor series will never have any preceding versions, so there's no
     * need to fetch tags and examine the repository state in the past. This applies when the
     * version is a release version, a snapshot, or the first alpha version. Subsequent alphas,
     * betas and release candidates need to check the previous prelease tags.
     *
     * @param versionString the version string to check
     * @return whether fetching git tags is required
     */
    @VisibleForTesting
    static boolean needsGitTags(String versionString) {
        if (versionString.endsWith(".0") || versionString.endsWith(".0-SNAPSHOT") || versionString.endsWith(".0-alpha1")) {
            return false;
        }

        return true;
    }

    @InputDirectory
    public DirectoryProperty getChangelogBundleDirectory() {
        return changelogBundleDirectory;
    }

    public void setChangelogBundleDirectory(Directory dir) {
        this.changelogBundleDirectory.set(dir);
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

    @InputFile
    public RegularFileProperty getDeprecationsTemplate() {
        return deprecationsTemplate;
    }

    public void setDeprecationsTemplate(RegularFile file) {
        this.deprecationsTemplate.set(file);
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

    @OutputFile
    public RegularFileProperty getDeprecationsFile() {
        return deprecationsFile;
    }

    public void setDeprecationsFile(RegularFile file) {
        this.deprecationsFile.set(file);
    }
}
