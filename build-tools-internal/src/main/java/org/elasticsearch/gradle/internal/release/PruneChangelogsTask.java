/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import com.google.common.annotations.VisibleForTesting;

import org.elasticsearch.gradle.VersionProperties;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.file.FileCollection;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;

import java.io.File;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

/**
 * Once a minor release has happened, we no longer need to keep the changelog files that went into
 * that release in the development branch for that major series or the branch for the next major
 * series
 * <p>
 * This last examines the git history in order to work out which files can be deleted, and
 * does the deletion after confirming with the user.
 */
public class PruneChangelogsTask extends DefaultTask {
    private static final Logger LOGGER = Logging.getLogger(PruneChangelogsTask.class);

    private FileCollection changelogs;
    private final GitWrapper gitWrapper;
    private final Path rootDir;

    @Inject
    public PruneChangelogsTask(Project project, ObjectFactory objectFactory, ExecOperations execOperations) {
        changelogs = objectFactory.fileCollection();
        gitWrapper = new GitWrapper(execOperations);
        rootDir = project.getRootDir().toPath();
    }

    @Internal
    public FileCollection getChangelogs() {
        return changelogs;
    }

    public void setChangelogs(FileCollection files) {
        this.changelogs = files;
    }

    @TaskAction
    public void executeTask() {
        findAndDeleteFiles(
            this.gitWrapper,
            files -> files.stream().filter(each -> each.delete() == false).collect(Collectors.toSet()),
            QualifiedVersion.of(VersionProperties.getElasticsearch()),
            this.getChangelogs().getFiles(),
            this.rootDir
        );
    }

    @VisibleForTesting
    static void findAndDeleteFiles(
        GitWrapper gitWrapper,
        DeleteHelper deleteHelper,
        QualifiedVersion version,
        Set<File> allFilesInCheckout,
        Path rootDir
    ) {
        if (allFilesInCheckout.isEmpty()) {
            LOGGER.warn("No changelog files in checkout, so nothing to delete.");
            return;
        }

        final Set<String> earlierFiles = findAllFilesInEarlierVersions(gitWrapper, version);

        if (earlierFiles.isEmpty()) {
            LOGGER.warn("No files need to be deleted.");
            return;
        }

        final Set<File> filesToDelete = allFilesInCheckout.stream()
            .filter(each -> earlierFiles.contains(each.getName()))
            .collect(Collectors.toCollection(TreeSet::new));

        if (filesToDelete.isEmpty()) {
            LOGGER.warn("No files need to be deleted.");
            return;
        }

        LOGGER.warn("The following changelog files will be deleted:");
        LOGGER.warn("");
        filesToDelete.forEach(file -> LOGGER.warn("\t{}", rootDir.relativize(file.toPath())));

        final Set<File> failedToDelete = deleteHelper.deleteFiles(filesToDelete);

        if (failedToDelete.isEmpty() == false) {
            throw new GradleException(
                "Failed to delete some files:\n\n"
                    + failedToDelete.stream().map(file -> "\t" + rootDir.relativize(file.toPath())).collect(Collectors.joining("\n"))
                    + "\n"
            );
        }
    }

    /**
     * Find the releases prior to the supplied version, and find the changelog files in those releases by inspecting the
     * git trees at each tag.
     * <p>
     * If the supplied version is the very first in a new major series, then the method will look tag in the previous
     * major series. Otherwise, all git tags in the current major series will be inspected.
     *
     * @param gitWrapper used for git operations
     * @param version the git history is inspected relative to this version
     * @return filenames for changelog files in previous releases, without any path
     */
    private static Set<String> findAllFilesInEarlierVersions(GitWrapper gitWrapper, QualifiedVersion version) {
        return findPreviousVersion(gitWrapper, version).flatMap(
            earlierVersion -> gitWrapper.listFiles("v" + earlierVersion, "docs/changelog")
        ).map(line -> Path.of(line).getFileName().toString()).collect(Collectors.toSet());
    }

    /**
     * Find the releases prior to the supplied version. The current major and the previous major are both
     * listed, since changes may be backported to the prior major e.g. in the event of a series bug
     * or security problem.
     *
     * @param gitWrapper used for git operations
     * @param version the git tags are inspected relative to this version
     * @return a stream of earlier versions
     */
    @VisibleForTesting
    static Stream<QualifiedVersion> findPreviousVersion(GitWrapper gitWrapper, QualifiedVersion version) {
        final String currentMajorPattern = "v" + version.major() + ".*";
        final String previousMajorPattern = "v" + (version.major() - 1) + ".*";

        return Stream.concat(gitWrapper.listVersions(currentMajorPattern), gitWrapper.listVersions(previousMajorPattern))
            .filter(v -> v.isBefore(version));
    }

    /**
     * Used to make it possible to mock destructive operations in tests.
     */
    @VisibleForTesting
    @FunctionalInterface
    interface DeleteHelper {
        Set<File> deleteFiles(Set<File> filesToDelete);
    }
}
