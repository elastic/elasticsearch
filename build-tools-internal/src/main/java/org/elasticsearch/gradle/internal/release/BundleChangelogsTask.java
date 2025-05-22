/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.release;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.Directory;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.RegularFile;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;
import org.gradle.process.ExecOperations;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.inject.Inject;

import static java.util.stream.Collectors.toList;

public class BundleChangelogsTask extends DefaultTask {
    private static final Logger LOGGER = Logging.getLogger(BundleChangelogsTask.class);

    private final ConfigurableFileCollection changelogs;

    private final RegularFileProperty bundleFile;
    private final DirectoryProperty changelogDirectory;
    private final DirectoryProperty changelogBundlesDirectory;

    private final GitWrapper gitWrapper;

    @Nullable
    private String branch;
    @Nullable
    private String bcRef;

    private boolean finalize;

    @Option(option = "branch", description = "Branch (or other ref) to use for generating the changelog bundle.")
    public void setBranch(String branch) {
        this.branch = branch;
    }

    @Option(
        option = "bc-ref",
        description = "A source ref, typically the sha of a BC, that should be used to source PRs for changelog entries. "
            + "The actual content of the changelogs will come from the 'branch' ref. "
            + "You should generally always use bc-ref."
    )
    public void setBcRef(String ref) {
        this.bcRef = ref;
    }

    @Option(option = "finalize", description = "Specify that the bundle is finalized, i.e. that the version has been released.")
    public void setFinalize(boolean finalize) {
        this.finalize = finalize;
    }

    private static final ObjectMapper yamlMapper = new ObjectMapper(
        new YAMLFactory().enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
            .disable(YAMLGenerator.Feature.SPLIT_LINES)
            .enable(YAMLGenerator.Feature.INDENT_ARRAYS_WITH_INDICATOR)
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
            .enable(YAMLGenerator.Feature.LITERAL_BLOCK_STYLE)
    ).setSerializationInclusion(JsonInclude.Include.NON_NULL);

    @Inject
    public BundleChangelogsTask(ObjectFactory objectFactory, ExecOperations execOperations) {
        changelogs = objectFactory.fileCollection();

        bundleFile = objectFactory.fileProperty();
        changelogDirectory = objectFactory.directoryProperty();
        changelogBundlesDirectory = objectFactory.directoryProperty();

        gitWrapper = new GitWrapper(execOperations);
    }

    @TaskAction
    public void executeTask() throws IOException {
        if (branch == null) {
            throw new IllegalArgumentException("'branch' not specified.");
        }

        final String upstreamRemote = gitWrapper.getUpstream();
        Set<String> entriesFromBc = Set.of();

        var didCheckoutChangelogs = false;
        try {
            var usingBcRef = bcRef != null && bcRef.isEmpty() == false;
            if (usingBcRef) {
                // Check out all the changelogs that existed at the time of the BC
                checkoutChangelogs(gitWrapper, upstreamRemote, bcRef);
                entriesFromBc = changelogDirectory.getAsFileTree().getFiles().stream().map(File::getName).collect(Collectors.toSet());

                // Then add/update changelogs from the HEAD of the branch
                // We do an "add" here, rather than checking out the entire directory, in case changelogs have been removed for some reason
                addChangelogsFromRef(gitWrapper, upstreamRemote, branch);
            } else {
                checkoutChangelogs(gitWrapper, upstreamRemote, branch);
            }

            didCheckoutChangelogs = true;
            Properties props = new Properties();
            props.load(
                new StringReader(
                    gitWrapper.runCommand("git", "show", upstreamRemote + "/" + branch + ":build-tools-internal/version.properties")
                )
            );
            String version = props.getProperty("elasticsearch");

            LOGGER.info("Finding changelog files for " + version + "...");

            Set<String> finalEntriesFromBc = entriesFromBc;
            List<ChangelogEntry> entries = changelogDirectory.getAsFileTree().getFiles().stream().filter(f -> {
                // When not using a bc ref, we just take everything from the branch/sha passed in
                if (usingBcRef == false) {
                    return true;
                }

                // If the changelog was present in the BC sha, use it
                if (finalEntriesFromBc.contains(f.getName())) {
                    return true;
                }

                // Otherwise, let's check to see if a reference to the PR exists in the commit log for the sha
                // This specifically covers the case of a PR being merged into the BC with a missing changelog file, and the file added
                // later.
                var prNumber = f.getName().replace(".yaml", "");
                var output = gitWrapper.runCommand("git", "log", bcRef, "--grep", "(#" + prNumber + ")");
                return output.trim().isEmpty() == false;
            }).map(ChangelogEntry::parse).sorted(Comparator.comparing(ChangelogEntry::getPr)).collect(toList());

            ChangelogBundle bundle = new ChangelogBundle(version, finalize, Instant.now().toString(), entries);

            yamlMapper.writeValue(new File("docs/release-notes/changelog-bundles/" + version + ".yml"), bundle);
        } finally {
            if (didCheckoutChangelogs) {
                gitWrapper.runCommand("git", "restore", "-s@", "-SW", "--", changelogDirectory.get().toString());
            }
        }
    }

    private void checkoutChangelogs(GitWrapper gitWrapper, String upstream, String ref) {
        gitWrapper.updateRemote(upstream);

        // If the changelog directory contains modified/new files, we should error out instead of wiping them out silently
        var output = gitWrapper.runCommand("git", "status", "--porcelain", changelogDirectory.get().toString()).trim();
        if (output.isEmpty() == false) {
            throw new IllegalStateException(
                "Changelog directory contains changes that will be wiped out by this task:\n" + changelogDirectory.get() + "\n" + output
            );
        }

        gitWrapper.runCommand("rm", "-rf", changelogDirectory.get().toString());
        var refSpec = upstream + "/" + ref;
        if (ref.contains("upstream/")) {
            refSpec = ref.replace("upstream/", upstream + "/");
        } else if (ref.matches("^[0-9a-f]+$")) {
            refSpec = ref;
        }
        gitWrapper.runCommand("git", "checkout", refSpec, "--", changelogDirectory.get().toString());
    }

    private void addChangelogsFromRef(GitWrapper gitWrapper, String upstream, String ref) {
        var refSpec = upstream + "/" + ref;
        if (ref.contains("upstream/")) {
            refSpec = ref.replace("upstream/", upstream + "/");
        } else if (ref.matches("^[0-9a-f]+$")) {
            refSpec = ref;
        }

        gitWrapper.runCommand("git", "checkout", refSpec, "--", changelogDirectory.get() + "/*.yaml");
    }

    @InputDirectory
    public DirectoryProperty getChangelogDirectory() {
        return changelogDirectory;
    }

    public void setChangelogDirectory(Directory dir) {
        this.changelogDirectory.set(dir);
    }

    @InputDirectory
    public DirectoryProperty getChangelogBundlesDirectory() {
        return changelogBundlesDirectory;
    }

    public void setChangelogBundlesDirectory(Directory dir) {
        this.changelogBundlesDirectory.set(dir);
    }

    @InputFiles
    public FileCollection getChangelogs() {
        return changelogs;
    }

    public void setChangelogs(FileCollection files) {
        this.changelogs.setFrom(files);
    }

    @OutputFile
    public RegularFileProperty getBundleFile() {
        return bundleFile;
    }

    public void setBundleFile(RegularFile file) {
        this.bundleFile.set(file);
    }
}
