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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
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

    /**
     * Configuration for an external repository whose changelog entries should be
     * merged into the Elasticsearch release notes bundle.
     *
     * @param repoUrl       HTTPS clone URL, e.g. {@code https://github.com/elastic/ml-cpp.git}
     * @param sourceRepo    GitHub owner/name used for PR links, e.g. {@code elastic/ml-cpp}
     * @param changelogPath path inside the repo containing YAML entries, e.g. {@code docs/changelog}
     */
    public record ExternalChangelogSource(String repoUrl, String sourceRepo, String changelogPath) {}

    private final ConfigurableFileCollection changelogs;

    private final RegularFileProperty bundleFile;
    private final DirectoryProperty changelogDirectory;
    private final DirectoryProperty changelogBundlesDirectory;

    private final GitWrapper gitWrapper;

    private List<ExternalChangelogSource> externalSources = List.of();

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

    /*
        Given a branch, and possibly a build candidate commit sha
        Check out the changelog yaml files from the branch/BC sha
        Then, bundle them all up into one file and write it to disk, along with a timestamp and whether the release is considered released

         When using a branch without a BC sha:
            - Check out the changelog yaml files from the HEAD of the branch

         When using a BC sha:
            - Check out the changelog yaml files from the BC commit
            - Update those files with any updates from the HEAD of the branch (in case the changelogs get modified later)
            - Check for any changelog yaml files that were added AFTER the BC,
              but whose PR was merged before the BC (in case someone adds a forgotten changelog after the fact)
    */
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

                // If the changelog was present in the BC sha, always use it
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

            // Fetch changelog entries from external repositories
            for (ExternalChangelogSource source : externalSources) {
                List<ChangelogEntry> externalEntries = fetchExternalChangelogs(source, branch);
                if (externalEntries.isEmpty() == false) {
                    LOGGER.info("Adding {} entries from {}", externalEntries.size(), source.sourceRepo());
                    entries.addAll(externalEntries);
                }
            }

            entries.sort(Comparator.comparing(ChangelogEntry::getPr));

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

    /**
     * Fetch changelog entries from an external repository for the given branch.
     * Adds the repo as a temporary git remote, fetches the branch, reads each YAML
     * file via {@code git show}, and sets {@code sourceRepo} on the parsed entries.
     */
    private List<ChangelogEntry> fetchExternalChangelogs(ExternalChangelogSource source, String branchRef) {
        String remoteName = "external-" + source.sourceRepo().replace("/", "-");

        try {
            gitWrapper.runCommand("git", "remote", "add", remoteName, source.repoUrl());
        } catch (Exception e) {
            LOGGER.info("Remote {} may already exist, updating URL", remoteName);
            gitWrapper.runCommand("git", "remote", "set-url", remoteName, source.repoUrl());
        }

        try {
            gitWrapper.runCommand("git", "fetch", "--depth=1", remoteName, branchRef);
        } catch (Exception e) {
            LOGGER.warn("Failed to fetch branch {} from {}: {}", branchRef, source.sourceRepo(), e.getMessage());
            return List.of();
        }

        String ref = remoteName + "/" + branchRef;
        String treePath = source.changelogPath();

        List<String> files;
        try {
            files = gitWrapper.listFiles(ref, treePath).filter(f -> f.endsWith(".yaml")).toList();
        } catch (Exception e) {
            LOGGER.warn("No changelog directory found at {} in {}:{}", treePath, source.sourceRepo(), branchRef);
            return List.of();
        }

        if (files.isEmpty()) {
            LOGGER.info("No external changelog entries found in {}:{}", source.sourceRepo(), branchRef);
            return List.of();
        }

        LOGGER.info("Found {} changelog file(s) in {}:{}", files.size(), source.sourceRepo(), branchRef);

        List<ChangelogEntry> entries = new ArrayList<>();
        for (String filePath : files) {
            try {
                String content = gitWrapper.runCommand("git", "show", ref + ":" + filePath);
                Path tempFile = Files.createTempFile("external-changelog-", ".yaml");
                try {
                    Files.writeString(tempFile, content);
                    ChangelogEntry entry = ChangelogEntry.parse(tempFile.toFile());
                    entry.setSourceRepo(source.sourceRepo());
                    entries.add(entry);
                } finally {
                    Files.deleteIfExists(tempFile);
                }
            } catch (Exception e) {
                LOGGER.warn("Failed to parse external changelog {}: {}", filePath, e.getMessage());
            }
        }

        return entries;
    }

    public void setExternalSources(List<ExternalChangelogSource> sources) {
        this.externalSources = sources;
    }

    public List<ExternalChangelogSource> getExternalSources() {
        return externalSources;
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
