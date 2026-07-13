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
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.Directory;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.RegularFile;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.tasks.Input;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
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
    public record ExternalChangelogSource(String repoUrl, String sourceRepo, String changelogPath) implements java.io.Serializable {}

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
            + "You should generally always use bc-ref. Blank or whitespace-only values are ignored."
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
        final String esBcRefForGit = (bcRef != null && bcRef.isBlank() == false) ? resolveElasticsearchGitRef(bcRef, upstreamRemote) : null;
        Set<String> entriesFromBc = Set.of();

        var didCheckoutChangelogs = false;
        try {
            var usingBcRef = bcRef != null && bcRef.isBlank() == false;
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
            // When using a BC ref, read version.properties from the BC commit so that the bundle is created for the
            // correct version. Reading from the branch HEAD would pick up any subsequent version bump (e.g. the branch
            // is already at 9.3.3 when finalizing the 9.3.2 release), causing the bundle to be written under the wrong
            // version file name.
            String versionRef;
            if (usingBcRef) {
                versionRef = Objects.requireNonNull(esBcRefForGit);
            } else {
                versionRef = upstreamRemote + "/" + branch;
            }
            Properties props = new Properties();
            props.load(new StringReader(gitWrapper.runCommand("git", "show", versionRef + ":build-tools-internal/version.properties")));
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
                var output = gitWrapper.runCommand("git", "log", esBcRefForGit, "--grep", "(#" + prNumber + ")");
                return output.trim().isEmpty() == false;
            }).map(ChangelogEntry::parse).sorted(changelogEntryComparator()).collect(toList());

            // Fetch changelog entries from external repositories
            for (ExternalChangelogSource source : externalSources) {
                List<ChangelogEntry> externalEntries = fetchExternalChangelogs(source, branch, esBcRefForGit, upstreamRemote);
                if (externalEntries.isEmpty() == false) {
                    LOGGER.info("Adding {} entries from {}", externalEntries.size(), source.sourceRepo());
                    entries.addAll(externalEntries);
                }
            }

            entries.sort(changelogEntryComparator());

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
     * Fetches directly from the repo URL into FETCH_HEAD (no persistent remote),
     * reads each YAML file via {@code git show}, and sets {@code sourceRepo} on
     * the parsed entries.
     * <p>
     * When {@code bcRefForFilter} is non-null (Elasticsearch {@code --bc-ref} is in use),
     * it must already be resolved for this repository (see {@link #resolveElasticsearchGitRef}).
     * Entries with a {@code pr} are kept only when that PR appears in external history not newer
     * than the BC ref's committer date (see {@link #gitLogHasGrepUntil}). Entries without a
     * {@code pr} are kept when the changelog file already existed in the external tree at that
     * cutoff (see {@link #externalChangelogFileExistedAtOrBeforeBc}).
     * <p>
     * {@code esUpstreamRemote} strips {@code <remote>/} from {@code branchRef} for {@code git fetch}
     * (same remote name as {@link GitWrapper#getUpstream()}).
     */
    private List<ChangelogEntry> fetchExternalChangelogs(
        ExternalChangelogSource source,
        String branchRef,
        @Nullable String bcRefForFilter,
        String esUpstreamRemote
    ) {
        if (isShaRef(branchRef)) {
            LOGGER.info("Skipping external changelog fetch for SHA-valued --branch: {}", branchRef);
            return List.of();
        }
        String normalizedBranch = normalizeBranchForExternalFetch(branchRef, esUpstreamRemote);

        try {
            if (bcRefForFilter != null && bcRefForFilter.isBlank() == false) {
                // Full history: shallow fetch can hide PR merges older than --depth from FETCH_HEAD,
                // causing BC grep filtering to drop valid external changelog entries.
                LOGGER.info(
                    "Fetching full history from {}:{} for BC filtering (may be slower than shallow fetch)",
                    source.sourceRepo(),
                    normalizedBranch
                );
                gitWrapper.runCommand("git", "fetch", source.repoUrl(), normalizedBranch);
            } else {
                gitWrapper.runCommand("git", "fetch", "--depth=1", source.repoUrl(), normalizedBranch);
            }
        } catch (Exception e) {
            throw new GradleException(
                "Failed to fetch branch " + normalizedBranch + " from " + source.sourceRepo() + " for external changelogs",
                e
            );
        }

        String externalHead = gitWrapper.runCommand("git", "rev-parse", "FETCH_HEAD").trim();

        String treePath = source.changelogPath();

        List<String> files;
        try {
            files = gitWrapper.listFiles("FETCH_HEAD", treePath).filter(f -> f.endsWith(".yaml")).toList();
        } catch (Exception e) {
            LOGGER.warn("No changelog directory found at {} in {}:{}", treePath, source.sourceRepo(), normalizedBranch);
            return List.of();
        }

        if (files.isEmpty()) {
            LOGGER.info("No external changelog entries found in {}:{}", source.sourceRepo(), normalizedBranch);
            return List.of();
        }

        LOGGER.info("Found {} changelog file(s) in {}:{}", files.size(), source.sourceRepo(), normalizedBranch);

        List<ParsedExternalChangelog> parsed = new ArrayList<>();
        for (String filePath : files) {
            String content = gitWrapper.runCommand("git", "show", "FETCH_HEAD:" + filePath);
            ChangelogEntry entry = ChangelogEntry.parse(content);
            entry.setSourceRepo(source.sourceRepo());
            applyExternalFilenamePrFallback(filePath, entry);
            parsed.add(new ParsedExternalChangelog(filePath, entry));
        }

        if (bcRefForFilter != null && bcRefForFilter.isBlank() == false) {
            String bcCommitterIso = committerIsoAtRef(bcRefForFilter);
            int before = parsed.size();
            parsed = parsed.stream()
                .filter(p -> includeExternalChangelogForBuildCandidate(p.entry(), externalHead, bcCommitterIso, p.repoRelativePath()))
                .collect(Collectors.toCollection(ArrayList::new));
            if (before != parsed.size()) {
                LOGGER.info(
                    "Filtered {} external changelog(s) from {} for BC ref {} ({} remaining)",
                    before - parsed.size(),
                    source.sourceRepo(),
                    bcRefForFilter,
                    parsed.size()
                );
            }
        }

        return parsed.stream().map(ParsedExternalChangelog::entry).collect(Collectors.toCollection(ArrayList::new));
    }

    private record ParsedExternalChangelog(String repoRelativePath, ChangelogEntry entry) {}

    /**
     * For external changelog entries, the {@code pr} field refers to that external repository,
     * not Elasticsearch; {@code git log} on the ES repo at {@code --bc-ref} could match an
     * unrelated ES PR with the same number. We only consult the fetched external tip, capped
     * by the BC ref's committer date (see {@link #gitLogHasGrepUntil}).
     * <p>
     * When {@code pr} is absent, we keep the entry only if {@code repoRelativePath} already
     * existed in the external tree at or before the BC committer time (mirroring local
     * {@code entriesFromBc} behavior).
     */
    private boolean includeExternalChangelogForBuildCandidate(
        ChangelogEntry entry,
        String externalTip,
        String bcCommitterIso,
        String repoRelativePath
    ) {
        Integer pr = entry.getPr();
        if (pr == null) {
            return externalChangelogFileExistedAtOrBeforeBc(externalTip, bcCommitterIso, repoRelativePath);
        }
        String grep = "(#" + pr + ")";
        return gitLogHasGrepUntil(externalTip, grep, bcCommitterIso);
    }

    /**
     * True if {@code pathInRepo} is present in the tree at the latest commit on {@code externalTip}'s
     * history with committer time not newer than {@code bcCommitterIso}.
     */
    private boolean externalChangelogFileExistedAtOrBeforeBc(String externalTip, String bcCommitterIso, String pathInRepo) {
        try {
            String rev = gitWrapper.runCommand("git", "rev-list", "-n", "1", "--until", bcCommitterIso, externalTip).trim();
            if (rev.isEmpty()) {
                return false;
            }
            gitWrapper.runCommand("git", "cat-file", "-e", rev + ":" + pathInRepo);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * If YAML omits {@code pr} but the changelog file is named like local ES entries
     * ({@code N.yaml} with numeric {@code N}), set the PR so BC filtering matches
     * {@code executeTask}'s local changelog behavior.
     */
    private static void applyExternalFilenamePrFallback(String repoRelativePath, ChangelogEntry entry) {
        if (entry.getPr() != null) {
            return;
        }
        int slash = repoRelativePath.lastIndexOf('/');
        String baseName = slash >= 0 ? repoRelativePath.substring(slash + 1) : repoRelativePath;
        if (baseName.endsWith(".yaml") == false) {
            return;
        }
        String stem = baseName.substring(0, baseName.length() - ".yaml".length());
        try {
            entry.setPr(Integer.parseInt(stem));
        } catch (NumberFormatException e) {
            // leave unset; BC filtering uses tree-at-BC for inclusion when pr is absent
        }
    }

    /**
     * Resolves {@code --bc-ref} / branch-style refs the same way as {@link #checkoutChangelogs}
     * so {@code git show} / {@code git log} run against the configured upstream remote name
     * instead of a literal {@code upstream/} remote that may not exist.
     */
    static String resolveElasticsearchGitRef(String ref, String upstreamRemote) {
        if (ref.contains("upstream/")) {
            return ref.replace("upstream/", upstreamRemote + "/");
        }
        if (ref.matches("^[0-9a-f]+$")) {
            return ref;
        }
        return upstreamRemote + "/" + ref;
    }

    /**
     * True if {@code ref}'s history contains a commit matching {@code grep} with committer
     * date not newer than {@code untilIsoInclusive} ({@code git show -s --format=%cI} form).
     */
    private boolean gitLogHasGrepUntil(String ref, String grep, String untilIsoInclusive) {
        return gitWrapper.runCommand("git", "log", ref, "--grep", grep, "--until", untilIsoInclusive, "-n", "1").trim().isEmpty() == false;
    }

    private String committerIsoAtRef(String ref) {
        return gitWrapper.runCommand("git", "show", "-s", "--format=%cI", ref).trim();
    }

    private static final Set<String> KNOWN_REMOTE_PREFIXES = Set.of("upstream/", "origin/");

    /**
     * Orders bundled changelog entries by PR number, then by {@code source_repo} so entries from
     * different repositories that share a PR number sort deterministically.
     */
    static Comparator<ChangelogEntry> changelogEntryComparator() {
        return Comparator.comparing(ChangelogEntry::getPr, Comparator.nullsLast(Comparator.naturalOrder()))
            .thenComparing(ChangelogEntry::getSourceRepo, Comparator.nullsFirst(Comparator.naturalOrder()));
    }

    static boolean isShaRef(String ref) {
        return ref.matches("(?i)^[0-9a-f]{7,40}$");
    }

    /**
     * Normalizes a branch reference for use with external repos. Strips known
     * remote prefixes ({@code upstream/}, {@code origin/}) which are ES-repo-specific,
     * and when {@code esUpstreamRemote} is set, strips {@code <remote>/} for the
     * Elasticsearch remote name returned by {@link GitWrapper#getUpstream()} (e.g.
     * {@code elastic/main} → {@code main}) so {@code git fetch} targets a branch that
     * exists on the external repository.
     * <p>
     * Raw commit SHAs are rejected since they are meaningless for external repositories.
     * Other refs (including branch names with slashes like {@code feature/foo}) are passed
     * through except for the prefixes above.
     */
    static String normalizeBranchForExternalFetch(String branchRef) {
        return normalizeBranchForExternalFetch(branchRef, null);
    }

    static String normalizeBranchForExternalFetch(String branchRef, @Nullable String esUpstreamRemote) {
        if (isShaRef(branchRef)) {
            throw new IllegalArgumentException(
                "Cannot use a commit SHA ("
                    + branchRef
                    + ") as --branch when fetching external changelog sources: "
                    + "git fetch on the remote repository requires a branch or tag name. "
                    + "Local Elasticsearch changelog YAML can still be checked out from a SHA; "
                    + "pass a named branch for --branch (and use --bc-ref for the build candidate when applicable)."
            );
        }
        for (String prefix : KNOWN_REMOTE_PREFIXES) {
            if (branchRef.startsWith(prefix)) {
                return branchRef.substring(prefix.length());
            }
        }
        if (esUpstreamRemote != null && esUpstreamRemote.isBlank() == false) {
            String remotePrefix = esUpstreamRemote + "/";
            if (branchRef.startsWith(remotePrefix)) {
                return branchRef.substring(remotePrefix.length());
            }
        }
        return branchRef;
    }

    public void setExternalSources(List<ExternalChangelogSource> sources) {
        this.externalSources = sources;
    }

    @Input
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
