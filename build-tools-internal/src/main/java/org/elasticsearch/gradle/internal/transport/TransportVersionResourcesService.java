/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.Property;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;
import org.gradle.api.tasks.Optional;
import org.gradle.process.ExecOperations;
import org.gradle.process.ExecResult;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.inject.Inject;

/**
 * An encapsulation of operations on transport version resources.
 *
 * <p>These are resource files to describe transport versions that will be loaded at Elasticsearch runtime. They exist
 * as jar resource files at runtime, and as a directory of resources at build time.
 *
 * <p>The layout of the transport version resources are as follows:
 * <ul>
 *     <li><b>/transport/definitions/referable/</b>
 *     - Definitions that can be looked up by name. The name is the filename before the .csv suffix.</li>
 *     <li><b>/transport/definitions/unreferable/</b>
 *     - Definitions which contain ids that are known at runtime, but cannot be looked up by name.</li>
 *     <li><b>/transport/upper_bounds/</b>
 *     - The maximum transport version definition that will be loaded on a branch.</li>
 * </ul>
 */
public abstract class TransportVersionResourcesService implements BuildService<TransportVersionResourcesService.Parameters> {

    private static final Logger logger = Logging.getLogger(TransportVersionResourcesService.class);
    private static final String UPSTREAM_REMOTE_NAME = "transport-version-resources-upstream";

    public interface Parameters extends BuildServiceParameters {
        DirectoryProperty getTransportResourcesDirectory();

        DirectoryProperty getRootDirectory();

        @Optional
        Property<String> getBaseRefOverride();
    }

    record IdAndDefinition(TransportVersionId id, TransportVersionDefinition definition) {}

    @Inject
    public abstract ExecOperations getExecOperations();

    private static final Path DEFINITIONS_DIR = Path.of("definitions");
    private static final Path REFERABLE_DIR = DEFINITIONS_DIR.resolve("referable");
    private static final Path UNREFERABLE_DIR = DEFINITIONS_DIR.resolve("unreferable");
    private static final Path UPPER_BOUNDS_DIR = Path.of("upper_bounds");

    private final Path transportResourcesDir;
    private final Path rootDir;
    private final AtomicReference<String> baseRefName = new AtomicReference<>();
    private final AtomicReference<Set<String>> upstreamResources = new AtomicReference<>(null);
    private final AtomicReference<Set<String>> changedResources = new AtomicReference<>(null);

    @Inject
    public TransportVersionResourcesService(Parameters params) {
        this.transportResourcesDir = params.getTransportResourcesDirectory().get().getAsFile().toPath();
        this.rootDir = params.getRootDirectory().get().getAsFile().toPath();
        if (params.getBaseRefOverride().isPresent()) {
            this.baseRefName.set(params.getBaseRefOverride().get());
        }
    }

    /**
     * Return the directory for this repository which contains transport version resources.
     * This should be an input to any tasks reading resources from this service.
     */
    Path getTransportResourcesDir() {
        return transportResourcesDir;
    }

    /**
     * Return the transport version definitions directory for this repository.
     * This should be an input to any tasks that only read definitions from this service.
     */
    Path getDefinitionsDir() {
        return transportResourcesDir.resolve(DEFINITIONS_DIR);
    }

    // return the path, relative to the resources dir, of a definition
    private Path getDefinitionRelativePath(String name, boolean isReferable) {
        Path dir = isReferable ? REFERABLE_DIR : UNREFERABLE_DIR;
        return dir.resolve(name + ".csv");
    }

    /** Return all referable definitions, mapped by their name. */
    Map<String, TransportVersionDefinition> getReferableDefinitions() throws IOException {
        return readDefinitions(transportResourcesDir.resolve(REFERABLE_DIR), true);
    }

    /** Return a single referable definition by name */
    TransportVersionDefinition getReferableDefinition(String name) throws IOException {
        Path resourcePath = transportResourcesDir.resolve(getDefinitionRelativePath(name, true));
        return TransportVersionDefinition.fromString(resourcePath, Files.readString(resourcePath, StandardCharsets.UTF_8), true);
    }

    /** Get a referable definition from the merge base ref in git if it exists there, or null otherwise */
    TransportVersionDefinition getReferableDefinitionFromGitBase(String name) {
        Path resourcePath = getDefinitionRelativePath(name, true);
        return getUpstreamFile(resourcePath, (path, contents) -> TransportVersionDefinition.fromString(path, contents, true));
    }

    /** Get the definition names which have local changes relative to upstream */
    Set<String> getChangedReferableDefinitionNames() {
        return getChangedNames(REFERABLE_DIR);
    }

    /** Test whether the given referable definition exists */
    boolean referableDefinitionExists(String name) {
        return Files.exists(transportResourcesDir.resolve(getDefinitionRelativePath(name, true)));
    }

    /** Return the path within the repository of the given named definition */
    Path getDefinitionPath(TransportVersionDefinition definition) {
        Path relativePath;
        if (definition.isReferable()) {
            relativePath = getDefinitionRelativePath(definition.name(), true);
        } else {
            relativePath = getDefinitionRelativePath(definition.name(), false);
        }
        return rootDir.relativize(transportResourcesDir.resolve(relativePath));
    }

    void writeDefinition(TransportVersionDefinition definition) throws IOException {
        Path path = transportResourcesDir.resolve(getDefinitionRelativePath(definition.name(), definition.isReferable()));
        String type = definition.isReferable() ? "referable" : "unreferable";
        logger.info("Writing " + type + " definition [" + definition + "] to [" + path + "]");
        Files.writeString(
            path,
            definition.ids().stream().map(Object::toString).collect(Collectors.joining(",")) + "\n",
            StandardCharsets.UTF_8
        );
        gitCommand("add", path.toString());
    }

    void deleteReferableDefinition(String name) throws IOException {
        Path path = transportResourcesDir.resolve(getDefinitionRelativePath(name, true));
        if (Files.deleteIfExists(path)) {
            gitCommand("rm", "--ignore-unmatch", path.toString());
        }
    }

    /** Return all unreferable definitions, mapped by their name. */
    Map<String, TransportVersionDefinition> getUnreferableDefinitions() throws IOException {
        return readDefinitions(transportResourcesDir.resolve(UNREFERABLE_DIR), false);
    }

    /** Get a referable definition from the merge base ref in git if it exists there, or null otherwise */
    TransportVersionDefinition getUnreferableDefinitionFromGitBase(String name) {
        Path resourcePath = getDefinitionRelativePath(name, false);
        return getUpstreamFile(resourcePath, (path, contents) -> TransportVersionDefinition.fromString(path, contents, false));
    }

    /** Get all the ids and definitions for a given base id, sorted by the complete id */
    Map<Integer, List<IdAndDefinition>> getIdsByBase() throws IOException {
        Map<Integer, List<IdAndDefinition>> idsByBase = new HashMap<>();

        // first collect all ids, organized by base
        Consumer<TransportVersionDefinition> addToBase = definition -> {
            for (TransportVersionId id : definition.ids()) {
                idsByBase.computeIfAbsent(id.base(), k -> new ArrayList<>()).add(new IdAndDefinition(id, definition));
            }
        };
        getReferableDefinitions().values().forEach(addToBase);
        getUnreferableDefinitions().values().forEach(addToBase);

        // now sort the ids within each base so we can check density later
        for (var ids : idsByBase.values()) {
            // first sort the ids list so we can check compactness and quickly lookup the highest id later
            ids.sort(Comparator.comparingInt(a -> a.id().complete()));
        }
        return idsByBase;
    }

    /** Read all upper bound files and return them mapped by their release name */
    Map<String, TransportVersionUpperBound> getUpperBounds() throws IOException {
        Map<String, TransportVersionUpperBound> upperBounds = new HashMap<>();
        try (var stream = Files.list(transportResourcesDir.resolve(UPPER_BOUNDS_DIR))) {
            for (var latestFile : stream.toList()) {
                String contents = Files.readString(latestFile, StandardCharsets.UTF_8).strip();
                var upperBound = TransportVersionUpperBound.fromString(latestFile, contents);
                upperBounds.put(upperBound.name(), upperBound);
            }
        }
        return upperBounds;
    }

    /** Retrieve an upper bound from the merge base ref in git by name  */
    TransportVersionUpperBound getUpperBoundFromGitBase(String name) {
        Path resourcePath = getUpperBoundRelativePath(name);
        return getUpstreamFile(resourcePath, TransportVersionUpperBound::fromString);
    }

    /** Retrieve all upper bounds that exist in the merge base ref in git */
    List<TransportVersionUpperBound> getUpperBoundsFromGitBase() throws IOException {
        List<TransportVersionUpperBound> upperBounds = new ArrayList<>();
        for (String upstreamPathString : getUpstreamResources()) {
            Path upstreamPath = Path.of(upstreamPathString);
            if (upstreamPath.startsWith(UPPER_BOUNDS_DIR) == false) {
                continue;
            }
            TransportVersionUpperBound upperBound = getUpstreamFile(upstreamPath, TransportVersionUpperBound::fromString);
            upperBounds.add(upperBound);
        }
        return upperBounds;
    }

    Set<String> getChangedUpperBoundNames() {
        return getChangedNames(UPPER_BOUNDS_DIR);
    }

    /** Write the given upper bound to a file in the transport resources */
    void writeUpperBound(TransportVersionUpperBound upperBound) throws IOException {
        Path path = transportResourcesDir.resolve(getUpperBoundRelativePath(upperBound.name()));
        logger.debug("Writing upper bound [" + upperBound + "] to [" + path + "]");
        Files.writeString(path, upperBound.definitionName() + "," + upperBound.definitionId().complete() + "\n", StandardCharsets.UTF_8);

        gitCommand("add", path.toString());
    }

    /** Return the path within the repository of the given latest */
    Path getUpperBoundRepositoryPath(TransportVersionUpperBound latest) {
        return rootDir.relativize(transportResourcesDir.resolve(getUpperBoundRelativePath(latest.name())));
    }

    private Path getUpperBoundRelativePath(String name) {
        return UPPER_BOUNDS_DIR.resolve(name + ".csv");
    }

    boolean hasCherryPickConflicts() {
        if (refExists("CHERRY_PICK_HEAD") == false) {
            return false;
        }
        return gitCommand("diff", "--name-only", "--diff-filter=U", transportResourcesDir.toString()).strip().isEmpty() == false;
    }

    void checkoutOriginalChange() {
        gitCommand("checkout", "--theirs", transportResourcesDir.toString());
        gitCommand("add", transportResourcesDir.toString());
    }

    boolean checkIfDefinitelyOnReleaseBranch(Collection<TransportVersionUpperBound> upperBounds, String currentUpperBoundName) {
        // only want to look at definitions <= the current upper bound.
        // TODO: we should filter all of the upper bounds/definitions that are validated by this, not just in this method
        TransportVersionUpperBound currentUpperBound = upperBounds.stream()
            .filter(u -> u.name().equals(currentUpperBoundName))
            .findFirst()
            .orElse(null);
        if (currentUpperBound == null) {
            // since there is no current upper bound, we don't know if we are on a release branch
            return false;
        }
        return upperBounds.stream().anyMatch(u -> u.definitionId().complete() > currentUpperBound.definitionId().complete());
    }

    private String getBaseRefName() {
        if (baseRefName.get() == null) {
            synchronized (baseRefName) {
                String refName;
                if (refExists("MERGE_HEAD")) {
                    refName = gitCommand("rev-parse", "--verify", "MERGE_HEAD").strip();
                } else {
                    String upstreamRef = findUpstreamRef();
                    refName = gitCommand("merge-base", upstreamRef, "HEAD").strip();
                }

                baseRefName.set(refName);
            }
        }
        return baseRefName.get();
    }

    private String findUpstreamRef() {
        String remotesOutput = gitCommand("remote").strip();
        if (remotesOutput.isEmpty()) {
            logger.warn("No remotes found. Using 'main' branch as upstream ref for transport version resources");
            return "main";
        }
        // default the branch name to look at to that which a PR in CI is targeting
        String branchName = System.getenv("BUILDKITE_PULL_REQUEST_BASE_BRANCH");
        if (branchName == null || branchName.strip().isEmpty()) {
            // fallback to the local branch being tested in CI
            branchName = System.getenv("BUILDKITE_BRANCH");
            if (branchName == null || branchName.strip().isEmpty()) {
                // fallback to main if we aren't in CI
                branchName = "main";
            }
        }
        List<String> remoteNames = List.of(remotesOutput.split("\n"));
        if (remoteNames.contains(UPSTREAM_REMOTE_NAME) == false) {
            // our special remote doesn't exist yet, so create it
            String upstreamUrl = null;
            for (String remoteName : remoteNames) {
                String getUrlOutput = gitCommand("remote", "get-url", remoteName).strip();
                if (getUrlOutput.startsWith("git@github.com:elastic/") || getUrlOutput.startsWith("https://github.com/elastic/")) {
                    upstreamUrl = getUrlOutput;
                }
            }

            if (upstreamUrl != null) {
                gitCommand("remote", "add", UPSTREAM_REMOTE_NAME, upstreamUrl);
            } else {
                logger.warn("No elastic github remotes found to copy. Using 'main' branch as upstream ref for transport version resources");
                return branchName;
            }
        }

        // make sure the remote main ref is up to date
        gitCommand("fetch", UPSTREAM_REMOTE_NAME, branchName);

        return UPSTREAM_REMOTE_NAME + "/" + branchName;
    }

    // Return the transport version resources paths that exist in upstream
    private Set<String> getUpstreamResources() {
        if (upstreamResources.get() == null) {
            synchronized (upstreamResources) {
                String output = gitCommand("ls-tree", "--name-only", "-r", getBaseRefName(), ".");

                HashSet<String> resources = new HashSet<>();
                Collections.addAll(resources, output.split("\n")); // git always outputs LF
                upstreamResources.set(resources);
            }
        }
        return upstreamResources.get();
    }

    // Return the transport version resources paths that have been changed relative to upstream
    private Set<String> getChangedResources() {
        if (changedResources.get() == null) {
            synchronized (changedResources) {
                HashSet<String> resources = new HashSet<>();

                String diffOutput = gitCommand("diff", "--name-only", "--relative", getBaseRefName(), ".");
                if (diffOutput.strip().isEmpty() == false) {
                    Collections.addAll(resources, diffOutput.split("\n")); // git always outputs LF
                }

                String untrackedOutput = gitCommand("ls-files", "--others", "--exclude-standard");
                if (untrackedOutput.strip().isEmpty() == false) {
                    Collections.addAll(resources, untrackedOutput.split("\n")); // git always outputs LF
                }

                changedResources.set(resources);
            }
        }
        return changedResources.get();
    }

    private Set<String> getChangedNames(Path resourcesDir) {
        Set<String> changedNames = new HashSet<>();
        // make sure the prefix is git style paths, always forward slashes
        String resourcesPrefix = resourcesDir.toString().replace('\\', '/');
        for (String changedPath : getChangedResources()) {
            if (changedPath.contains(resourcesPrefix) == false) {
                continue;
            }
            int lastSlashNdx = changedPath.lastIndexOf('/');
            String name = changedPath.substring(lastSlashNdx + 1, changedPath.length() - 4 /* .csv */);
            changedNames.add(name);
        }
        return changedNames;
    }

    // Read a transport version resource from the upstream, or return null if it doesn't exist there
    private <T> T getUpstreamFile(Path resourcePath, BiFunction<Path, String, T> parser) {
        String pathString = resourcePath.toString().replace('\\', '/'); // normalize to forward slash that git uses
        if (getUpstreamResources().contains(pathString) == false) {
            return null;
        }

        String content = gitCommand("show", getBaseRefName() + ":./" + pathString).strip();
        return parser.apply(resourcePath, content);
    }

    private boolean refExists(String refName) {
        // the existence of the MERGE_HEAD/CHERRY_PICK_HEAD ref means we are in the middle of a merge/cherry-pick
        String gitDir = gitCommand("rev-parse", "--git-dir").strip();
        return Files.exists(Path.of(gitDir).resolve(refName));
    }

    private static Map<String, TransportVersionDefinition> readDefinitions(Path dir, boolean isReferable) throws IOException {
        if (Files.isDirectory(dir) == false) {
            return Map.of();
        }
        Map<String, TransportVersionDefinition> definitions = new HashMap<>();
        try (var definitionsStream = Files.list(dir)) {
            for (var definitionFile : definitionsStream.toList()) {
                String contents = Files.readString(definitionFile, StandardCharsets.UTF_8).strip();
                var definition = TransportVersionDefinition.fromString(definitionFile, contents, isReferable);
                definitions.put(definition.name(), definition);
            }
        }
        return definitions;
    }

    // run a git command, relative to the transport version resources directory
    private String gitCommand(String... args) {
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();

        List<String> command = new ArrayList<>();
        Collections.addAll(command, "git", "-C", getTransportResourcesDir().toString());
        Collections.addAll(command, args);

        ExecResult result = getExecOperations().exec(spec -> {
            spec.setCommandLine(command);
            spec.setStandardOutput(stdout);
            spec.setErrorOutput(stdout);
            spec.setIgnoreExitValue(true);
        });

        if (result.getExitValue() != 0) {
            throw new RuntimeException(
                "git command failed with exit code "
                    + result.getExitValue()
                    + System.lineSeparator()
                    + "command: "
                    + String.join(" ", command)
                    + System.lineSeparator()
                    + "output:"
                    + System.lineSeparator()
                    + stdout.toString(StandardCharsets.UTF_8)
            );
        }

        return stdout.toString(StandardCharsets.UTF_8);
    }
}
