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
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;
import org.gradle.process.ExecOperations;
import org.gradle.process.ExecResult;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
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
 *     - The maximum transport version definition that will be loaded for each release releaseBranch.</li>
 * </ul>
 */
public abstract class TransportVersionResourcesService implements BuildService<TransportVersionResourcesService.Parameters> {

    private static final Logger logger = Logging.getLogger(TransportVersionResourcesService.class);

    public interface Parameters extends BuildServiceParameters {
        DirectoryProperty getTransportResourcesDirectory();

        DirectoryProperty getRootDirectory();
    }

    @Inject
    public abstract ExecOperations getExecOperations();

    private static final Path DEFINITIONS_DIR = Path.of("definitions");
    private static final Path REFERABLE_DIR = DEFINITIONS_DIR.resolve("referable");
    private static final Path UNREFERABLE_DIR = DEFINITIONS_DIR.resolve("unreferable");
    private static final Path UPPER_BOUNDS_DIR = Path.of("upper_bounds");

    private final Path transportResourcesDir;
    private final Path rootDir;
    private final AtomicReference<Set<String>> mainResources = new AtomicReference<>(null);
    private final AtomicReference<Set<String>> changedResources = new AtomicReference<>(null);

    @Inject
    public TransportVersionResourcesService(Parameters params) {
        this.transportResourcesDir = params.getTransportResourcesDirectory().get().getAsFile().toPath();
        this.rootDir = params.getRootDirectory().get().getAsFile().toPath();
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

    // return the path, relative to the resources dir, of a referable definition
    private Path getReferableDefinitionRelativePath(String name) {
        return REFERABLE_DIR.resolve(name + ".csv");
    }

    /** Return all referable definitions, mapped by their name. */
    Map<String, TransportVersionDefinition> getReferableDefinitions() throws IOException {
        return readDefinitions(transportResourcesDir.resolve(REFERABLE_DIR));
    }

    /** Get a referable definition from main if it exists there, or null otherwise */
    TransportVersionDefinition getReferableDefinitionFromMain(String name) {
        Path resourcePath = getReferableDefinitionRelativePath(name);
        return getMainFile(resourcePath, TransportVersionDefinition::fromString);
    }

    /** Get the definition names which have local changes relative to the main branch */
    List<String> getChangedReferableDefinitionNames() {
        List<String> changedDefinitions = new ArrayList<>();
        String referablePrefix = REFERABLE_DIR.toString();
        for (String changedPath : getChangedResources()) {
            if (changedPath.contains(referablePrefix) == false) {
                continue;
            }
            String name = changedPath.substring(referablePrefix.length() + 1 /* skip slash */, changedPath.length() - 4 /* .csv */);
            changedDefinitions.add(name);
        }
        return changedDefinitions;
    }

    /** Test whether the given referable definition exists */
    boolean referableDefinitionExists(String name) {
        return Files.exists(transportResourcesDir.resolve(getReferableDefinitionRelativePath(name)));
    }

    /** Return the path within the repository of the given named definition */
    Path getReferableDefinitionRepositoryPath(TransportVersionDefinition definition) {
        return rootDir.relativize(transportResourcesDir.resolve(getReferableDefinitionRelativePath(definition.name())));
    }

    void writeReferableDefinition(TransportVersionDefinition definition) throws IOException {
        Path path = transportResourcesDir.resolve(getReferableDefinitionRelativePath(definition.name()));
        logger.debug("Writing referable definition [" + definition + "] to [" + path + "]");
        Files.writeString(
            path,
            definition.ids().stream().map(Object::toString).collect(Collectors.joining(",")) + "\n",
            StandardCharsets.UTF_8
        );
    }

    void deleteReferableDefinition(String name) throws IOException {
        Path path = transportResourcesDir.resolve(getReferableDefinitionRelativePath(name));
        Files.deleteIfExists(path);
    }

    // return the path, relative to the resources dir, of an unreferable definition
    private Path getUnreferableDefinitionRelativePath(String name) {
        return UNREFERABLE_DIR.resolve(name + ".csv");
    }

    /** Return all unreferable definitions, mapped by their name. */
    Map<String, TransportVersionDefinition> getUnreferableDefinitions() throws IOException {
        return readDefinitions(transportResourcesDir.resolve(UNREFERABLE_DIR));
    }

    /** Get a referable definition from main if it exists there, or null otherwise */
    TransportVersionDefinition getUnreferableDefinitionFromMain(String name) {
        Path resourcePath = getUnreferableDefinitionRelativePath(name);
        return getMainFile(resourcePath, TransportVersionDefinition::fromString);
    }

    /** Return the path within the repository of the given referable definition */
    Path getUnreferableDefinitionRepositoryPath(TransportVersionDefinition definition) {
        return rootDir.relativize(transportResourcesDir.resolve(getUnreferableDefinitionRelativePath(definition.name())));
    }

    /** Read all upper bound files and return them mapped by their release releaseBranch */
    Map<String, TransportVersionUpperBound> getUpperBounds() throws IOException {
        Map<String, TransportVersionUpperBound> upperBounds = new HashMap<>();
        try (var stream = Files.list(transportResourcesDir.resolve(UPPER_BOUNDS_DIR))) {
            for (var latestFile : stream.toList()) {
                String contents = Files.readString(latestFile, StandardCharsets.UTF_8).strip();
                var upperBound = TransportVersionUpperBound.fromString(latestFile, contents);
                upperBounds.put(upperBound.releaseBranch(), upperBound);
            }
        }
        return upperBounds;
    }

    /** Retrieve the upper bound for the given release releaseBranch on main */
    TransportVersionUpperBound getUpperBoundFromMain(String releaseBranch) {
        Path resourcePath = getUpperBoundRelativePath(releaseBranch);
        return getMainFile(resourcePath, TransportVersionUpperBound::fromString);
    }

    /** Retrieve all upper bounds that exist on main */
    List<TransportVersionUpperBound> getUpperBoundsFromMain() throws IOException {
        List<TransportVersionUpperBound> upperBounds = new ArrayList<>();
        for (String mainPathString : getMainResources()) {
            Path mainPath = Path.of(mainPathString);
            if (mainPath.startsWith(UPPER_BOUNDS_DIR) == false) {
                continue;
            }
            TransportVersionUpperBound upperBound = getMainFile(mainPath, TransportVersionUpperBound::fromString);
            upperBounds.add(upperBound);
        }
        return upperBounds;
    }

    /** Write the given upper bound to a file in the transport resources */
    void writeUpperBound(TransportVersionUpperBound upperBound) throws IOException {
        Path path = transportResourcesDir.resolve(getUpperBoundRelativePath(upperBound.releaseBranch()));
        logger.debug("Writing upper bound [" + upperBound + "] to [" + path + "]");
        Files.writeString(path, upperBound.name() + "," + upperBound.id().complete() + "\n", StandardCharsets.UTF_8);
    }

    /** Return the path within the repository of the given latest */
    Path getUpperBoundRepositoryPath(TransportVersionUpperBound latest) {
        return rootDir.relativize(transportResourcesDir.resolve(getUpperBoundRelativePath(latest.releaseBranch())));
    }

    private Path getUpperBoundRelativePath(String releaseBranch) {
        return UPPER_BOUNDS_DIR.resolve(releaseBranch + ".csv");
    }

    // Return the transport version resources paths that exist in main
    private Set<String> getMainResources() {
        if (mainResources.get() == null) {
            synchronized (mainResources) {
                String output = gitCommand("ls-tree", "--name-only", "-r", "main", ".");

                HashSet<String> resources = new HashSet<>();
                Collections.addAll(resources, output.split("\n")); // git always outputs LF
                mainResources.set(resources);
            }
        }
        return mainResources.get();
    }

    // Return the transport version resources paths that have been changed relative to main
    private Set<String> getChangedResources() {
        if (changedResources.get() == null) {
            synchronized (changedResources) {
                HashSet<String> resources = new HashSet<>();

                String diffOutput = gitCommand("diff", "--name-only", "main", ".");
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

    // Read a transport version resource from the main releaseBranch, or return null if it doesn't exist on main
    private <T> T getMainFile(Path resourcePath, BiFunction<Path, String, T> parser) {
        String pathString = resourcePath.toString().replace('\\', '/'); // normalize to forward slash that git uses
        if (getMainResources().contains(pathString) == false) {
            return null;
        }

        String content = gitCommand("show", "main:./" + pathString).strip();
        return parser.apply(resourcePath, content);
    }

    private static Map<String, TransportVersionDefinition> readDefinitions(Path dir) throws IOException {
        if (Files.isDirectory(dir) == false) {
            return Map.of();
        }
        Map<String, TransportVersionDefinition> definitions = new HashMap<>();
        try (var definitionsStream = Files.list(dir)) {
            for (var definitionFile : definitionsStream.toList()) {
                String contents = Files.readString(definitionFile, StandardCharsets.UTF_8).strip();
                var definition = TransportVersionDefinition.fromString(definitionFile, contents);
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
