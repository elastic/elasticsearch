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

import javax.inject.Inject;

/**
 * An encapsulation of operations on transport version resources.
 *
 * <p>These are resource files to describe transport versions that will be loaded at Elasticsearch runtime. They exist
 * as jar resource files at runtime, and as a directory of resources at build time.
 *
 * <p>The layout of the transport version resources are as follows:
 * <ul>
 *     <li><b>/transport/definitions/named/</b>
 *     - Definitions that can be looked up by name. The name is the filename before the .csv suffix.</li>
 *     <li><b>/transport/definitions/unreferenced/</b>
 *     - Definitions which contain ids that are known at runtime, but cannot be looked up by name.</li>
 *     <li><b>/transport/latest/</b>
 *     - The latest transport version definition for each release branch.</li>
 * </ul>
 */
public abstract class TransportVersionResourcesService implements BuildService<TransportVersionResourcesService.Parameters> {

    public interface Parameters extends BuildServiceParameters {
        DirectoryProperty getTransportResourcesDirectory();

        DirectoryProperty getRootDirectory();
    }

    @Inject
    public abstract ExecOperations getExecOperations();

    private static final Path DEFINITIONS_DIR = Path.of("definitions");
    private static final Path NAMED_DIR = DEFINITIONS_DIR.resolve("named");
    private static final Path UNREFERENCED_DIR = DEFINITIONS_DIR.resolve("unreferenced");
    private static final Path LATEST_DIR = Path.of("latest");

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

    // return the path, relative to the resources dir, of a named definition
    private Path getNamedDefinitionRelativePath(String name) {
        return NAMED_DIR.resolve(name + ".csv");
    }

    /** Return all named definitions, mapped by their name. */
    Map<String, TransportVersionDefinition> getNamedDefinitions() throws IOException {
        return readDefinitions(transportResourcesDir.resolve(NAMED_DIR));
    }

    /** Get a named definition from main if it exists there, or null otherwise */
    TransportVersionDefinition getNamedDefinitionFromMain(String name) {
        Path resourcePath = getNamedDefinitionRelativePath(name);
        return getMainFile(resourcePath, TransportVersionDefinition::fromString);
    }

    /** Test whether the given named definition exists */
    boolean namedDefinitionExists(String name) {
        return Files.exists(transportResourcesDir.resolve(getNamedDefinitionRelativePath(name)));
    }

    /** Return the path within the repository of the given named definition */
    Path getNamedDefinitionRepositoryPath(TransportVersionDefinition definition) {
        return rootDir.relativize(transportResourcesDir.resolve(getNamedDefinitionRelativePath(definition.name())));
    }

    // return the path, relative to the resources dir, of an unreferenced definition
    private Path getUnreferencedDefinitionRelativePath(String name) {
        return UNREFERENCED_DIR.resolve(name + ".csv");
    }

    /** Return all unreferenced definitions, mapped by their name. */
    Map<String, TransportVersionDefinition> getUnreferencedDefinitions() throws IOException {
        return readDefinitions(transportResourcesDir.resolve(UNREFERENCED_DIR));
    }

    /** Get a named definition from main if it exists there, or null otherwise */
    TransportVersionDefinition getUnreferencedDefinitionFromMain(String name) {
        Path resourcePath = getUnreferencedDefinitionRelativePath(name);
        return getMainFile(resourcePath, TransportVersionDefinition::fromString);
    }

    /** Return the path within the repository of the given named definition */
    Path getUnreferencedDefinitionRepositoryPath(TransportVersionDefinition definition) {
        return rootDir.relativize(transportResourcesDir.resolve(getUnreferencedDefinitionRelativePath(definition.name())));
    }

    /** Read all latest files and return them mapped by their release branch */
    Map<String, TransportVersionLatest> getLatestByReleaseBranch() throws IOException {
        Map<String, TransportVersionLatest> latests = new HashMap<>();
        try (var stream = Files.list(transportResourcesDir.resolve(LATEST_DIR))) {
            for (var latestFile : stream.toList()) {
                String contents = Files.readString(latestFile, StandardCharsets.UTF_8).strip();
                var latest = TransportVersionLatest.fromString(latestFile, contents);
                latests.put(latest.name(), latest);
            }
        }
        return latests;
    }

    /** Retrieve the latest transport version for the given release branch on main */
    TransportVersionLatest getLatestFromMain(String releaseBranch) {
        Path resourcePath = getLatestRelativePath(releaseBranch);
        return getMainFile(resourcePath, TransportVersionLatest::fromString);
    }

    /** Return the path within the repository of the given latest */
    Path getLatestRepositoryPath(TransportVersionLatest latest) {
        return rootDir.relativize(transportResourcesDir.resolve(getLatestRelativePath(latest.branch())));
    }

    private Path getLatestRelativePath(String releaseBranch) {
        return LATEST_DIR.resolve(releaseBranch + ".csv");
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
                String output = gitCommand("diff", "--name-only", "main", ".");

                HashSet<String> resources = new HashSet<>();
                Collections.addAll(resources, output.split("\n")); // git always outputs LF
                changedResources.set(resources);
            }
        }
        return changedResources.get();
    }

    // Read a transport version resource from the main branch, or return null if it doesn't exist on main
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
