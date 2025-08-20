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

import javax.inject.Inject;
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
        Map<String, TransportVersionDefinition> definitions = new HashMap<>();
        // temporarily include unreferenced in named until validation understands the distinction
        for (var dir : List.of(NAMED_DIR, UNREFERENCED_DIR)) {
            Path path = transportResourcesDir.resolve(dir);
            if (Files.isDirectory(path) == false) {
                continue;
            }
            try (var definitionsStream = Files.list(path)) {
                for (var definitionFile : definitionsStream.toList()) {
                    String contents = Files.readString(definitionFile, StandardCharsets.UTF_8).strip();
                    var definition = TransportVersionDefinition.fromString(definitionFile.getFileName().toString(), contents);
                    definitions.put(definition.name(), definition);
                }
            }
        }
        return definitions;
    }

    /** Test whether the given named definition exists */
    TransportVersionDefinition getNamedDefinitionFromMain(String name) {
        String resourcePath = getNamedDefinitionRelativePath(name).toString();
        return getMainFile(resourcePath, TransportVersionDefinition::fromString);
    }

    List<TransportVersionDefinition> getChangedNamedDefinitions() throws IOException {
        List<TransportVersionDefinition> changedDefinitions = new ArrayList<>();
        String namedPrefix = NAMED_DIR.toString();
        for (String changedPath : getChangedResources()) {
            if (changedPath.startsWith(namedPrefix) == false) {
                continue;
            }
            TransportVersionDefinition definition = getMainFile(changedPath, TransportVersionDefinition::fromString);
            changedDefinitions.add(definition);
        }
        return changedDefinitions;
    }

    /** Test whether the given named definition exists */
    boolean namedDefinitionExists(String name) {
        return Files.exists(transportResourcesDir.resolve(getNamedDefinitionRelativePath(name)));
    }

    /** Return the path within the repository of the given named definition */
    Path getRepositoryPath(TransportVersionDefinition definition) {
        return rootDir.relativize(transportResourcesDir.resolve(getNamedDefinitionRelativePath(definition.name())));
    }

    void writeNamedDefinition(TransportVersionDefinition definition) throws IOException {
        Path path = transportResourcesDir.resolve(getNamedDefinitionRelativePath(definition.name()));
        Files.writeString(path,
                definition.ids().stream().map(Object::toString).collect(Collectors.joining(",")) + "\n",
                StandardCharsets.UTF_8
        );
    }

    void deleteNamedDefinition(String name) throws IOException{
        Path path = transportResourcesDir.resolve(getNamedDefinitionRelativePath(name));
        Files.deleteIfExists(path);
    }

    /** Read all latest files and return them mapped by their release branch */
    Map<String, TransportVersionLatest> getLatestByReleaseBranch() throws IOException {
        Map<String, TransportVersionLatest> latests = new HashMap<>();
        try (var stream = Files.list(transportResourcesDir.resolve(LATEST_DIR))) {
            for (var latestFile : stream.toList()) {
                String contents = Files.readString(latestFile, StandardCharsets.UTF_8).strip();
                var latest = TransportVersionLatest.fromString(latestFile.getFileName().toString(), contents);
                latests.put(latest.name(), latest);
            }
        }
        return latests;
    }

    /** Retrieve the latest transport version for the given release branch on main */
    TransportVersionLatest getLatestFromMain(String releaseBranch) {
        String resourcePath = getLatestRelativePath(releaseBranch).toString();
        return getMainFile(resourcePath, TransportVersionLatest::fromString);
    }

    List<TransportVersionLatest> getMainLatests() throws IOException {
        List<TransportVersionLatest> latestFiles = new ArrayList<>();
        String latestPrefix = LATEST_DIR.toString();
        for (String mainPath : getMainResources()) {
            if (mainPath.startsWith(latestPrefix) == false) {
                continue;
            }
            TransportVersionLatest latest = getMainFile(mainPath, TransportVersionLatest::fromString);
            latestFiles.add(latest);
        }
        return latestFiles;
    }

    /** Return the path within the repository of the given latest */
    Path getRepositoryPath(TransportVersionLatest latest) {
        return rootDir.relativize(transportResourcesDir.resolve(getLatestRelativePath(latest.releaseBranch())));
    }

    void writeLatestFile(TransportVersionLatest latest) throws IOException {
        Path path = transportResourcesDir.resolve(getLatestRelativePath(latest.releaseBranch()));
        Files.writeString(path, latest.name() + "," + latest.id().complete() + "\n", StandardCharsets.UTF_8);
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
                Collections.addAll(resources, output.split(System.lineSeparator()));
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
                Collections.addAll(resources, output.split(System.lineSeparator()));
                changedResources.set(resources);
            }
        }
        return changedResources.get();
    }

    // Read a transport version resource from the main branch, or return null if it doesn't exist on main
    private <T> T getMainFile(String resourcePath, BiFunction<String, String, T> parser) {
        if (getMainResources().contains(resourcePath) == false) {
            return null;
        }
        String content = gitCommand("show", "main:./" + resourcePath).strip();
        return parser.apply(resourcePath, content);
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
