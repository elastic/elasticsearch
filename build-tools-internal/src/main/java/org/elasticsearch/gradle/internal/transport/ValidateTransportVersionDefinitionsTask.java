/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import com.google.common.collect.Comparators;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;
import org.gradle.process.ExecResult;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;

import javax.inject.Inject;

import static org.elasticsearch.gradle.internal.transport.TransportVersionReference.listFromFile;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.definitionFilePath;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.latestFilePath;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.readDefinitionFile;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.readLatestFile;

/**
 * Validates that each defined transport version constant is referenced by at least one project.
 */
@CacheableTask
public abstract class ValidateTransportVersionDefinitionsTask extends DefaultTask {

    @InputDirectory
    @Optional
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract DirectoryProperty getResourcesDirectory();

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getReferencesFiles();

    private record IdAndDefinition(TransportVersionId id, TransportVersionDefinition definition) {}

    private static final Pattern NAME_FORMAT = Pattern.compile("[a-z0-9_]+");

    private final Path rootPath;
    private final ExecOperations execOperations;

    // all transport version names referenced
    private final Set<String> allNames = new HashSet<>();
    // direct lookup of definition by name
    private final Map<String, TransportVersionDefinition> definitions = new HashMap<>();
    // which resource files already existed
    private final Set<String> existingResources = new HashSet<>();
    // reverse lookup of ids back to name
    private final Map<Integer, String> definedIds = new HashMap<>();
    // lookup of base ids back to definition
    private final Map<Integer, List<IdAndDefinition>> idsByBase = new HashMap<>();
    // direct lookup of latest for each branch
    Map<String, TransportVersionLatest> latestByBranch = new HashMap<>();

    @Inject
    public ValidateTransportVersionDefinitionsTask(ExecOperations execOperations) {
        this.execOperations = execOperations;
        this.rootPath = getProject().getRootProject().getLayout().getProjectDirectory().getAsFile().toPath();
    }

    @TaskAction
    public void validateTransportVersions() throws IOException {
        Path resourcesDir = getResourcesDirectory().getAsFile().get().toPath();
        Path definitionsDir = resourcesDir.resolve("defined");
        Path latestDir = resourcesDir.resolve("latest");

        // first check which resource files already exist in main
        recordExistingResources();

        // then collect all names referenced in the codebase
        for (var referencesFile : getReferencesFiles()) {
            listFromFile(referencesFile.toPath()).stream().map(TransportVersionReference::name).forEach(allNames::add);
        }

        // now load all definitions, do some validation and record them by various keys for later quick lookup
        // NOTE: this must run after loading referenced names and existing definitions
        // NOTE: this is sorted so that the order of cross validation is deterministic
        try (var definitionsStream = Files.list(definitionsDir).sorted()) {
            for (var definitionFile : definitionsStream.toList()) {
                recordAndValidateDefinition(readDefinitionFile(definitionFile));
            }
        }

        // cleanup base lookup so we can check ids
        // NOTE: this must run after definition recording
        for (var entry : idsByBase.entrySet()) {
            cleanupAndValidateBase(entry.getKey(), entry.getValue());
        }

        // now load all latest versions and do validation
        // NOTE: this must run after definition recording and idsByBase cleanup
        try (var latestStream = Files.list(latestDir)) {
            for (var latestFile : latestStream.toList()) {
                recordAndValidateLatest(readLatestFile(latestFile));
            }
        }
    }

    private String gitCommand(String... args) {
        final ByteArrayOutputStream stdout = new ByteArrayOutputStream();

        List<String> command = new ArrayList<>();
        Collections.addAll(command, "git", "-C", rootPath.toAbsolutePath().toString());
        Collections.addAll(command, args);

        ExecResult result = execOperations.exec(spec -> {
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

    private void recordExistingResources() {
        String resourcesPath = relativePath(getResourcesDirectory().getAsFile().get().toPath());
        String output = gitCommand("ls-tree", "--name-only", "-r", "main", resourcesPath);
        Collections.addAll(existingResources, output.split(System.lineSeparator()));
    }

    private void recordAndValidateDefinition(TransportVersionDefinition definition) {
        definitions.put(definition.name(), definition);
        // record the ids for each base id so we can ensure compactness later
        for (TransportVersionId id : definition.ids()) {
            idsByBase.computeIfAbsent(id.base(), k -> new ArrayList<>()).add(new IdAndDefinition(id, definition));
        }

        // validate any modifications
        Map<Integer, TransportVersionId> existingIdsByBase = new HashMap<>();
        TransportVersionDefinition originalDefinition = readExistingDefinition(definition.name());
        if (originalDefinition != null) {

            int primaryId = definition.ids().get(0).complete();
            int originalPrimaryId = originalDefinition.ids().get(0).complete();
            if (primaryId != originalPrimaryId) {
                throwDefinitionFailure(definition.name(), "has modified primary id from " + originalPrimaryId + " to " + primaryId);
            }

            originalDefinition.ids().forEach(id -> existingIdsByBase.put(id.base(), id));
        }

        if (allNames.contains(definition.name()) == false && definition.name().startsWith("initial_") == false) {
            throwDefinitionFailure(definition.name(), "is not referenced");
        }
        if (NAME_FORMAT.matcher(definition.name()).matches() == false) {
            throwDefinitionFailure(definition.name(), "does not have a valid name, must be lowercase alphanumeric and underscore");
        }
        if (definition.ids().isEmpty()) {
            throwDefinitionFailure(definition.name(), "does not contain any ids");
        }
        if (Comparators.isInOrder(definition.ids(), Comparator.reverseOrder()) == false) {
            throwDefinitionFailure(definition.name(), "does not have ordered ids");
        }
        for (int ndx = 0; ndx < definition.ids().size(); ++ndx) {
            TransportVersionId id = definition.ids().get(ndx);

            String existing = definedIds.put(id.complete(), definition.name());
            if (existing != null) {
                throwDefinitionFailure(
                    definition.name(),
                    "contains id " + id + " already defined in [" + definitionRelativePath(existing) + "]"
                );
            }

            if (ndx == 0) {
                // TODO: initial versions will only be applicable to a release branch, so they won't have an associated
                // main version. They will also be loaded differently in the future, but until they are separate, we ignore them here.
                if (id.patch() != 0 && definition.name().startsWith("initial_") == false) {
                    throwDefinitionFailure(definition.name(), "has patch version " + id.complete() + " as primary id");
                }
            } else {
                if (id.patch() == 0) {
                    throwDefinitionFailure(definition.name(), "contains bwc id [" + id + "] with a patch part of 0");
                }
            }

            // check modifications of ids on same branch, ie sharing same base
            TransportVersionId maybeModifiedId = existingIdsByBase.get(id.base());
            if (maybeModifiedId != null && maybeModifiedId.complete() != id.complete()) {
                throwDefinitionFailure(definition.name(), "modifies existing patch id from " + maybeModifiedId + " to " + id);
            }
        }
    }

    private TransportVersionDefinition readExistingDefinition(String name) {
        return readExistingFile(name, this::definitionRelativePath, TransportVersionDefinition::fromString);
    }

    private TransportVersionLatest readExistingLatest(String branch) {
        return readExistingFile(branch, this::latestRelativePath, TransportVersionLatest::fromString);
    }

    private <T> T readExistingFile(String name, Function<String, String> pathFunction, BiFunction<String, String, T> parser) {
        String relativePath = pathFunction.apply(name);
        if (existingResources.contains(relativePath) == false) {
            return null;
        }
        String content = gitCommand("show", "main:" + relativePath).strip();
        return parser.apply(relativePath, content);
    }

    private void recordAndValidateLatest(TransportVersionLatest latest) {
        latestByBranch.put(latest.branch(), latest);

        TransportVersionDefinition latestDefinition = definitions.get(latest.name());
        if (latestDefinition == null) {
            throwLatestFailure(latest.branch(), "contains transport version name [" + latest.name() + "] which is not defined");
        }
        if (latestDefinition.ids().contains(latest.id()) == false) {
            throwLatestFailure(
                latest.branch(),
                "has id " + latest.id() + " which is not in definition [" + definitionRelativePath(latest.name()) + "]"
            );
        }

        List<IdAndDefinition> baseIds = idsByBase.get(latest.id().base());
        IdAndDefinition lastId = baseIds.getLast();
        if (lastId.id().complete() != latest.id().complete()) {
            throwLatestFailure(
                latest.branch(),
                "has id "
                    + latest.id()
                    + " from ["
                    + latest.name()
                    + "] with base "
                    + latest.id().base()
                    + " but another id "
                    + lastId.id().complete()
                    + " from ["
                    + lastId.definition().name()
                    + "] is later for that base"
            );
        }

        TransportVersionLatest existingLatest = readExistingLatest(latest.branch());
        if (existingLatest != null) {
            if (latest.id().patch() != 0 && latest.id().base() != existingLatest.id().base()) {
                throwLatestFailure(latest.branch(), "modifies base id from " + existingLatest.id().base() + " to " + latest.id().base());
            }
        }
    }

    private void cleanupAndValidateBase(int base, List<IdAndDefinition> ids) {
        // first sort the ids list so we can check compactness and quickly lookup the highest id later
        ids.sort(Comparator.comparingInt(a -> a.id().complete()));

        // TODO: switch this to a fully dense check once all existing transport versions have been migrated
        IdAndDefinition previous = ids.getLast();
        for (int ndx = ids.size() - 2; ndx >= 0; --ndx) {
            IdAndDefinition next = ids.get(ndx);
            // note that next and previous are reversed here because we are iterating in reverse order
            if (previous.id().complete() - 1 != next.id().complete()) {
                throw new IllegalStateException(
                    "Transport version base id " + base + " is missing patch ids between " + next.id() + " and " + previous.id()
                );
            }
            previous = next;
        }
    }

    private void throwDefinitionFailure(String name, String message) {
        throw new IllegalStateException("Transport version definition file [" + definitionRelativePath(name) + "] " + message);
    }

    private void throwLatestFailure(String branch, String message) {
        throw new IllegalStateException("Latest transport version file [" + latestRelativePath(branch) + "] " + message);
    }

    private String definitionRelativePath(String name) {
        return relativePath(definitionFilePath(getResourcesDirectory().get(), name));
    }

    private String latestRelativePath(String branch) {
        return relativePath(latestFilePath(getResourcesDirectory().get(), branch));
    }

    private String relativePath(Path file) {
        return rootPath.relativize(file).toString();
    }
}
