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
import org.gradle.api.provider.Property;
import org.gradle.api.services.ServiceReference;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Validates that each defined transport version constant is referenced by at least one project.
 */
@CacheableTask
public abstract class ValidateTransportVersionResourcesTask extends DefaultTask {

    @InputDirectory
    @Optional
    @PathSensitive(PathSensitivity.RELATIVE)
    public Path getResourcesDir() {
        return getResources().get().getTransportResourcesDir();
    }

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getReferencesFiles();

    private record IdAndDefinition(TransportVersionId id, TransportVersionDefinition definition) {}

    private static final Pattern NAME_FORMAT = Pattern.compile("[a-z0-9_]+");

    @ServiceReference("transportVersionResources")
    abstract Property<TransportVersionResourcesService> getResources();

    @TaskAction
    public void validateTransportVersions() throws IOException {
        TransportVersionResourcesService resources = getResources().get();
        Set<String> referencedNames = TransportVersionReference.collectNames(getReferencesFiles());
        Map<String, TransportVersionDefinition> namedDefinitions = resources.getNamedDefinitions();
        Map<String, TransportVersionDefinition> unreferencedDefinitions = resources.getUnreferencedDefinitions();
        Map<String, TransportVersionDefinition> allDefinitions = collectAllDefinitions(namedDefinitions, unreferencedDefinitions);
        Map<Integer, List<IdAndDefinition>> idsByBase = collectIdsByBase(allDefinitions.values());
        Map<String, TransportVersionLatest> latestByReleaseBranch = resources.getLatestByReleaseBranch();

        for (var definition : namedDefinitions.values()) {
            validateNamedDefinition(definition, referencedNames);
        }

        for (var definition : unreferencedDefinitions.values()) {
            validateUnreferencedDefinition(definition);
        }

        for (var entry : idsByBase.entrySet()) {
            validateBase(entry.getKey(), entry.getValue());
        }

        for (var latest : latestByReleaseBranch.values()) {
            validateLatest(latest, allDefinitions, idsByBase);
        }
    }

    private Map<String, TransportVersionDefinition> collectAllDefinitions(
        Map<String, TransportVersionDefinition> namedDefinitions,
        Map<String, TransportVersionDefinition> unreferencedDefinitions
    ) {
        Map<String, TransportVersionDefinition> allDefinitions = new HashMap<>(namedDefinitions);
        for (var entry : unreferencedDefinitions.entrySet()) {
            TransportVersionDefinition existing = allDefinitions.put(entry.getKey(), entry.getValue());
            if (existing != null) {
                Path unreferencedPath = getResources().get().getUnreferencedDefinitionRepositoryPath(entry.getValue());
                throwDefinitionFailure(existing, "has same name as unreferenced definition [" + unreferencedPath + "]");
            }
        }
        return allDefinitions;
    }

    private Map<Integer, List<IdAndDefinition>> collectIdsByBase(Collection<TransportVersionDefinition> definitions) {
        Map<Integer, List<IdAndDefinition>> idsByBase = new HashMap<>();

        // first collect all ids, organized by base
        for (TransportVersionDefinition definition : definitions) {
            for (TransportVersionId id : definition.ids()) {
                idsByBase.computeIfAbsent(id.base(), k -> new ArrayList<>()).add(new IdAndDefinition(id, definition));
            }
        }

        // now sort the ids within each base so we can check density later
        for (var ids : idsByBase.values()) {
            // first sort the ids list so we can check compactness and quickly lookup the highest id later
            ids.sort(Comparator.comparingInt(a -> a.id().complete()));
        }

        return idsByBase;
    }

    private void validateNamedDefinition(TransportVersionDefinition definition, Set<String> referencedNames) {

        // validate any modifications
        Map<Integer, TransportVersionId> existingIdsByBase = new HashMap<>();
        TransportVersionDefinition originalDefinition = getResources().get().getNamedDefinitionFromMain(definition.name());
        if (originalDefinition != null) {
            validateIdenticalPrimaryId(definition, originalDefinition);
            originalDefinition.ids().forEach(id -> existingIdsByBase.put(id.base(), id));
        }

        if (referencedNames.contains(definition.name()) == false) {
            throwDefinitionFailure(definition, "is not referenced");
        }
        if (NAME_FORMAT.matcher(definition.name()).matches() == false) {
            throwDefinitionFailure(definition, "does not have a valid name, must be lowercase alphanumeric and underscore");
        }
        if (definition.ids().isEmpty()) {
            throwDefinitionFailure(definition, "does not contain any ids");
        }
        if (Comparators.isInOrder(definition.ids(), Comparator.naturalOrder()) == false) {
            throwDefinitionFailure(definition, "does not have ordered ids");
        }
        for (int ndx = 0; ndx < definition.ids().size(); ++ndx) {
            TransportVersionId id = definition.ids().get(ndx);

            if (ndx == 0) {
                if (id.patch() != 0) {
                    throwDefinitionFailure(definition, "has patch version " + id.complete() + " as primary id");
                }
            } else {
                if (id.patch() == 0) {
                    throwDefinitionFailure(definition, "contains bwc id [" + id + "] with a patch part of 0");
                }
            }

            // check modifications of ids on same branch, ie sharing same base
            TransportVersionId maybeModifiedId = existingIdsByBase.get(id.base());
            if (maybeModifiedId != null && maybeModifiedId.complete() != id.complete()) {
                throwDefinitionFailure(definition, "modifies existing patch id from " + maybeModifiedId + " to " + id);
            }
        }
    }

    private void validateUnreferencedDefinition(TransportVersionDefinition definition) {
        TransportVersionDefinition originalDefinition = getResources().get().getUnreferencedDefinitionFromMain(definition.name());
        if (originalDefinition != null) {
            validateIdenticalPrimaryId(definition, originalDefinition);
        }
        if (definition.ids().isEmpty()) {
            throwDefinitionFailure(definition, "does not contain any ids");
        }
        if (definition.ids().size() > 1) {
            throwDefinitionFailure(definition, " contains more than one id");
        }
        // note: no name validation, anything that is a valid filename is ok, this allows eg initial_8.9.1
    }

    private void validateIdenticalPrimaryId(TransportVersionDefinition definition, TransportVersionDefinition originalDefinition) {
        assert definition.name().equals(originalDefinition.name());

        int primaryId = definition.ids().get(0).complete();
        int originalPrimaryId = originalDefinition.ids().get(0).complete();
        if (primaryId != originalPrimaryId) {
            throwDefinitionFailure(definition, "has modified primary id from " + originalPrimaryId + " to " + primaryId);
        }
    }

    private void validateLatest(
        TransportVersionLatest latest,
        Map<String, TransportVersionDefinition> definitions,
        Map<Integer, List<IdAndDefinition>> idsByBase
    ) {
        TransportVersionDefinition latestDefinition = definitions.get(latest.name());
        if (latestDefinition == null) {
            throwLatestFailure(latest, "contains transport version name [" + latest.name() + "] which is not defined");
        }
        if (latestDefinition.ids().contains(latest.id()) == false) {
            Path relativePath = getResources().get().getNamedDefinitionRepositoryPath(latestDefinition);
            throwLatestFailure(latest, "has id " + latest.id() + " which is not in definition [" + relativePath + "]");
        }

        List<IdAndDefinition> baseIds = idsByBase.get(latest.id().base());
        IdAndDefinition lastId = baseIds.getLast();
        if (lastId.id().complete() != latest.id().complete()) {
            throwLatestFailure(
                latest,
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

        TransportVersionLatest existingLatest = getResources().get().getLatestFromMain(latest.branch());
        if (existingLatest != null) {
            if (latest.id().patch() != 0 && latest.id().base() != existingLatest.id().base()) {
                throwLatestFailure(latest, "modifies base id from " + existingLatest.id().base() + " to " + latest.id().base());
            }
        }
    }

    private void validateBase(int base, List<IdAndDefinition> ids) {
        // TODO: switch this to a fully dense check once all existing transport versions have been migrated
        IdAndDefinition previous = ids.getLast();
        for (int ndx = ids.size() - 2; ndx >= 0; --ndx) {
            IdAndDefinition current = ids.get(ndx);

            if (previous.id().equals(current.id())) {
                Path existingDefinitionPath = getResources().get().getNamedDefinitionRepositoryPath(previous.definition);
                throwDefinitionFailure(
                    current.definition(),
                    "contains id " + current.id + " already defined in [" + existingDefinitionPath + "]"
                );
            }

            if (previous.id().complete() - 1 != current.id().complete()) {
                throw new IllegalStateException(
                    "Transport version base id " + base + " is missing patch ids between " + current.id() + " and " + previous.id()
                );
            }
            previous = current;
        }
    }

    private void throwDefinitionFailure(TransportVersionDefinition definition, String message) {
        Path relativePath = getResources().get().getNamedDefinitionRepositoryPath(definition);
        throw new IllegalStateException("Transport version definition file [" + relativePath + "] " + message);
    }

    private void throwLatestFailure(TransportVersionLatest latest, String message) {
        Path relativePath = getResources().get().getLatestRepositoryPath(latest);
        throw new IllegalStateException("Latest transport version file [" + relativePath + "] " + message);
    }
}
