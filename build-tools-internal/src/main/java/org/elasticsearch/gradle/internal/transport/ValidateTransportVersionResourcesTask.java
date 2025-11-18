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

import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitTask;
import org.elasticsearch.gradle.internal.transport.TransportVersionResourcesService.IdAndDefinition;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.provider.Property;
import org.gradle.api.services.ServiceReference;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.VerificationException;
import org.gradle.api.tasks.VerificationTask;

import java.io.IOException;
import java.nio.file.Path;
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
public abstract class ValidateTransportVersionResourcesTask extends PrecommitTask implements VerificationTask {

    @InputDirectory
    @Optional
    @PathSensitive(PathSensitivity.RELATIVE)
    public Path getResourcesDir() {
        return getResources().get().getTransportResourcesDir();
    }

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getReferencesFiles();

    @Input
    public abstract Property<Boolean> getShouldValidateDensity();

    @Input
    public abstract Property<Boolean> getShouldValidatePrimaryIdNotPatch();

    /**
     * The name of the upper bounds file which will be used at runtime on the current branch. Normally
     * this equates to VersionProperties.getElasticsearchVersion().
     */
    @Input
    public abstract Property<String> getCurrentUpperBoundName();

    @Input
    public abstract Property<Boolean> getCI();

    private static final Pattern NAME_FORMAT = Pattern.compile("[a-z0-9_]+");

    @ServiceReference("transportVersionResources")
    abstract Property<TransportVersionResourcesService> getResources();

    @TaskAction
    public void validateTransportVersions() throws IOException {
        TransportVersionResourcesService resources = getResources().get();
        Set<String> referencedNames = TransportVersionReference.collectNames(getReferencesFiles());
        Map<String, TransportVersionDefinition> referableDefinitions = resources.getReferableDefinitions();
        Map<String, TransportVersionDefinition> unreferableDefinitions = resources.getUnreferableDefinitions();
        Map<String, TransportVersionDefinition> allDefinitions = collectAllDefinitions(referableDefinitions, unreferableDefinitions);
        Map<Integer, List<IdAndDefinition>> idsByBase = resources.getIdsByBase();
        Map<String, TransportVersionUpperBound> upperBounds = resources.getUpperBounds();
        TransportVersionUpperBound currentUpperBound = upperBounds.get(getCurrentUpperBoundName().get());
        boolean onReleaseBranch = resources.checkIfDefinitelyOnReleaseBranch(upperBounds.values(), getCurrentUpperBoundName().get());
        boolean validateModifications = onReleaseBranch == false || getCI().get();

        for (var definition : referableDefinitions.values()) {
            validateNamedDefinition(definition, referencedNames, validateModifications);
        }

        for (var definition : unreferableDefinitions.values()) {
            validateUnreferableDefinition(definition, validateModifications);
        }

        for (var entry : idsByBase.entrySet()) {
            int baseId = entry.getKey();
            // on main we validate all bases, but on release branches we only validate up to the current upper bound
            if (onReleaseBranch == false || baseId <= currentUpperBound.definitionId().base()) {
                validateBase(baseId, entry.getValue());
            }
        }

        if (onReleaseBranch) {
            // on release branches we only check the current upper bound, others may be inaccurate
            validateUpperBound(currentUpperBound, allDefinitions, idsByBase, validateModifications);
        } else {
            for (var upperBound : upperBounds.values()) {
                validateUpperBound(upperBound, allDefinitions, idsByBase, validateModifications);
            }

            validatePrimaryIds(resources, upperBounds, allDefinitions);
        }
    }

    private Map<String, TransportVersionDefinition> collectAllDefinitions(
        Map<String, TransportVersionDefinition> referableDefinitions,
        Map<String, TransportVersionDefinition> unreferableDefinitions
    ) {
        Map<String, TransportVersionDefinition> allDefinitions = new HashMap<>(referableDefinitions);
        for (var entry : unreferableDefinitions.entrySet()) {
            TransportVersionDefinition existing = allDefinitions.put(entry.getKey(), entry.getValue());
            if (existing != null) {
                Path unreferablePath = getResources().get().getDefinitionPath(entry.getValue());
                throwDefinitionFailure(existing, "has same name as unreferable definition [" + unreferablePath + "]");
            }
        }
        return allDefinitions;
    }

    private void validateNamedDefinition(
        TransportVersionDefinition definition,
        Set<String> referencedNames,
        boolean validateModifications
    ) {
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
                if (getShouldValidatePrimaryIdNotPatch().get() && id.patch() != 0) {
                    throwDefinitionFailure(definition, "has patch version " + id.complete() + " as primary id");
                }
            } else {
                if (id.patch() == 0) {
                    throwDefinitionFailure(definition, "contains bwc id [" + id + "] with a patch part of 0");
                }
            }
        }

        if (validateModifications) {
            TransportVersionDefinition originalDefinition = getResources().get().getReferableDefinitionFromGitBase(definition.name());
            if (originalDefinition != null) {
                validateIdenticalPrimaryId(definition, originalDefinition);
                for (int i = 1; i < originalDefinition.ids().size(); ++i) {
                    TransportVersionId originalId = originalDefinition.ids().get(i);

                    // we have a very small number of ids in a definition, so just search linearly
                    boolean found = false;
                    for (int j = 1; j < definition.ids().size(); ++j) {
                        TransportVersionId id = definition.ids().get(j);
                        if (id.base() == originalId.base()) {
                            found = true;
                            if (id.complete() != originalId.complete()) {
                                throwDefinitionFailure(definition, "has modified patch id from " + originalId + " to " + id);
                            }
                        }
                    }
                    if (found == false) {
                        throwDefinitionFailure(definition, "has removed id " + originalId);
                    }
                }
            }
        }
    }

    private void validateUnreferableDefinition(TransportVersionDefinition definition, boolean validateModifications) {
        if (validateModifications) {
            TransportVersionDefinition originalDefinition = getResources().get().getUnreferableDefinitionFromGitBase(definition.name());
            if (originalDefinition != null) {
                validateIdenticalPrimaryId(definition, originalDefinition);
            }
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

    private void validateUpperBound(
        TransportVersionUpperBound upperBound,
        Map<String, TransportVersionDefinition> definitions,
        Map<Integer, List<IdAndDefinition>> idsByBase,
        boolean validateModifications
    ) {
        TransportVersionDefinition upperBoundDefinition = definitions.get(upperBound.definitionName());
        if (upperBoundDefinition == null) {
            throwUpperBoundFailure(
                upperBound,
                "contains transport version name [" + upperBound.definitionName() + "] which is not defined"
            );
        }
        if (upperBoundDefinition.ids().contains(upperBound.definitionId()) == false) {
            Path relativePath = getResources().get().getDefinitionPath(upperBoundDefinition);
            throwUpperBoundFailure(
                upperBound,
                "has id " + upperBound.definitionId() + " which is not in definition [" + relativePath + "]"
            );
        }

        List<IdAndDefinition> baseIds = idsByBase.get(upperBound.definitionId().base());
        IdAndDefinition lastId = baseIds.getLast();
        if (lastId.id().complete() != upperBound.definitionId().complete()) {
            throwUpperBoundFailure(
                upperBound,
                "has id "
                    + upperBound.definitionId()
                    + " from ["
                    + upperBound.definitionName()
                    + "] with base "
                    + upperBound.definitionId().base()
                    + " but another id "
                    + lastId.id().complete()
                    + " from ["
                    + lastId.definition().name()
                    + "] is later for that base"
            );
        }

        if (validateModifications) {
            TransportVersionUpperBound existingUpperBound = getResources().get().getUpperBoundFromGitBase(upperBound.name());
            if (existingUpperBound != null && getShouldValidatePrimaryIdNotPatch().get()) {
                if (upperBound.definitionId().patch() != 0
                    && upperBound.definitionId().base() != existingUpperBound.definitionId().base()) {
                    throwUpperBoundFailure(
                        upperBound,
                        "modifies base id from " + existingUpperBound.definitionId().base() + " to " + upperBound.definitionId().base()
                    );
                }
            }
        }
    }

    private void validateBase(int base, List<IdAndDefinition> ids) {
        // TODO: switch this to a fully dense check once all existing transport versions have been migrated
        IdAndDefinition previous = ids.getLast();
        for (int ndx = ids.size() - 2; ndx >= 0; --ndx) {
            IdAndDefinition current = ids.get(ndx);

            if (previous.id().equals(current.id())) {
                Path existingDefinitionPath = getResources().get().getDefinitionPath(previous.definition());
                throwDefinitionFailure(
                    current.definition(),
                    "contains id " + current.id() + " already defined in [" + existingDefinitionPath + "]"
                );
            }

            if (getShouldValidateDensity().get() && previous.id().complete() - 1 != current.id().complete()) {
                throw new IllegalStateException(
                    "Transport version base id " + base + " is missing patch ids between " + current.id() + " and " + previous.id()
                );
            }
            previous = current;
        }
    }

    private void validatePrimaryIds(
        TransportVersionResourcesService resources,
        Map<String, TransportVersionUpperBound> upperBounds,
        Map<String, TransportVersionDefinition> allDefinitions
    ) {
        // first id is always the highest within a definition, and validated earlier
        // note the first element is actually the highest because the id comparator is in descending order
        var sortedDefinitions = allDefinitions.values().stream().sorted(Comparator.comparing(d -> d.ids().getFirst())).toList();
        TransportVersionDefinition highestDefinition = sortedDefinitions.getFirst();
        TransportVersionId highestId = highestDefinition.ids().getFirst();

        if (sortedDefinitions.size() > 1 && getShouldValidateDensity().get()) {
            TransportVersionDefinition secondHighestDefinition = sortedDefinitions.get(1);
            TransportVersionId secondHighestId = secondHighestDefinition.ids().getFirst();
            if (highestId.complete() > secondHighestId.complete() + 1000) {
                throwDefinitionFailure(
                    highestDefinition,
                    "has primary id "
                        + highestId
                        + " which is more than maximum increment 1000 from id "
                        + secondHighestId
                        + " in definition ["
                        + resources.getDefinitionPath(secondHighestDefinition)
                        + "]"
                );
            }
        }

        for (var upperBound : upperBounds.values()) {
            if (upperBound.definitionId().equals(highestId)) {
                return;
            }
        }

        throwDefinitionFailure(
            highestDefinition,
            "has the highest transport version id [" + highestId + "] but is not present in any upper bounds files"
        );
    }

    private void throwDefinitionFailure(TransportVersionDefinition definition, String message) {
        Path relativePath = getResources().get().getDefinitionPath(definition);
        throw new VerificationException("Transport version definition file [" + relativePath + "] " + message);
    }

    private void throwUpperBoundFailure(TransportVersionUpperBound upperBound, String message) {
        Path relativePath = getResources().get().getUpperBoundRepositoryPath(upperBound);
        throw new VerificationException("Transport version upper bound file [" + relativePath + "] " + message);
    }
}
