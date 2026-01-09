/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.elasticsearch.gradle.internal.transport.TransportVersionResourcesService.IdAndDefinition;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.services.ServiceReference;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This task generates transport version definition files. These files
 * are runtime resources that TransportVersion loads statically.
 * They contain a comma separated list of integer ids. Each file is named the same
 * as the transport version name itself (with the .csv suffix).
 *
 * Additionally, when definition files are added or updated, the upper bounds files
 * for each relevant branch's upper bound file are also updated.
 */
public abstract class GenerateTransportVersionDefinitionTask extends DefaultTask {

    /**
     * Files that contain the references to the transport version names.
     */
    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getReferencesFiles();

    @ServiceReference("transportVersionResources")
    abstract Property<TransportVersionResourcesService> getResourceService();

    @Input
    @Optional
    @Option(option = "name", description = "The name of the Transport Version definition, e.g. --name=my_new_tv")
    public abstract Property<String> getDefinitionName();

    @Input
    @Optional
    @Option(
        option = "backport-branches",
        description = "The branches this definition will be backported to, e.g. --backport-branches=9.1,8.19"
    )
    public abstract Property<String> getBackportBranches();

    @Input
    @Optional
    @Option(option = "increment", description = "The amount to increment the id from the current upper bounds file by")
    public abstract Property<Integer> getIncrement();

    @Input
    @Optional
    abstract Property<Boolean> getResolveConflict();

    /**
     * The name of the upper bounds file which will be used at runtime on the current branch. Normally
     * this equates to VersionProperties.getElasticsearchVersion().
     */
    @Input
    public abstract Property<String> getCurrentUpperBoundName();

    /**
     * An additional upper bound file that will be consulted when generating a transport version.
     * The larger of this and the current upper bound will be used to create the new primary id.
     */
    @InputFile
    @Optional
    public abstract RegularFileProperty getAlternateUpperBoundFile();

    @TaskAction
    public void run() throws IOException {
        TransportVersionResourcesService resources = getResourceService().get();
        List<TransportVersionUpperBound> upstreamUpperBounds = resources.getUpperBoundsFromGitBase();
        boolean onReleaseBranch = resources.checkIfDefinitelyOnReleaseBranch(upstreamUpperBounds, getCurrentUpperBoundName().get());
        if (onReleaseBranch) {
            throw new IllegalArgumentException("Transport version generation cannot run on release branches");
        }

        Set<String> referencedNames = TransportVersionReference.collectNames(getReferencesFiles());
        Set<String> changedDefinitionNames = resources.getChangedReferableDefinitionNames();
        String targetDefinitionName = getTargetDefinitionName(resources, referencedNames, changedDefinitionNames);

        // First check for any unused definitions. This later generation to not get confused by a definition that can't be used.
        removeUnusedNamedDefinitions(resources, referencedNames, changedDefinitionNames);

        Map<Integer, List<IdAndDefinition>> idsByBase = resources.getIdsByBase();
        if (targetDefinitionName.isEmpty()) {
            getLogger().lifecycle("No transport version name detected, resetting upper bounds");
            resetAllUpperBounds(resources, idsByBase);
        } else {
            getLogger().lifecycle("Generating transport version name: " + targetDefinitionName);

            Set<String> targetUpperBoundNames = getTargetUpperBoundNames(resources, upstreamUpperBounds, targetDefinitionName);

            List<TransportVersionId> ids = updateUpperBounds(
                resources,
                upstreamUpperBounds,
                targetUpperBoundNames,
                idsByBase,
                targetDefinitionName
            );
            // (Re)write the definition file.
            resources.writeDefinition(new TransportVersionDefinition(targetDefinitionName, ids, true));
        }

    }

    private List<TransportVersionId> updateUpperBounds(
        TransportVersionResourcesService resources,
        List<TransportVersionUpperBound> existingUpperBounds,
        Set<String> targetUpperBoundNames,
        Map<Integer, List<IdAndDefinition>> idsByBase,
        String definitionName
    ) throws IOException {
        String currentUpperBoundName = getCurrentUpperBoundName().get();
        int increment = getIncrement().get();
        if (increment <= 0) {
            throw new IllegalArgumentException("Invalid increment " + increment + ", must be a positive integer");
        }
        if (increment > 1000) {
            throw new IllegalArgumentException("Invalid increment " + increment + ", must be no larger than 1000");
        }
        List<TransportVersionId> ids = new ArrayList<>();
        boolean stageInGit = getResolveConflict().getOrElse(false);

        TransportVersionDefinition existingDefinition = resources.getReferableDefinitionFromGitBase(definitionName);
        for (TransportVersionUpperBound existingUpperBound : existingUpperBounds) {
            String upperBoundName = existingUpperBound.name();

            if (targetUpperBoundNames.contains(upperBoundName)) {
                // Case: targeting this upper bound, find an existing id if it exists
                TransportVersionId targetId = maybeGetExistingId(existingUpperBound, existingDefinition, definitionName);
                if (targetId == null) {
                    // Case: an id doesn't yet exist for this upper bound, so create one
                    int targetIncrement = upperBoundName.equals(currentUpperBoundName) ? increment : 1;
                    targetId = createTargetId(existingUpperBound, targetIncrement);
                    var newUpperBound = new TransportVersionUpperBound(upperBoundName, definitionName, targetId);
                    resources.writeUpperBound(newUpperBound, stageInGit);
                }
                ids.add(targetId);
            } else if (resources.getChangedUpperBoundNames().contains(upperBoundName)) {
                // Default case: we're not targeting this branch so reset it
                resetUpperBound(resources, existingUpperBound, idsByBase, definitionName);
            }
        }

        Collections.sort(ids);
        return ids;
    }

    private String getTargetDefinitionName(
        TransportVersionResourcesService resources,
        Set<String> referencedNames,
        Set<String> changedDefinitions
    ) {
        if (getDefinitionName().isPresent()) {
            // an explicit name was passed in, so use it
            return getDefinitionName().get();
        }

        // First check for unreferenced names. We only care about the first one. If there is more than one
        // validation will fail later and the developer will have to remove one. When that happens, generation
        // will re-run and we will fixup the state to use whatever new name remains.
        for (String referencedName : referencedNames) {
            if (resources.referableDefinitionExists(referencedName) == false) {
                return referencedName;
            }
        }

        // Since we didn't find any missing names, we use the first changed name. If there is more than
        // one changed name, validation will fail later, just as above.
        if (changedDefinitions.isEmpty()) {
            return "";
        } else {
            String changedDefinitionName = changedDefinitions.iterator().next();
            if (referencedNames.contains(changedDefinitionName)) {
                return changedDefinitionName;
            } else {
                return ""; // the changed name is unreferenced, so go into "reset mode"
            }
        }
    }

    private Set<String> getTargetUpperBoundNames(
        TransportVersionResourcesService resources,
        List<TransportVersionUpperBound> upstreamUpperBounds,
        String targetDefinitionName
    ) throws IOException {
        if (getResolveConflict().getOrElse(false)) {
            if (getBackportBranches().isPresent()) {
                throw new IllegalArgumentException("Cannot use --resolve-conflict with --backport-branches");
            }

            return getUpperBoundNamesFromDefinition(resources, upstreamUpperBounds, targetDefinitionName);
        }

        Set<String> targetUpperBoundNames = new HashSet<>();
        targetUpperBoundNames.add(getCurrentUpperBoundName().get());
        if (getBackportBranches().isPresent()) {
            targetUpperBoundNames.addAll(List.of(getBackportBranches().get().split(",")));
        }

        Set<String> missingBranches = new HashSet<>(targetUpperBoundNames);
        List<String> knownUpperBoundNames = new ArrayList<>();
        for (TransportVersionUpperBound upperBound : upstreamUpperBounds) {
            knownUpperBoundNames.add(upperBound.name());
            missingBranches.remove(upperBound.name());
        }
        if (missingBranches.isEmpty() == false) {
            List<String> sortedMissing = missingBranches.stream().sorted().toList();
            List<String> sortedKnown = knownUpperBoundNames.stream().sorted().toList();
            throw new IllegalArgumentException(
                "Missing upper bounds files for branches " + sortedMissing + ", known branches are " + sortedKnown
            );
        }

        return targetUpperBoundNames;
    }

    private Set<String> getUpperBoundNamesFromDefinition(
        TransportVersionResourcesService resources,
        List<TransportVersionUpperBound> upstreamUpperBounds,
        String targetDefinitionName
    ) throws IOException {
        TransportVersionDefinition definition = resources.getReferableDefinition(targetDefinitionName);
        Set<String> upperBoundNames = new HashSet<>();
        upperBoundNames.add(getCurrentUpperBoundName().get());

        // skip the primary id as that is current, which we always add
        for (int i = 1; i < definition.ids().size(); ++i) {
            TransportVersionId id = definition.ids().get(i);
            // we have a small number of upper bound files, so just scan for the ones we want
            for (TransportVersionUpperBound upperBound : upstreamUpperBounds) {
                if (upperBound.definitionId().base() == id.base()) {
                    upperBoundNames.add(upperBound.name());
                }
            }
        }

        return upperBoundNames;
    }

    private void resetAllUpperBounds(TransportVersionResourcesService resources, Map<Integer, List<IdAndDefinition>> idsByBase)
        throws IOException {
        for (String upperBoundName : resources.getChangedUpperBoundNames()) {
            TransportVersionUpperBound upstreamUpperBound = resources.getUpperBoundFromGitBase(upperBoundName);
            resetUpperBound(resources, upstreamUpperBound, idsByBase, null);
        }
    }

    private void resetUpperBound(
        TransportVersionResourcesService resources,
        TransportVersionUpperBound upperBound,
        Map<Integer, List<IdAndDefinition>> idsByBase,
        String ignoreDefinitionName
    ) throws IOException {
        List<IdAndDefinition> idsForUpperBound = idsByBase.get(upperBound.definitionId().base());
        if (idsForUpperBound == null) {
            throw new RuntimeException("Could not find base id: " + upperBound.definitionId().base());
        }
        IdAndDefinition resetValue = idsForUpperBound.getLast();
        if (resetValue.definition().name().equals(ignoreDefinitionName)) {
            // there must be another definition in this base since the ignored definition is new
            assert idsForUpperBound.size() >= 2;
            resetValue = idsForUpperBound.get(idsForUpperBound.size() - 2);
        }
        var resetUpperBound = new TransportVersionUpperBound(upperBound.name(), resetValue.definition().name(), resetValue.id());
        resources.writeUpperBound(resetUpperBound, false);
    }

    private void removeUnusedNamedDefinitions(
        TransportVersionResourcesService resources,
        Set<String> referencedNames,
        Set<String> changedDefinitions
    ) throws IOException {
        for (String definitionName : changedDefinitions) {
            if (referencedNames.contains(definitionName) == false) {
                // we added this definition file, but it's now unreferenced, so delete it
                getLogger().lifecycle("Deleting unreferenced named transport version definition [" + definitionName + "]");
                resources.deleteReferableDefinition(definitionName);
            }
        }
    }

    private TransportVersionId maybeGetExistingId(
        TransportVersionUpperBound upperBound,
        TransportVersionDefinition existingDefinition,
        String name
    ) {
        if (existingDefinition == null) {
            // the name doesn't yet exist, so there is no id to return
            return null;
        }
        if (upperBound.definitionName().equals(name)) {
            // the name exists and this upper bound already points at it
            return upperBound.definitionId();
        }
        if (upperBound.name().equals(getCurrentUpperBoundName().get())) {
            // this is the upper bound of the current branch, so use the primary id
            return existingDefinition.ids().getFirst();
        }
        // the upper bound is for a non-current branch, so find the id with the same base
        for (TransportVersionId id : existingDefinition.ids()) {
            if (id.base() == upperBound.definitionId().base()) {
                return id;
            }
        }
        return null; // no existing id for this upper bound
    }

    private TransportVersionId createTargetId(TransportVersionUpperBound existingUpperBound, int increment) throws IOException {
        int currentId = existingUpperBound.definitionId().complete();

        // allow for an alternate upper bound file to be consulted. This supports Serverless basing its
        // own transport version ids on the greater of server or serverless
        if (getAlternateUpperBoundFile().isPresent()) {
            Path altUpperBoundPath = getAlternateUpperBoundFile().get().getAsFile().toPath();
            String contents = Files.readString(altUpperBoundPath, StandardCharsets.UTF_8);
            var altUpperBound = TransportVersionUpperBound.fromString(altUpperBoundPath, contents);
            if (altUpperBound.definitionId().complete() > currentId) {
                currentId = altUpperBound.definitionId().complete();
            }
        }

        return TransportVersionId.fromInt(currentId + increment);
    }

}
