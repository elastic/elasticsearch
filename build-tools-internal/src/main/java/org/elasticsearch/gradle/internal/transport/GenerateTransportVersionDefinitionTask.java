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
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.options.Option;

import java.io.IOException;
import java.util.ArrayList;
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
public abstract class GenerateTransportVersionDefinitionTask extends AbstractGenerateTransportVersionDefinitionTask {

    /**
     * Files that contain the references to the transport version names.
     */
    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getReferencesFiles();

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

    @Override
    protected void runGeneration(
        TransportVersionResourcesService resources,
        List<TransportVersionUpperBound> upstreamUpperBounds,
        boolean onReleaseBranch
    ) throws IOException {
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
            generateTransportVersionDefinition(resources, targetDefinitionName, upstreamUpperBounds, idsByBase);
        }
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

    @Override
    protected Set<String> getTargetUpperBoundNames(
        TransportVersionResourcesService resources,
        List<TransportVersionUpperBound> upstreamUpperBounds,
        String targetDefinitionName
    ) throws IOException {
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
        resources.writeUpperBound(resetUpperBound);
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

    @Override
    protected void writeUpperBound(TransportVersionResourcesService resources, TransportVersionUpperBound newUpperBound)
        throws IOException {
        resources.writeUpperBound(newUpperBound);
    }
}
