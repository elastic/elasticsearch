/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.provider.Property;
import org.gradle.api.services.ServiceReference;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This task generates transport version definition files. These files
 * are runtime resources that TransportVersion loads statically.
 * They contain a comma separated list of integer ids. Each file is named the same
 * as the transport version name itself (with the .csv suffix).
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
    public abstract Property<String> getTransportVersionName();

    @Input
    @Optional
    @Option(
        option = "backport-branches",
        description = "The branches this definition will be backported to, e.g. --backport-branches=9.1,8.19"
    )
    public abstract Property<String> getBackportBranches();

    @Input
    @Optional
    @Option(option = "increment", description = "The amount to increment the primary id for the main releaseBranch")
    public abstract Property<Integer> getPrimaryIncrement();

    /**
     * The version number currently associated with the `main` branch, (e.g. as returned by
     * VersionProperties.getElasticsearchVersion()).
     */
    @Input
    public abstract Property<String> getMainReleaseBranch();

    @TaskAction
    public void run() throws IOException {
        TransportVersionResourcesService resources = getResourceService().get();
        Set<String> referencedNames = TransportVersionReference.collectNames(getReferencesFiles());
        List<String> changedDefinitionNames = resources.getChangedReferableDefinitionNames();
        String name = getTransportVersionName().isPresent()
            ? getTransportVersionName().get()
            : findAddedTransportVersionName(resources, referencedNames, changedDefinitionNames);

        List<TransportVersionUpperBound> mainUpperBounds = resources.getUpperBoundsFromMain();
        Set<String> releaseBranches = getTargetReleaseBranches();
        checkReleaseBranches(mainUpperBounds, releaseBranches);

        if (name.isEmpty()) {
            resetAllUpperBounds(resources);
        } else {
            List<TransportVersionId> ids = updateUpperBounds(resources, mainUpperBounds, releaseBranches, name);
            // (Re)write the definition file.
            resources.writeReferableDefinition(new TransportVersionDefinition(name, ids));
        }

        removeUnusedNamedDefinitions(resources, referencedNames, changedDefinitionNames);
    }

    private void checkReleaseBranches(List<TransportVersionUpperBound> mainUpperBounds, Set<String> releaseBranches) {
        Set<String> missingBranches = new HashSet<>(releaseBranches);
        List<String> knownReleaseBranches = new ArrayList<>();
        for (TransportVersionUpperBound upperBound : mainUpperBounds) {
            knownReleaseBranches.add(upperBound.releaseBranch());
            missingBranches.remove(upperBound.releaseBranch());
        }
        if (missingBranches.isEmpty() == false) {
            List<String> sortedMissing = missingBranches.stream().sorted().toList();
            List<String> sortedKnown = knownReleaseBranches.stream().sorted().toList();
            throw new IllegalArgumentException(
                "Missing upper bounds files for branches " + sortedMissing + ", known branches are " + sortedKnown
            );
        }
    }

    private List<TransportVersionId> updateUpperBounds(
        TransportVersionResourcesService resources,
        List<TransportVersionUpperBound> existingUpperBounds,
        Set<String> targetReleaseBranches,
        String name
    ) throws IOException {
        String mainReleaseBranch = getMainReleaseBranch().get();
        int primaryIncrement = getPrimaryIncrement().get();
        if (primaryIncrement <= 0) {
            throw new IllegalArgumentException("Invalid increment " + primaryIncrement + ", must be a positive integer");
        }
        List<TransportVersionId> ids = new ArrayList<>();

        TransportVersionDefinition existingDefinition = resources.getReferableDefinitionFromMain(name);
        for (TransportVersionUpperBound existingUpperBound : existingUpperBounds) {
            String releaseBranch = existingUpperBound.releaseBranch();

            if (targetReleaseBranches.contains(releaseBranch)) {
                // Case: targeting this branch, find an existing id for this branch if it exists
                TransportVersionId targetId = maybeGetExistingId(existingUpperBound, existingDefinition, name);
                if (targetId == null) {
                    // Case: an id doesn't yet exist for this branch, so create one
                    int increment = releaseBranch.equals(mainReleaseBranch) ? primaryIncrement : 1;
                    targetId = TransportVersionId.fromInt(existingUpperBound.id().complete() + increment);
                    var newUpperBound = new TransportVersionUpperBound(releaseBranch, name, targetId);
                    resources.writeUpperBound(newUpperBound);
                }
                ids.add(targetId);
            } else {
                // Default case: we're not targeting this branch so reset it
                resources.writeUpperBound(existingUpperBound);
            }
        }

        Collections.sort(ids);
        return ids;
    }

    private Set<String> getTargetReleaseBranches() {
        Set<String> releaseBranches = new HashSet<>();
        releaseBranches.add(getMainReleaseBranch().get());
        if (getBackportBranches().isPresent()) {
            releaseBranches.addAll(List.of(getBackportBranches().get().split(",")));
        }

        return releaseBranches;
    }

    private void resetAllUpperBounds(TransportVersionResourcesService resources) throws IOException {
        // TODO: this should also _delete_ extraneous files from latest?
        for (TransportVersionUpperBound upperBound : resources.getUpperBoundsFromMain()) {
            resources.writeUpperBound(upperBound);
        }
    }

    private void removeUnusedNamedDefinitions(
        TransportVersionResourcesService resources,
        Set<String> referencedNames,
        List<String> changedDefinitions
    ) throws IOException {
        for (String definitionName : changedDefinitions) {
            if (referencedNames.contains(definitionName) == false) {
                // we added this definition file, but it's now unreferenced, so delete it
                getLogger().lifecycle("Deleting unreferenced named transport version definition [" + definitionName + "]");
                resources.deleteReferableDefinition(definitionName);
            }
        }
    }

    private String findAddedTransportVersionName(
        TransportVersionResourcesService resources,
        Set<String> referencedNames,
        List<String> changedDefinitions
    ) {
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
            String changedDefinitionName = changedDefinitions.getFirst();
            if (referencedNames.contains(changedDefinitionName)) {
                return changedDefinitionName;
            } else {
                return ""; // the changed name is unreferenced, so go into "reset mode"
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
        if (upperBound.name().equals(name)) {
            // the name exists and this upper bound already points at it
            return upperBound.id();
        }
        if (upperBound.releaseBranch().equals(getMainReleaseBranch().get())) {
            // the upper bound is for main, so return the primary id
            return existingDefinition.ids().get(0);
        }
        // the upper bound is for a non-main branch, so find the id with the same base
        for (TransportVersionId id : existingDefinition.ids()) {
            if (id.base() == upperBound.id().base()) {
                return id;
            }
        }
        return null; // no id for this release branch
    }

}
