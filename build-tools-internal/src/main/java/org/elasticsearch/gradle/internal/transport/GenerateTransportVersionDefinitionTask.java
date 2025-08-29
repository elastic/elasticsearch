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
    @Option(
        option = "name",
        description = "The name of the Transport Version definition, e.g. --name=my_new_tv"
    )
    public abstract Property<String> getTransportVersionName();

    @Optional
    @Input
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
            resetAllLatestFiles(resources);
        } else {
            List<TransportVersionId> ids = updateUpperBoundsFiles(resources, mainUpperBounds, releaseBranches, name);
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
            throw new IllegalArgumentException("Missing upper bounds files for branches " + sortedMissing +
                    ", known branches are " + sortedKnown);
        }
    }

    private List<TransportVersionId> updateUpperBoundsFiles(TransportVersionResourcesService resources, List<TransportVersionUpperBound> existingUpperBounds, Set<String> targetReleaseBranches, String name) throws IOException {
        String mainReleaseBranch = getMainReleaseBranch().get();
        int primaryIncrement = getPrimaryIncrement().get();
        if (primaryIncrement <= 0) {
            throw new IllegalArgumentException("Invalid increment [" + primaryIncrement + "], must be a positive integer");
        }
        List<TransportVersionId> ids = new ArrayList<>();

        TransportVersionDefinition existingDefinition = resources.getReferableDefinitionFromMain(name);
        for (TransportVersionUpperBound existingUpperBound : existingUpperBounds) {

            TransportVersionUpperBound upperBoundToWrite = existingUpperBound;

            // LOGIC
            /*
              - name may or may not already be set in the existing upper bound
              - target release branches may or may not target the upper bound file
             */

            resources.writeUpperBound(upperBoundToWrite);

            if (name.equals(existingUpperBound.name())) {
                if (targetReleaseBranches.contains(existingUpperBound.releaseBranch()) == false) {
                    // Here, we aren't targeting this latest file, but need to undo prior updates if the list of minor
                    // versions has changed. We must regenerate this latest file to make this operation idempotent.
                    resources.writeUpperBound(existingUpperBound);
                } else {
                    ids.add(existingUpperBound.id());
                }
            } else {
                if (targetReleaseBranches.contains(existingUpperBound.releaseBranch())) {
                    TransportVersionId targetId = null;
                    if (existingDefinition != null) {
                        for (TransportVersionId id : existingDefinition.ids()) {
                            if (id.base() == existingUpperBound.id().base()) {
                                targetId = id;
                                break;
                            }
                        }
                    }
                    if (targetId == null) {
                        int increment = existingUpperBound.releaseBranch().equals(mainReleaseBranch) ? primaryIncrement : 1;
                        TransportVersionId id = TransportVersionId.fromInt(existingUpperBound.id().complete() + increment);
                        ids.add(id);
                        resources.writeUpperBound(new TransportVersionUpperBound(existingUpperBound.releaseBranch(), name, id));
                    }
                } else {
                    resources.writeUpperBound(existingUpperBound);
                }
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

    private void resetAllLatestFiles(TransportVersionResourcesService resources) throws IOException {
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
}
