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
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.services.ServiceReference;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractGenerateTransportVersionDefinitionTask extends DefaultTask {

    @ServiceReference("transportVersionResources")
    abstract Property<TransportVersionResourcesService> getResourceService();

    @Input
    @Optional
    @Option(option = "increment", description = "The amount to increment the id from the current upper bounds file by")
    public abstract Property<Integer> getIncrement();

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

    protected abstract void runGeneration(
        TransportVersionResourcesService resources,
        List<TransportVersionUpperBound> upstreamUpperBounds,
        boolean onReleaseBranch
    ) throws IOException;

    protected abstract Set<String> getTargetUpperBoundNames(
        TransportVersionResourcesService resources,
        List<TransportVersionUpperBound> upstreamUpperBounds,
        String targetDefinitionName
    ) throws IOException;

    protected abstract void writeUpperBound(TransportVersionResourcesService resources, TransportVersionUpperBound newUpperBound)
        throws IOException;

    @TaskAction
    public void run() throws IOException {
        TransportVersionResourcesService resources = getResourceService().get();
        List<TransportVersionUpperBound> upstreamUpperBounds = resources.getUpperBoundsFromGitBase();
        boolean onReleaseBranch = resources.checkIfDefinitelyOnReleaseBranch(upstreamUpperBounds, getCurrentUpperBoundName().get());

        runGeneration(resources, upstreamUpperBounds, onReleaseBranch);
    }

    protected void generateTransportVersionDefinition(
        TransportVersionResourcesService resources,
        String targetDefinitionName,
        List<TransportVersionUpperBound> upstreamUpperBounds,
        Map<Integer, List<IdAndDefinition>> idsByBase
    ) throws IOException {
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
                    writeUpperBound(resources, newUpperBound);
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
