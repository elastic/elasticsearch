/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class ResolveTransportVersionConflictTask extends AbstractGenerateTransportVersionDefinitionTask {

    @Override
    protected void runGeneration(
        TransportVersionResourcesService resources,
        List<TransportVersionUpperBound> upstreamUpperBounds,
        boolean onReleaseBranch
    ) throws IOException {

        if (onReleaseBranch) {
            if (resources.hasCherryPickConflicts()) {
                getLogger().lifecycle("Resolving transport version conflicts by accepting upstream changes...");
                resources.checkoutOriginalChange();
            } else {
                getLogger().lifecycle("No transport version merge conflicts detected");
            }
        } else {
            Set<String> changedDefinitionNames = resources.getChangedReferableDefinitionNames();
            if (changedDefinitionNames.isEmpty()) {
                getLogger().lifecycle("No transport version changes detected, skipping transport version re-generation");
                return;
            }
            String targetDefinitionName = changedDefinitionNames.iterator().next();

            generateTransportVersionDefinition(resources, targetDefinitionName, upstreamUpperBounds, resources.getIdsByBase());
        }
    }

    @Override
    protected Set<String> getTargetUpperBoundNames(
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

    @Override
    protected void writeUpperBound(TransportVersionResourcesService resources, TransportVersionUpperBound newUpperBound)
        throws IOException {
        resources.writeUpperBound(newUpperBound);
    }
}
