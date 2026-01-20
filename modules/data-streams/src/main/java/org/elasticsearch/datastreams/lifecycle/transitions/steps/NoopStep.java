/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.transitions.steps;

// TODO: REMOVE BEFORE PR

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStep;
import org.elasticsearch.datastreams.lifecycle.transitions.StepResources;
import org.elasticsearch.index.Index;

import static org.apache.logging.log4j.LogManager.getLogger;

/**
 * A no-op step used as a placeholder for testing.
 */
public final class NoopStep implements DlmStep {

    private static final Logger logger = getLogger(NoopStep.class);
    private int iterCount = 0;

    @Override
    public boolean stepCompleted(Index index, ProjectState projectState) {
        if (iterCount < 3) {
            iterCount++;
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void execute(StepResources stepResources) {
        logger.info("Executing NoopStep for index: {} in project: {}", stepResources.indexName(), stepResources.projectId());
        // No-op
    }

    @Override
    public String stepName() {
        return "No-Op Step";
    }
}
