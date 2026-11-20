/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.ci;

import org.gradle.api.provider.Property;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;

/**
 * Parses the checked-in CI part assignment once per build instead of once per project.
 */
public abstract class CiPartsBuildService implements BuildService<CiPartsBuildService.Params> {

    public interface Params extends BuildServiceParameters {
        /** Contents of the assignment file; absent when the file does not exist. */
        Property<String> getAssignmentJson();
    }

    private CiPartAssignment assignment;

    public synchronized CiPartAssignment assignment() {
        if (assignment == null) {
            if (getParameters().getAssignmentJson().isPresent() == false) {
                throw new IllegalStateException("CI part assignment " + CiPartsPlugin.ASSIGNMENT_FILE + " is missing");
            }
            assignment = CiPartAssignment.parse(getParameters().getAssignmentJson().get());
        }
        return assignment;
    }
}
