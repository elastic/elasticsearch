/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import static org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans.NO_SOAR_ASSIGNMENT;

public class SoarAssignments implements OverspillAssignments {

    private final int[] assignments;

    public SoarAssignments(int[] assignments) {
        this.assignments = assignments;
    }

    @Override
    public int size() {
        return assignments.length;
    }

    @Override
    public int[] getAssignmentsFor(int ordinal) {
        if (assignments.length > ordinal && assignments[ordinal] != NO_SOAR_ASSIGNMENT) {
            return new int[] { assignments[ordinal] };
        } else {
            return new int[0];
        }
    }
}
