/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;

import java.util.HashSet;
import java.util.Set;

/**
 * Resharding is currently only implemented in Serverless. This service only exposes minimal metadata about resharding
 * needed by other services.
 */
public class IndexReshardService {
    /**
     * Returns the indices from the provided set that are currently being resharded.
     */
    public static Set<Index> reshardingIndices(final ProjectState projectState, final Set<Index> indicesToCheck) {
        final Set<Index> indices = new HashSet<>();
        for (Index index : indicesToCheck) {
            IndexMetadata indexMetadata = projectState.metadata().index(index);

            if (indexMetadata != null && indexMetadata.getReshardingMetadata() != null) {
                indices.add(index);
            }
        }
        return indices;
    }
}
