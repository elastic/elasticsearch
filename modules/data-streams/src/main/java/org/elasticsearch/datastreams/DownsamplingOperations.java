/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.Index;

import java.util.Set;

public interface DownsamplingOperations {

    /**
     * Returns the names of indices in the given project that are currently being downsampled, determined by inspecting the
     * downsampling persistent tasks in the cluster state.
     */
    Set<Index> getActivelyDownsampledIndexNames(ProjectMetadata project);

    /**
     * Returns empty downsampling operations instance.
     */
    static DownsamplingOperations noop() {
        return new DownsamplingOperations() {

            @Override
            public Set<Index> getActivelyDownsampledIndexNames(ProjectMetadata project) {
                return Set.of();
            }
        };
    }
}
