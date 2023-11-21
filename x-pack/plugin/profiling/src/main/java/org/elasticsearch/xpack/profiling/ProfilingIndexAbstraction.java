/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;

import java.util.List;

/**
 * An index that is used by Universal Profiling.
 */
interface ProfilingIndexAbstraction {
    String getName();

    int getVersion();

    List<Migration> getMigrations(int currentIndexTemplateVersion);

    /**
     * Extracts the appropriate index metadata for a given index from the cluster state.
     *
     * @param state Current cluster state. Never <code>null</code>.
     * @return The corresponding index metadata or <code>null</code> if there are none.
     */
    IndexMetadata indexMetadata(ClusterState state);
}
