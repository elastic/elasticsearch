/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

public interface SystemIndexMigrationTask {
    /**
     * Method to be called by  {@link SystemIndexMigrationTaskExecutor} to perform migration
     *
     * @param listener listener to provide updates back to caller
     */
    void migrate(ActionListener<Void> listener);

    /**
     * Any node features that are required for this migration to run. This makes sure that all nodes in the cluster can handle any
     * changes in behaviour introduced by the migration.
     *
     * @return a set of features needed to be supported or an empty set if no change in behaviour is expected
     */
    Set<NodeFeature> nodeFeaturesRequired();

    /**
     * The min mapping version required to support this migration. This makes sure that the index has at least the min mapping that is
     * required to support the migration.
     *
     * @return the minimum mapping version required to apply this migration
     */
    int minMappingVersion();

    /**
     * Check if all the preconditions to perform the migration are met
     *
     * @return true if ready otherwise false
     */
    boolean checkPreConditions();
}
