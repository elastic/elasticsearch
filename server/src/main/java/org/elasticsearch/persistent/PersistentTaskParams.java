/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.persistent;

import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.xcontent.ToXContentObject;

/**
 * Parameters used to start persistent task
 */
public interface PersistentTaskParams extends VersionedNamedWriteable, ToXContentObject {
    /**
     * Lightweight tasks are allowed to be scheduled to nodes shutting down iff all nodes are shutting down.
     * The setting
     * {@link PersistentTasksClusterService#CLUSTER_TASKS_ALLOCATION_ALLOW_LIGHTWEIGHT_ASSIGNMENTS_TO_NODES_SHUTTING_DOWN_SETTING}
     * needs to be set for this behavior to apply.
     */
    default boolean isLightweight() {
        return false;
    }
}
