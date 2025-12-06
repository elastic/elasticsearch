/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate;

import java.util.Collection;
import java.util.Set;

/**
 * SPI service interface for supplying {@link ReservedClusterStateHandler} implementations to Elasticsearch
 * from plugins/modules.
 */
public interface ReservedStateHandlerProvider {
    /**
     * Returns a list of {@link ReservedClusterStateHandler} implementations for updating cluster state.
     */
    default Collection<ReservedClusterStateHandler<?>> clusterHandlers() {
        return Set.of();
    }

    /**
     * Returns a list of {@link ReservedClusterStateHandler} implementations for updating project state.
     */
    default Collection<ReservedProjectStateHandler<?>> projectHandlers() {
        return Set.of();
    }
}
