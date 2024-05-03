/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reservedstate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;

import java.util.Set;
import java.util.function.Consumer;

/**
 * A {@link ClusterState} wrapper used by the ReservedClusterStateService to pass the
 * current state as well as previous keys set by an {@link ReservedClusterStateHandler} to each transform
 * step of the cluster state update.
 *
 * Each {@link ReservedClusterStateHandler} can also provide a non cluster state transform consumer that should run after
 * the cluster state is fully validated. This allows for handlers to perform extra steps, like clearing caches or saving
 * other state outside the cluster state. The consumer, if provided, must return a {@link NonStateTransformResult} with
 * the keys that will be saved as reserved in the cluster state.
 */
public record TransformState(ClusterState state, Set<String> keys, Consumer<ActionListener<NonStateTransformResult>> nonStateTransform) {
    public TransformState(ClusterState state, Set<String> keys) {
        this(state, keys, null);
    }
}
