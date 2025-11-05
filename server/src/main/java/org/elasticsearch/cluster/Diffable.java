/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.common.io.stream.Writeable;

/**
 * Represents a cluster state component that can generate a serializable diff representation
 * of changes between two states. This interface enables efficient cluster state updates by
 * transmitting only the differences rather than the entire state.
 *
 * <p>Implementations of this interface must ensure that the generated diff can be applied
 * to the previous state to reconstruct the current state.</p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Creating a diff between two cluster states
 * ClusterState previous = getPreviousState();
 * ClusterState current = getCurrentState();
 * Diff<ClusterState> diff = current.diff(previous);
 *
 * // The diff can be serialized and applied to reconstruct the current state
 * ClusterState reconstructed = diff.apply(previous);
 * }</pre>
 *
 * @param <T> the type of the cluster state component
 */
public interface Diffable<T> extends Writeable {

    /**
     * Generates a serializable diff object representing the changes between this state
     * and the previous state. The returned diff can be applied to the previous state
     * to produce this state.
     *
     * @param previousState the previous state to compare against
     * @return a {@link Diff} object representing the differences
     */
    Diff<T> diff(T previousState);

}
