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
 * Represents a serializable difference between two states of a cluster state component.
 * A diff object captures the changes needed to transform one state into another,
 * enabling efficient transmission and application of cluster state updates.
 *
 * <p>This interface is the counterpart to {@link Diffable}, where {@code Diffable}
 * generates diffs and {@code Diff} applies them.</p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Applying a diff to reconstruct a state
 * ClusterState previousState = getPreviousState();
 * Diff<ClusterState> diff = receivedDiff();
 * ClusterState currentState = diff.apply(previousState);
 * }</pre>
 *
 * @param <T> the type of the cluster state component
 */
public interface Diff<T> extends Writeable {

    /**
     * Applies this diff to the specified state component and returns the resulting state.
     * The returned state represents the state after applying all changes captured in this diff.
     *
     * @param part the state to which this diff should be applied
     * @return the resulting state after applying the diff
     */
    T apply(T part);
}
