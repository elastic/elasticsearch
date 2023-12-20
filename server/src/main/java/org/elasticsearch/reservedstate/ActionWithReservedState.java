/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reservedstate;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.core.Strings.format;

public interface ActionWithReservedState<T> {
    /**
     * Override this method if the master node action also has an {@link ReservedClusterStateHandler}
     * interaction.
     * <p>
     * We need to check if certain settings or entities are allowed to be modified by the master node
     * action, depending on if they are set as reserved in 'operator' mode (file based settings, modules, plugins).
     *
     * @return an Optional of the {@link ReservedClusterStateHandler} name
     */
    default Optional<String> reservedStateHandlerName() {
        return Optional.empty();
    }

    /**
     * Override this method to return the keys of the cluster state or cluster entities that are modified by
     * the Request object.
     * <p>
     * This method is used by the reserved state handler logic (see {@link ReservedClusterStateHandler})
     * to verify if the keys don't conflict with an existing key set as reserved.
     *
     * @param request the TransportMasterNode request
     * @return set of String keys intended to be modified/set/deleted by this request
     */
    default Set<String> modifiedKeys(T request) {
        return Collections.emptySet();
    }

    /**
     * Helper method that verifies for key clashes on reserved state updates
     * @param state the current cluster state
     * @param handlerName the name of the reserved state handler related to this implementation
     * @param modified the set of modified keys by the related request
     * @param request a string representation of the request for error reporting purposes
     */
    default void validateForReservedState(ClusterState state, String handlerName, Set<String> modified, String request) {
        List<String> errors = new ArrayList<>();

        for (ReservedStateMetadata metadata : state.metadata().reservedStateMetadata().values()) {
            Set<String> conflicts = metadata.conflicts(handlerName, modified);
            if (conflicts.isEmpty() == false) {
                errors.add(format("[%s] set as read-only by [%s]", String.join(", ", conflicts), metadata.namespace()));
            }
        }

        if (errors.isEmpty() == false) {
            throw new IllegalArgumentException(
                format("Failed to process request [%s] with errors: [%s]", request, String.join(", ", errors))
            );
        }
    }
}
