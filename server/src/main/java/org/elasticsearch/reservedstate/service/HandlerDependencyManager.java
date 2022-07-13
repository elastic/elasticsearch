/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reservedstate.service;

import org.elasticsearch.reservedstate.ReservedClusterStateHandler;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Package private class used by the {@link ReservedClusterStateController} to order the reserved
 * cluster state handlers, by their declared dependencies. See also {@link ReservedClusterStateHandler}.
 */
class HandlerDependencyManager {
    /**
     * Returns an ordered set ({@link LinkedHashSet}) of the cluster state handlers that need to
     * execute for a given list of handler names supplied through the {@link ReservedStateChunk}.
     * @param handlers All possible handlers that are registered with the {@link ReservedClusterStateController}
     * @param handlerNames Names of handlers found in the {@link ReservedStateChunk}
     * @return
     */
    static LinkedHashSet<String> orderedStateHandlers(Map<String, ReservedClusterStateHandler<?>> handlers, Set<String> handlerNames) {
        LinkedHashSet<String> orderedHandlers = new LinkedHashSet<>();
        LinkedHashSet<String> dependencyStack = new LinkedHashSet<>();

        for (String key : handlerNames) {
            addStateHandler(handlers, key, handlerNames, orderedHandlers, dependencyStack);
        }

        return orderedHandlers;
    }

    private static void addStateHandler(
        Map<String, ReservedClusterStateHandler<?>> handlers,
        String key,
        Set<String> keys,
        LinkedHashSet<String> ordered,
        LinkedHashSet<String> visited
    ) {
        if (visited.contains(key)) {
            StringBuilder msg = new StringBuilder("Cycle found in settings dependencies: ");
            visited.forEach(s -> {
                msg.append(s);
                msg.append(" -> ");
            });
            msg.append(key);
            throw new IllegalStateException(msg.toString());
        }

        if (ordered.contains(key)) {
            // already added by another dependent handler
            return;
        }

        visited.add(key);
        ReservedClusterStateHandler<?> handler = handlers.get(key);

        if (handler == null) {
            throw new IllegalStateException("Unknown handler type: " + key);
        }

        for (String dependency : handler.dependencies()) {
            if (keys.contains(dependency) == false) {
                throw new IllegalStateException("Missing handler dependency definition: " + key + " -> " + dependency);
            }
            addStateHandler(handlers, dependency, keys, ordered, visited);
        }

        visited.remove(key);
        ordered.add(key);
    }
}
