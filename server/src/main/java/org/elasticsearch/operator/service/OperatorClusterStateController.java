/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.operator;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * TODO: Write docs
 */
public class OperatorClusterStateController {
    Map<String, OperatorHandler<?>> handlers = null;
    final ClusterService clusterService;

    public OperatorClusterStateController(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public void initHandlers(List<OperatorHandler<?>> handlerList) {
        handlers = handlerList.stream().collect(Collectors.toMap(OperatorHandler::key, Function.identity()));
    }

    public ClusterState process(String namespace, XContentParser parser) throws IOException {
        Map<String, Object> source = parser.map();

        LinkedHashSet<String> orderedHandlers = orderedStateHandlers(source.keySet());

        ClusterState state = clusterService.state();

        // TODO: extract the namespace keys from the state, if any and pass them to each transform

        for (var handlerKey : orderedHandlers) {
            OperatorHandler<?> handler = handlers.get(handlerKey);
            try {
                // TODO: fetch and pass previous keys for handler to be able to delete
                state = handler.transform(source.get(handlerKey), new TransformState(state, new HashSet<>())).state();
            } catch (Exception e) {
                throw new IllegalStateException("Error processing state change request for: " + handler.key(), e);
            }
        }

        // TODO: extract the keys written for this namespace, and store them in the cluster state
        // TODO: call a clusterService state update task
        // TODO: call reroute service

        return state;
    }

    LinkedHashSet<String> orderedStateHandlers(Set<String> keys) {
        LinkedHashSet<String> orderedHandlers = new LinkedHashSet<>();
        LinkedHashSet<String> dependencyStack = new LinkedHashSet<>();

        for (String key : keys) {
            addStateHandler(key, keys, orderedHandlers, dependencyStack);
        }

        return orderedHandlers;
    }

    void addStateHandler(String key, Set<String> keys, LinkedHashSet<String> ordered, LinkedHashSet<String> visited) {
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
        OperatorHandler<?> handler = handlers.get(key);

        if (handler == null) {
            throw new IllegalStateException("Unknown settings definition type: " + key);
        }

        for (String dependency : handler.dependencies()) {
            if (keys.contains(dependency) == false) {
                throw new IllegalStateException("Missing settings dependency definition: " + key + " -> " + dependency);
            }
            addStateHandler(dependency, keys, ordered, visited);
        }

        visited.remove(key);
        ordered.add(key);
    }
}
