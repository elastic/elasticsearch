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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * TODO: Write docs
 */
public class OperatorSettingsController {
    Map<String, OperatorHandler> handlers = null;
    final ClusterSettings clusterSettings;
    final ClusterService clusterService;

    public OperatorSettingsController(ClusterSettings clusterSettings, ClusterService clusterService) {
        this.clusterSettings = clusterSettings;
        this.clusterService = clusterService;
    }

    public void initHandlers(List<OperatorHandler> handlerList) {
        handlers = handlerList.stream().collect(Collectors.toMap(OperatorHandler::key, Function.identity()));
    }

    public ClusterState process(XContentParser parser) throws IOException {
        Map<String, Object> source = parser.map();

        LinkedHashSet<String> orderedHandler = orderedStateHandlers(source.keySet());

        AtomicReference<ClusterState> state = new AtomicReference<>(clusterService.state());

        orderedHandler.forEach(k -> {
            OperatorHandler handler = handlers.get(k);
            state.set(handler.transform(source.get(k), clusterSettings, state.get()));
        });

        return null;
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
