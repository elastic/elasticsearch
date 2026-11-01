/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.pausablefield;

import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * A test plugin that makes query execution pausable. A runtime long field with {@code "lang": "pause"} blocks in its script while
 * paused, so a test can hold requests in flight, and everything they have already allocated, for as long as it needs. Pausing and
 * releasing is done with {@code POST /_pause_field/block} and {@code POST /_pause_field/unblock}, and {@code GET /_pause_field/status}
 * reports how many threads are currently blocked.
 *
 * <p>{@code POST /_pause_field/stop} blocks like {@code block}, and also closes an admission gate: every query on the field then waits
 * while it is being built, before the queries of the clauses that follow it are built, until {@code POST /_pause_field/admit} lets one
 * of the waiting queries in. A test can so serialize expensive query construction across shards and requests. {@code unblock} removes
 * the gate too.
 */
public class PausableFieldPlugin extends Plugin implements ActionPlugin, ScriptPlugin {

    @Override
    public Collection<ActionHandler> getActions() {
        return List.of(new ActionHandler(TransportPauseFieldAction.TYPE, TransportPauseFieldAction.class));
    }

    @Override
    public List<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return List.of(new RestPauseFieldAction());
    }

    @Override
    @SuppressWarnings("unchecked")
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        return new ScriptEngine() {
            @Override
            public String getType() {
                return "pause";
            }

            @Override
            public <FactoryType> FactoryType compile(
                String name,
                String code,
                ScriptContext<FactoryType> context,
                Map<String, String> params
            ) {
                if (context == LongFieldScript.CONTEXT) {
                    return (FactoryType) (LongFieldScript.Factory) (fieldName, p, searchLookup, onScriptError) -> {
                        // Invoked while a query on the field is built, before the queries of later clauses are.
                        PausableField.waitForAdmission();
                        return ctx -> new LongFieldScript(fieldName, p, searchLookup, onScriptError, ctx) {
                            @Override
                            public void execute() {
                                PausableField.waitForExecutionPermit();
                                emit(1);
                            }
                        };
                    };
                }
                throw new IllegalStateException("unsupported type " + context);
            }

            @Override
            public Set<ScriptContext<?>> getSupportedContexts() {
                return Set.of(LongFieldScript.CONTEXT);
            }
        };
    }
}
