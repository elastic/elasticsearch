/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.esql.heap_attack;

import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
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

public class HeapAttackPlugin extends Plugin implements ActionPlugin, ScriptPlugin {

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
        return List.of(new RestTriggerOutOfMemoryAction(), new RestPauseFieldAction());
    }

    // Deliberately unregistered, only used in unit tests. Copied to AbstractSimpleTransportTestCase#IGNORE_DESERIALIZATION_ERRORS_SETTING
    // so that tests in other packages can see it too.
    static final Setting<Boolean> IGNORE_DESERIALIZATION_ERRORS_SETTING = Setting.boolSetting(
        "transport.ignore_deserialization_errors",
        true,
        Setting.Property.NodeScope
    );

    @Override
    public List<Setting<?>> getSettings() {
        return CollectionUtils.appendToCopy(super.getSettings(), IGNORE_DESERIALIZATION_ERRORS_SETTING);
    }

    @Override
    public Settings additionalSettings() {
        return Settings.builder().put(super.additionalSettings()).put(IGNORE_DESERIALIZATION_ERRORS_SETTING.getKey(), true).build();
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
                    return (FactoryType) (LongFieldScript.Factory) (
                        fieldName,
                        p,
                        searchLookup,
                        onScriptError) -> ctx -> new LongFieldScript(fieldName, p, searchLookup, onScriptError, ctx) {
                            @Override
                            public void execute() {
                                PausableField.waitForExecutionPermit();
                                emit(1);
                            }
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
