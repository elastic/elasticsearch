/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.watcher.condition.always.AlwaysCondition;
import org.elasticsearch.watcher.condition.always.AlwaysConditionFactory;
import org.elasticsearch.watcher.condition.never.NeverCondition;
import org.elasticsearch.watcher.condition.never.NeverConditionFactory;
import org.elasticsearch.watcher.condition.script.ScriptCondition;
import org.elasticsearch.watcher.condition.script.ScriptConditionFactory;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class ConditionModule extends AbstractModule {

    private final Map<String, Class<? extends ConditionFactory>> factories = new HashMap<>();

    public void registerCondition(String type, Class<? extends ConditionFactory> factoryType) {
        factories.put(type, factoryType);
    }

    @Override
    protected void configure() {

        MapBinder<String, ConditionFactory> factoriesBinder = MapBinder.newMapBinder(binder(), String.class, ConditionFactory.class);

        bind(ScriptConditionFactory.class).asEagerSingleton();
        factoriesBinder.addBinding(ScriptCondition.TYPE).to(ScriptConditionFactory.class);

        bind(NeverConditionFactory.class).asEagerSingleton();
        factoriesBinder.addBinding(NeverCondition.TYPE).to(NeverConditionFactory.class);

        bind(AlwaysConditionFactory.class).asEagerSingleton();
        factoriesBinder.addBinding(AlwaysCondition.TYPE).to(AlwaysConditionFactory.class);

        for (Map.Entry<String, Class<? extends ConditionFactory>> entry : factories.entrySet()) {
            bind(entry.getValue()).asEagerSingleton();
            factoriesBinder.addBinding(entry.getKey()).to(entry.getValue());
        }

        bind(ConditionRegistry.class).asEagerSingleton();
    }
}
