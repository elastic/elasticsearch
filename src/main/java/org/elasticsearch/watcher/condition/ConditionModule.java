/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition;

import org.elasticsearch.watcher.condition.script.ScriptCondition;
import org.elasticsearch.watcher.condition.simple.AlwaysFalseCondition;
import org.elasticsearch.watcher.condition.simple.AlwaysTrueCondition;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class ConditionModule extends AbstractModule {

    private final Map<String, Class<? extends Condition.Parser>> parsers = new HashMap<>();

    public void registerCondition(String type, Class<? extends Condition.Parser> parserType) {
        parsers.put(type, parserType);
    }

    @Override
    protected void configure() {

        MapBinder<String, Condition.Parser> parsersBinder = MapBinder.newMapBinder(binder(), String.class, Condition.Parser.class);
        bind(ScriptCondition.Parser.class).asEagerSingleton();
        parsersBinder.addBinding(ScriptCondition.TYPE).to(ScriptCondition.Parser.class);
        bind(AlwaysFalseCondition.Parser.class).asEagerSingleton();
        parsersBinder.addBinding(AlwaysFalseCondition.TYPE).to(AlwaysFalseCondition.Parser.class);
        bind(AlwaysTrueCondition.Parser.class).asEagerSingleton();
        parsersBinder.addBinding(AlwaysTrueCondition.TYPE).to(AlwaysTrueCondition.Parser.class);

        for (Map.Entry<String, Class<? extends Condition.Parser>> entry : parsers.entrySet()) {
            bind(entry.getValue()).asEagerSingleton();
            parsersBinder.addBinding(entry.getKey()).to(entry.getValue());
        }

        bind(ConditionRegistry.class).asEagerSingleton();
    }
}
