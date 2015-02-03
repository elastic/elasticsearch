/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.trigger;

import org.elasticsearch.alerts.trigger.search.ScriptSearchTrigger;
import org.elasticsearch.alerts.trigger.simple.SimpleTrigger;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class TriggerModule extends AbstractModule {

    private final Map<String, Class<? extends Trigger.Parser>> parsers = new HashMap<>();

    public void registerTrigger(String type, Class<? extends Trigger.Parser> parserType) {
        parsers.put(type, parserType);
    }

    @Override
    protected void configure() {

        MapBinder<String, Trigger.Parser> parsersBinder = MapBinder.newMapBinder(binder(), String.class, Trigger.Parser.class);
        bind(ScriptSearchTrigger.Parser.class).asEagerSingleton();
        parsersBinder.addBinding(ScriptSearchTrigger.TYPE).to(ScriptSearchTrigger.Parser.class);
        bind(SimpleTrigger.Parser.class).asEagerSingleton();
        parsersBinder.addBinding(SimpleTrigger.TYPE).to(SimpleTrigger.Parser.class);

        for (Map.Entry<String, Class<? extends Trigger.Parser>> entry : parsers.entrySet()) {
            bind(entry.getValue()).asEagerSingleton();
            parsersBinder.addBinding(entry.getKey()).to(entry.getValue());
        }

        bind(TriggerRegistry.class).asEagerSingleton();
    }
}
