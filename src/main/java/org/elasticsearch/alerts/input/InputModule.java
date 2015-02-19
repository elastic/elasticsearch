/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.input;

import org.elasticsearch.alerts.input.search.SearchInput;
import org.elasticsearch.alerts.input.simple.SimpleInput;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class InputModule extends AbstractModule {

    private final Map<String, Class<? extends Input.Parser>> parsers = new HashMap<>();

    public void registerInput(String type, Class<? extends Input.Parser> parserType) {
        parsers.put(type, parserType);
    }

    @Override
    protected void configure() {

        MapBinder<String, Input.Parser> parsersBinder = MapBinder.newMapBinder(binder(), String.class, Input.Parser.class);
        bind(SearchInput.Parser.class).asEagerSingleton();
        parsersBinder.addBinding(SearchInput.TYPE).to(SearchInput.Parser.class);
        bind(SimpleInput.Parser.class).asEagerSingleton();
        parsersBinder.addBinding(SimpleInput.TYPE).to(SimpleInput.Parser.class);

        for (Map.Entry<String, Class<? extends Input.Parser>> entry : parsers.entrySet()) {
            bind(entry.getValue()).asEagerSingleton();
            parsersBinder.addBinding(entry.getKey()).to(entry.getValue());
        }

        bind(InputRegistry.class).asEagerSingleton();
    }
}
