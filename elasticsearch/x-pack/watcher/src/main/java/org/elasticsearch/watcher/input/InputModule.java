/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.watcher.input.chain.ChainInput;
import org.elasticsearch.watcher.input.chain.ChainInputFactory;
import org.elasticsearch.watcher.input.http.HttpInput;
import org.elasticsearch.watcher.input.http.HttpInputFactory;
import org.elasticsearch.watcher.input.none.NoneInput;
import org.elasticsearch.watcher.input.none.NoneInputFactory;
import org.elasticsearch.watcher.input.search.SearchInput;
import org.elasticsearch.watcher.input.search.SearchInputFactory;
import org.elasticsearch.watcher.input.simple.SimpleInput;
import org.elasticsearch.watcher.input.simple.SimpleInputFactory;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class InputModule extends AbstractModule {

    private final Map<String, Class<? extends InputFactory>> parsers = new HashMap<>();

    public void registerInput(String type, Class<? extends InputFactory> parserType) {
        parsers.put(type, parserType);
    }

    @Override
    protected void configure() {
        MapBinder<String, InputFactory> parsersBinder = MapBinder.newMapBinder(binder(), String.class, InputFactory.class);

        bind(SearchInputFactory.class).asEagerSingleton();
        parsersBinder.addBinding(SearchInput.TYPE).to(SearchInputFactory.class);

        bind(SimpleInputFactory.class).asEagerSingleton();
        parsersBinder.addBinding(SimpleInput.TYPE).to(SimpleInputFactory.class);

        bind(HttpInputFactory.class).asEagerSingleton();
        parsersBinder.addBinding(HttpInput.TYPE).to(HttpInputFactory.class);

        bind(NoneInputFactory.class).asEagerSingleton();
        parsersBinder.addBinding(NoneInput.TYPE).to(NoneInputFactory.class);

        // no bind() needed, done using the LazyInitializationModule
        parsersBinder.addBinding(ChainInput.TYPE).to(ChainInputFactory.class);

        for (Map.Entry<String, Class<? extends InputFactory>> entry : parsers.entrySet()) {
            bind(entry.getValue()).asEagerSingleton();
            parsersBinder.addBinding(entry.getKey()).to(entry.getValue());
        }

        bind(InputRegistry.class).asEagerSingleton();
    }
}
