/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.watcher.transform.chain.ChainTransform;
import org.elasticsearch.watcher.transform.chain.ChainTransformFactory;
import org.elasticsearch.watcher.transform.script.ScriptTransform;
import org.elasticsearch.watcher.transform.script.ScriptTransformFactory;
import org.elasticsearch.watcher.transform.search.SearchTransform;
import org.elasticsearch.watcher.transform.search.SearchTransformFactory;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class TransformModule extends AbstractModule {

    private Map<String, Class<? extends TransformFactory>> factories = new HashMap<>();

    public void registerTransform(String payloadType, Class<? extends TransformFactory> parserType) {
        factories.put(payloadType, parserType);
    }

    @Override
    protected void configure() {
        MapBinder<String, TransformFactory> mbinder = MapBinder.newMapBinder(binder(), String.class, TransformFactory.class);

        bind(SearchTransformFactory.class).asEagerSingleton();
        mbinder.addBinding(SearchTransform.TYPE).to(SearchTransformFactory.class);

        bind(ScriptTransformFactory.class).asEagerSingleton();
        mbinder.addBinding(ScriptTransform.TYPE).to(ScriptTransformFactory.class);

        // no bind() needed, done using the LazyInitializationModule
        mbinder.addBinding(ChainTransform.TYPE).to(ChainTransformFactory.class);

        for (Map.Entry<String, Class<? extends TransformFactory>> entry : factories.entrySet()) {
            bind(entry.getValue()).asEagerSingleton();
            mbinder.addBinding(entry.getKey()).to(entry.getValue());
        }
    }
}
