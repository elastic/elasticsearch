/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transform;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class TransformModule extends AbstractModule {

    private Map<String, Class<? extends Transform.Parser>> parsers = new HashMap<>();

    public void registerPayload(String payloadType, Class<? extends Transform.Parser> parserType) {
        parsers.put(payloadType, parserType);
    }

    @Override
    protected void configure() {

        MapBinder<String, Transform.Parser> mbinder = MapBinder.newMapBinder(binder(), String.class, Transform.Parser.class);
        bind(SearchTransform.Parser.class).asEagerSingleton();
        mbinder.addBinding(SearchTransform.TYPE).to(SearchTransform.Parser.class);

        for (Map.Entry<String, Class<? extends Transform.Parser>> entry : parsers.entrySet()) {
            bind(entry.getValue()).asEagerSingleton();
            mbinder.addBinding(entry.getKey()).to(entry.getValue());
        }
    }
}
