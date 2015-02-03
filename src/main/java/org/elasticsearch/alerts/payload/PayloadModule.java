/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.payload;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class PayloadModule extends AbstractModule {

    private Map<String, Class<? extends Payload.Parser>> parsers = new HashMap<>();

    public void registerPayload(String payloadType, Class<? extends Payload.Parser> parserType) {
        parsers.put(payloadType, parserType);
    }

    @Override
    protected void configure() {

        MapBinder<String, Payload.Parser> mbinder = MapBinder.newMapBinder(binder(), String.class, Payload.Parser.class);
        bind(SearchPayload.Parser.class).asEagerSingleton();
        mbinder.addBinding(SearchPayload.TYPE).to(SearchPayload.Parser.class);

        for (Map.Entry<String, Class<? extends Payload.Parser>> entry : parsers.entrySet()) {
            bind(entry.getValue()).asEagerSingleton();
            mbinder.addBinding(entry.getKey()).to(entry.getValue());
        }
    }
}
