/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.marvel.agent.collector.indices.IndexStatsCollector;
import org.elasticsearch.marvel.agent.renderer.indices.IndexStatsRenderer;

import java.util.HashMap;
import java.util.Map;

public class RendererModule extends AbstractModule {

    private Map<String, Class<? extends Renderer>> renderers = new HashMap<>();

    public void registerRenderer(String payloadType, Class<? extends Renderer> renderer) {
        renderers.put(payloadType, renderer);
    }

    @Override
    protected void configure() {
        MapBinder<String, Renderer> mbinder = MapBinder.newMapBinder(binder(), String.class, Renderer.class);

        // Bind default renderers
        bind(IndexStatsRenderer.class).asEagerSingleton();
        mbinder.addBinding(IndexStatsCollector.TYPE).to(IndexStatsRenderer.class);

        for (Map.Entry<String, Class<? extends Renderer>> entry : renderers.entrySet()) {
            bind(entry.getValue()).asEagerSingleton();
            mbinder.addBinding(entry.getKey()).to(entry.getValue());
        }
    }
}
