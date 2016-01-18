/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;

import java.util.Collections;
import java.util.Map;

public class RendererRegistry {

    private final Map<Class<? extends MarvelDoc>, Renderer> renderers;

    @Inject
    public RendererRegistry(Map<Class<? extends MarvelDoc>, Renderer> renderers) {
        this.renderers = Collections.unmodifiableMap(renderers);
    }

    public Renderer getRenderer(MarvelDoc marvelDoc) {
        return renderers.get(marvelDoc.getClass());
    }
}
