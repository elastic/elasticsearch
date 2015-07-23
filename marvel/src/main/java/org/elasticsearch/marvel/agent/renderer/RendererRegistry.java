/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;

import java.util.Map;

public class RendererRegistry {

    private final Map<String, Renderer> renderers;

    @Inject
    public RendererRegistry(Map<String, Renderer> renderers) {
        this.renderers = ImmutableMap.copyOf(renderers);
    }

    public Renderer renderer(String type) {
        return renderers.get(type);
    }
}
