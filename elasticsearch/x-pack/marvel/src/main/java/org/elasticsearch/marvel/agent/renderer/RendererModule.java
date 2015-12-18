/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterInfoCollector;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStateCollector;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStatsCollector;
import org.elasticsearch.marvel.agent.collector.indices.IndexRecoveryCollector;
import org.elasticsearch.marvel.agent.collector.indices.IndexStatsCollector;
import org.elasticsearch.marvel.agent.collector.indices.IndicesStatsCollector;
import org.elasticsearch.marvel.agent.collector.node.NodeStatsCollector;
import org.elasticsearch.marvel.agent.collector.shards.ShardsCollector;
import org.elasticsearch.marvel.agent.renderer.cluster.ClusterInfoRenderer;
import org.elasticsearch.marvel.agent.renderer.cluster.ClusterStateNodeRenderer;
import org.elasticsearch.marvel.agent.renderer.cluster.ClusterStateRenderer;
import org.elasticsearch.marvel.agent.renderer.cluster.ClusterStatsRenderer;
import org.elasticsearch.marvel.agent.renderer.cluster.DiscoveryNodeRenderer;
import org.elasticsearch.marvel.agent.renderer.indices.IndexRecoveryRenderer;
import org.elasticsearch.marvel.agent.renderer.indices.IndexStatsRenderer;
import org.elasticsearch.marvel.agent.renderer.indices.IndicesStatsRenderer;
import org.elasticsearch.marvel.agent.renderer.node.NodeStatsRenderer;
import org.elasticsearch.marvel.agent.renderer.shards.ShardsRenderer;

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
        bind(ClusterInfoRenderer.class).asEagerSingleton();
        mbinder.addBinding(ClusterInfoCollector.TYPE).to(ClusterInfoRenderer.class);

        bind(IndicesStatsRenderer.class).asEagerSingleton();
        mbinder.addBinding(IndicesStatsCollector.TYPE).to(IndicesStatsRenderer.class);

        bind(IndexStatsRenderer.class).asEagerSingleton();
        mbinder.addBinding(IndexStatsCollector.TYPE).to(IndexStatsRenderer.class);

        bind(ClusterStatsRenderer.class).asEagerSingleton();
        mbinder.addBinding(ClusterStatsCollector.TYPE).to(ClusterStatsRenderer.class);

        bind(ClusterStateRenderer.class).asEagerSingleton();
        mbinder.addBinding(ClusterStateCollector.TYPE).to(ClusterStateRenderer.class);
        mbinder.addBinding(ClusterStateCollector.NODES_TYPE).to(ClusterStateNodeRenderer.class);
        mbinder.addBinding(ClusterStateCollector.NODE_TYPE).to(DiscoveryNodeRenderer.class);

        bind(ShardsRenderer.class).asEagerSingleton();
        mbinder.addBinding(ShardsCollector.TYPE).to(ShardsRenderer.class);

        bind(NodeStatsRenderer.class).asEagerSingleton();
        mbinder.addBinding(NodeStatsCollector.TYPE).to(NodeStatsRenderer.class);

        bind(IndexRecoveryRenderer.class).asEagerSingleton();
        mbinder.addBinding(IndexRecoveryCollector.TYPE).to(IndexRecoveryRenderer.class);

        for (Map.Entry<String, Class<? extends Renderer>> entry : renderers.entrySet()) {
            bind(entry.getValue()).asEagerSingleton();
            mbinder.addBinding(entry.getKey()).to(entry.getValue());
        }
    }
}
