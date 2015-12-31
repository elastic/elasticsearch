/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStateCollector;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStatsCollector;
import org.elasticsearch.marvel.agent.collector.indices.IndexRecoveryCollector;
import org.elasticsearch.marvel.agent.collector.indices.IndexStatsCollector;
import org.elasticsearch.marvel.agent.collector.indices.IndicesStatsCollector;
import org.elasticsearch.marvel.agent.collector.node.NodeStatsCollector;
import org.elasticsearch.marvel.agent.collector.shards.ShardsCollector;

import java.util.HashSet;
import java.util.Set;

public class CollectorModule extends AbstractModule {

    private final Set<Class<? extends Collector>> collectors = new HashSet<>();

    public CollectorModule() {
        // Registers default collectors
        registerCollector(IndicesStatsCollector.class);
        registerCollector(IndexStatsCollector.class);
        registerCollector(ClusterStatsCollector.class);
        registerCollector(ClusterStateCollector.class);
        registerCollector(ShardsCollector.class);
        registerCollector(NodeStatsCollector.class);
        registerCollector(IndexRecoveryCollector.class);
    }

    @Override
    protected void configure() {
        Multibinder<Collector> binder = Multibinder.newSetBinder(binder(), Collector.class);
        for (Class<? extends Collector> collector : collectors) {
            bind(collector).asEagerSingleton();
            binder.addBinding().to(collector);
        }
    }

    public void registerCollector(Class<? extends Collector> collector) {
        collectors.add(collector);
    }
}