/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.indiceswriteloadtracker;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

public class IndicesWriteLoadTrackerPlugin extends Plugin {
    private final SetOnce<IndicesWriteLoadStatsCollector> indicesWriteLoadsStatsCollectorRef = new SetOnce<>();

    public IndicesWriteLoadTrackerPlugin() {}

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer
    ) {
        final var indicesWriteLoadStatsCollector = new IndicesWriteLoadStatsCollector(
            clusterService,
            nodeEnvironment.nodeId(),
            System::nanoTime
        );
        indicesWriteLoadsStatsCollectorRef.set(indicesWriteLoadStatsCollector);

        return Collections.emptyList();
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        assert indicesWriteLoadsStatsCollectorRef.get() != null;
        indexModule.addIndexEventListener(indicesWriteLoadsStatsCollectorRef.get());
    }

    static boolean currentThreadIsWriterLoadCollectorThreadOrTestThread() {
        return Thread.currentThread().getName().startsWith("TEST-");
    }
}
