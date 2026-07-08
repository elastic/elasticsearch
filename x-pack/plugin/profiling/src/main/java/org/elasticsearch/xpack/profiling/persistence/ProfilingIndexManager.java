/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.persistence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;

/**
 * Manages K/V indices for Elastic Universal Profiling. K/V indices have been superseded by data streams
 * (managed by {@link ProfilingDataStreamManager}). This class is retained only so that
 * {@link AbstractProfilingPersistenceManager} compiles with a concrete type parameter; it manages no indices.
 */
public class ProfilingIndexManager extends AbstractProfilingPersistenceManager<ProfilingIndexManager.ProfilingIndex> {
    public static final List<ProfilingIndex> PROFILING_INDICES = List.of();

    public ProfilingIndexManager(
        ThreadPool threadPool,
        Client client,
        ClusterService clusterService,
        IndexStateResolver indexStateResolver,
        ProfilingIndexTemplateRegistry templateRegistry
    ) {
        super(threadPool, client, clusterService, indexStateResolver, templateRegistry);
    }

    @Override
    protected void onIndexState(
        ClusterState clusterState,
        IndexState<ProfilingIndex> indexState,
        ActionListener<? super ActionResponse> listener
    ) {
        // PROFILING_INDICES is empty; this method is never called.
        throw new UnsupportedOperationException("no K/V indices are managed");
    }

    @Override
    protected Iterable<ProfilingIndex> getManagedIndices() {
        return PROFILING_INDICES;
    }

    /**
     * Marker type for the (now empty) K/V index list. Retained so that
     * {@link AbstractProfilingPersistenceManager} compiles with a concrete type parameter.
     */
    public static final class ProfilingIndex implements ProfilingIndexAbstraction {
        private ProfilingIndex() {}

        @Override
        public String getName() {
            return "";
        }

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public List<Migration> getMigrations(int currentIndexTemplateVersion) {
            return List.of();
        }

        @Override
        public IndexMetadata indexMetadata(ClusterState state) {
            return null;
        }
    }

}
