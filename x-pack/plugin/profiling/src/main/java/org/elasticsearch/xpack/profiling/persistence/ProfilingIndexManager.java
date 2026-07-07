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
import java.util.Map;

/**
 * Manages K/V indices for Elastic Universal Profiling. K/V indices have been superseded by data streams
 * (managed by {@link ProfilingDataStreamManager}). This class is retained only to detect legacy K/V
 * indices still present in the cluster and surface them via the status API.
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

    private static final List<String> DS_MIGRATED_KV_PREFIXES = List.of(
        ".profiling-executables-v",
        ".profiling-stacktraces-v",
        ".profiling-stackframes-v"
    );

    /**
     * Returns {@code true} if the cluster still contains legacy K/V indices for any of the three
     * profiling patterns that have been migrated to data streams. While ES prevents an alias and a
     * data stream with the same name from coexisting, this check surfaces the blocked state
     * proactively in the status API so operators know they must delete the K/V indices to complete
     * the migration.
     */
    public static boolean hasLegacyKvIndices(ClusterState state) {
        Map<String, IndexMetadata> indices = state.metadata().getProject().indices();
        return DS_MIGRATED_KV_PREFIXES.stream().anyMatch(prefix -> indices.keySet().stream().anyMatch(name -> name.startsWith(prefix)));
    }
}
