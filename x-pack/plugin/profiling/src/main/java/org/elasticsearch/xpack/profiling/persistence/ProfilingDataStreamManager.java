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
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Manages data streams for Elastic Universal Profiling. Data streams are no longer pre-created at startup;
 * instead, index templates are installed by {@link ProfilingIndexTemplateRegistry} and Elasticsearch
 * auto-creates each data stream on first document ingest. This keeps "Set up Profiling" lightweight —
 * the setup is complete as soon as the templates are in place, with no need to wait for empty shards.
 */
public class ProfilingDataStreamManager extends AbstractProfilingPersistenceManager<ProfilingDataStreamManager.ProfilingDataStream> {
    public static final List<ProfilingDataStream> PROFILING_DATASTREAMS = List.of();

    public ProfilingDataStreamManager(
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
        IndexState<ProfilingDataStream> indexState,
        ActionListener<? super ActionResponse> listener
    ) {
        // PROFILING_DATASTREAMS is empty; this method is never called.
        throw new UnsupportedOperationException("no data streams are managed");
    }

    @Override
    protected Iterable<ProfilingDataStream> getManagedIndices() {
        return PROFILING_DATASTREAMS;
    }

    /**
     * A datastream that is used by Universal Profiling.
     */
    static class ProfilingDataStream implements ProfilingIndexAbstraction {
        private final String name;
        private final int version;
        private final List<Migration> migrations;

        public static ProfilingDataStream of(String name, int version) {
            return of(name, version, null);
        }

        public static ProfilingDataStream of(String name, int version, Migration.Builder builder) {
            List<Migration> migrations = builder != null ? builder.build(version) : null;
            return new ProfilingDataStream(name, version, migrations);
        }

        private ProfilingDataStream(String name, int version, List<Migration> migrations) {
            this.name = name;
            this.version = version;
            this.migrations = migrations;
        }

        public ProfilingDataStream withVersion(int version) {
            return new ProfilingDataStream(name, version, migrations);
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getVersion() {
            return version;
        }

        @Override
        public List<Migration> getMigrations(int currentIndexTemplateVersion) {
            return migrations != null
                ? migrations.stream().filter(m -> m.getTargetIndexTemplateVersion() > currentIndexTemplateVersion).toList()
                : Collections.emptyList();
        }

        @Override
        public IndexMetadata indexMetadata(ClusterState state) {
            Map<String, DataStream> dataStreams = state.metadata().getProject().dataStreams();
            if (dataStreams == null) {
                return null;
            }
            DataStream ds = dataStreams.get(this.getName());
            if (ds == null) {
                return null;
            }
            Index writeIndex = ds.getWriteIndex();
            if (writeIndex == null) {
                return null;
            }
            return state.metadata().getProject().index(writeIndex);
        }

        @Override
        public String toString() {
            return getName();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ProfilingDataStream that = (ProfilingDataStream) o;
            return version == that.version && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, version);
        }
    }

    public static boolean isAllResourcesCreated(ClusterState state, IndexStateResolver indexStateResolver) {
        for (ProfilingDataStream profilingDataStream : PROFILING_DATASTREAMS) {
            if (indexStateResolver.getIndexState(state, profilingDataStream).getStatus() != IndexStatus.UP_TO_DATE) {
                return false;
            }
        }
        return true;
    }

    public static boolean isAnyResourceTooOld(ClusterState state, IndexStateResolver indexStateResolver) {
        for (ProfilingDataStream profilingDataStream : PROFILING_DATASTREAMS) {
            if (indexStateResolver.getIndexState(state, profilingDataStream).getStatus() == IndexStatus.TOO_OLD) {
                return true;
            }
        }
        return false;
    }
}
