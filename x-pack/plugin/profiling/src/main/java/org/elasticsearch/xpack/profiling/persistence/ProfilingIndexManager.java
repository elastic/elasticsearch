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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Manages K/V indices for Elastic Universal Profiling. K/V indices have been superseded by data streams
 * (managed by {@link ProfilingDataStreamManager}), so no indices are created here. The class is retained
 * because the migration path for existing K/V indices is to delete them manually before switching to data
 * streams — mixed schemas (K/V + data stream for the same name) are not supported.
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

    public enum OnVersionBump {
        DELETE_OLD,
        KEEP_OLD
    }

    /**
     * An index that is used by Universal Profiling.
     */
    public static class ProfilingIndex implements ProfilingIndexAbstraction {
        private final String namePrefix;
        private final int version;
        private final String generation;
        private final OnVersionBump onVersionBump;
        private final List<Migration> migrations;

        static ProfilingIndex regular(String name, int version, OnVersionBump onVersionBump) {
            return regular(name, version, onVersionBump, null);
        }

        static ProfilingIndex regular(String name, int version, OnVersionBump onVersionBump, Migration.Builder builder) {
            List<Migration> migrations = builder != null ? builder.build(version) : null;
            return new ProfilingIndex(name, version, null, onVersionBump, migrations);
        }

        static ProfilingIndex kv(String name, int version) {
            return kv(name, version, null);
        }

        static ProfilingIndex kv(String name, int version, Migration.Builder builder) {
            List<Migration> migrations = builder != null ? builder.build(version) : null;
            // K/V indices will age automatically as per the ILM policy, and we won't force-upgrade them on version bumps
            return new ProfilingIndex(name, version, "000001", OnVersionBump.KEEP_OLD, migrations);
        }

        private ProfilingIndex(String namePrefix, int version, String generation, OnVersionBump onVersionBump, List<Migration> migrations) {
            this.namePrefix = namePrefix;
            this.version = version;
            this.generation = generation;
            this.onVersionBump = onVersionBump;
            this.migrations = migrations;
        }

        public ProfilingIndex withVersion(int version) {
            return new ProfilingIndex(namePrefix, version, generation, onVersionBump, migrations);
        }

        public ProfilingIndex withGeneration(String generation) {
            return new ProfilingIndex(namePrefix, version, generation, onVersionBump, migrations);
        }

        public boolean isMatchWithoutVersion(String indexName) {
            String expectedPrefix = "." + namePrefix + "-v";
            return indexName.startsWith(expectedPrefix) && isVersionNumber(indexName, expectedPrefix.length());
        }

        private boolean isVersionNumber(String name, int startIndex) {
            final int versionNumberLength = 3;
            String versionNumberCandidate = name.substring(startIndex, Math.min(startIndex + versionNumberLength, name.length()));
            return versionNumberCandidate.length() == versionNumberLength
                // do an explicit range check here for latin digits as Character#isDigit() also considers other
                // Unicode digit characters that we don't want to recognize here.
                && versionNumberCandidate.chars().allMatch((c) -> '0' <= c && c <= '9');
        }

        public boolean isMatchWithoutGeneration(String indexName) {
            return indexName.startsWith(indexPrefix());
        }

        public boolean isFullMatch(String indexName) {
            return toString().equals(indexName);
        }

        public boolean isKvIndex() {
            return generation != null;
        }

        public String getAlias() {
            return namePrefix;
        }

        @Override
        public String getName() {
            return isKvIndex() ? String.format(Locale.ROOT, "%s-%s", indexPrefix(), generation) : indexPrefix();
        }

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
            Map<String, IndexMetadata> indicesMetadata = state.metadata().getProject().indices();
            if (indicesMetadata == null) {
                return null;
            }
            IndexMetadata metadata = indicesMetadata.get(this.toString());
            // prioritize the most recent generation from the current version
            if (metadata == null && isKvIndex()) {
                metadata = indicesMetadata.entrySet()
                    .stream()
                    .filter(e -> isMatchWithoutGeneration(e.getKey()))
                    // use the most recent index to make sure we use the most recent version info from the _meta field
                    .max(Comparator.comparingLong(e -> e.getValue().getCreationDate()))
                    .map(Map.Entry::getValue)
                    .orElse(null);
            }

            // attempt to find an index from an earlier generation
            if (metadata == null) {
                metadata = indicesMetadata.entrySet()
                    .stream()
                    .filter(e -> isMatchWithoutVersion(e.getKey()))
                    // use the most recent index to make sure we use the most recent version info from the _meta field
                    .max(Comparator.comparingLong(e -> e.getValue().getCreationDate()))
                    .map(Map.Entry::getValue)
                    .orElse(null);
            }

            return metadata;
        }

        public OnVersionBump getOnVersionBump() {
            return onVersionBump;
        }

        private String indexPrefix() {
            return String.format(Locale.ROOT, ".%s-v%03d", namePrefix, version);
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
            ProfilingIndex index = (ProfilingIndex) o;
            return version == index.version
                && Objects.equals(namePrefix, index.namePrefix)
                && Objects.equals(generation, index.generation)
                && onVersionBump == index.onVersionBump;
        }

        @Override
        public int hashCode() {
            return Objects.hash(namePrefix, version, generation, onVersionBump);
        }
    }

    /**
     * The index name prefixes for the three profiling patterns that have been migrated from K/V indices to data streams.
     * If any index with these prefixes exists in the cluster, the user needs to delete them to allow the data stream
     * templates to take effect.
     */
    private static final List<String> DS_MIGRATED_KV_PREFIXES = List.of(
        ".profiling-executables-v",
        ".profiling-stacktraces-v",
        ".profiling-stackframes-v"
    );

    /**
     * Returns {@code true} if the cluster contains legacy K/V indices for any of the profiling patterns that have been
     * migrated to data streams. While ES prevents an alias and a data stream with the same name from coexisting (the
     * ingest path will fail if the alias blocks DS creation), this check surfaces the blocked state proactively in the
     * status API so users know they must delete the K/V indices to complete the migration.
     */
    public static boolean hasLegacyKvIndices(ClusterState state) {
        Map<String, IndexMetadata> indices = state.metadata().getProject().indices();
        return DS_MIGRATED_KV_PREFIXES.stream().anyMatch(prefix -> indices.keySet().stream().anyMatch(name -> name.startsWith(prefix)));
    }

    public static boolean isAllResourcesCreated(ClusterState state, IndexStateResolver indexStateResolver) {
        for (ProfilingIndex profilingIndex : PROFILING_INDICES) {
            if (indexStateResolver.getIndexState(state, profilingIndex).getStatus() != IndexStatus.UP_TO_DATE) {
                return false;
            }
        }
        return true;
    }

    public static boolean isAnyResourceTooOld(ClusterState state, IndexStateResolver indexStateResolver) {
        for (ProfilingIndex profilingIndex : PROFILING_INDICES) {
            if (indexStateResolver.getIndexState(state, profilingIndex).getStatus() == IndexStatus.TOO_OLD) {
                return true;
            }
        }
        return false;
    }
}
