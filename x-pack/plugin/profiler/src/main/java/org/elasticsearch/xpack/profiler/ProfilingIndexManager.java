/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Creates all indices that are required for using Elastic Universal Profiling.
 */
public class ProfilingIndexManager extends AbstractProfilingPersistenceManager<ProfilingIndexManager.ProfilingIndex> {
    // For testing
    public static final List<ProfilingIndex> PROFILING_INDICES = List.of(
        ProfilingIndex.regular(
            "profiling-returnpads-private",
            ProfilingIndexTemplateRegistry.PROFILING_RETURNPADS_PRIVATE_VERSION,
            OnVersionBump.KEEP_OLD
        ),
        ProfilingIndex.regular(
            "profiling-sq-executables",
            ProfilingIndexTemplateRegistry.PROFILING_SQ_EXECUTABLES_VERSION,
            OnVersionBump.DELETE_OLD
        ),
        ProfilingIndex.regular(
            "profiling-sq-leafframes",
            ProfilingIndexTemplateRegistry.PROFILING_SQ_LEAFFRAMES_VERSION,
            OnVersionBump.DELETE_OLD
        ),
        ProfilingIndex.regular(
            "profiling-symbols-private",
            ProfilingIndexTemplateRegistry.PROFILING_SYMBOLS_VERSION,
            OnVersionBump.KEEP_OLD
        ),
        ProfilingIndex.kv("profiling-executables", ProfilingIndexTemplateRegistry.PROFILING_EXECUTABLES_VERSION),
        ProfilingIndex.kv("profiling-stackframes", ProfilingIndexTemplateRegistry.PROFILING_STACKFRAMES_VERSION),
        ProfilingIndex.kv("profiling-stacktraces", ProfilingIndexTemplateRegistry.PROFILING_STACKTRACES_VERSION),
        ProfilingIndex.kv("profiling-symbols-global", ProfilingIndexTemplateRegistry.PROFILING_SYMBOLS_VERSION)
    );

    private final ThreadPool threadPool;
    private final Client client;

    public ProfilingIndexManager(ThreadPool threadPool, Client client, ClusterService clusterService) {
        super(clusterService);
        this.threadPool = threadPool;
        this.client = client;
    }

    @Override
    protected void onStatus(
        ClusterState clusterState,
        Status status,
        ProfilingIndex index,
        ActionListener<? super ActionResponse> listener
    ) {
        switch (status) {
            case NEEDS_CREATION -> createIndex(clusterState, index, listener);
            case NEEDS_VERSION_BUMP -> bumpVersion(clusterState, index, listener);
            default -> {
                logger.debug("Skipping status change [{}] for index [{}].", status, index);
                // ensure that listener is notified we're done
                listener.onResponse(null);
            }
        }
    }

    @Override
    protected IndexMetadata indexMetadata(ClusterState state, ProfilingIndex index) {
        Map<String, IndexMetadata> indicesMetadata = state.metadata().indices();
        if (indicesMetadata == null) {
            return null;
        }
        IndexMetadata metadata = indicesMetadata.get(index.toString());
        // prioritize the most recent generation from the current version
        if (metadata == null && index.isKvIndex()) {
            metadata = indicesMetadata.entrySet()
                .stream()
                .filter(e -> index.isMatchWithoutGeneration(e.getKey()))
                // use the most recent index to make sure we use the most recent version info from the _meta field
                .max(Comparator.comparingLong(e -> e.getValue().getCreationDate()))
                .map(Map.Entry::getValue)
                .orElse(null);
        }

        // attempt to find an index from an earlier generation
        if (metadata == null) {
            metadata = indicesMetadata.entrySet()
                .stream()
                .filter(e -> index.isMatchWithoutVersion(e.getKey()))
                // use the most recent index to make sure we use the most recent version info from the _meta field
                .max(Comparator.comparingLong(e -> e.getValue().getCreationDate()))
                .map(Map.Entry::getValue)
                .orElse(null);
        }

        return metadata;
    }

    private void bumpVersion(ClusterState state, ProfilingIndex index, ActionListener<? super ActionResponse> listener) {
        if (index.getOnVersionBump() == OnVersionBump.DELETE_OLD) {
            Map<String, IndexMetadata> indicesMetadata = state.metadata().indices();
            List<String> priorIndexVersions = indicesMetadata.keySet()
                .stream()
                // ignore the current index and look only for old versions
                .filter(Predicate.not(index::isFullMatch))
                .filter(index::isMatchWithoutVersion)
                .toList();
            if (priorIndexVersions.isEmpty() == false) {
                logger.debug("deleting indices [{}] on index version bump for [{}].", priorIndexVersions, index.getAlias());
                deleteIndices(
                    priorIndexVersions.toArray(new String[0]),
                    // the cluster state that we are operating on is a snapshot and won't reflect that the alias has just gone.
                    // Therefore, we use putIndex here which does not check for the existence of an alias
                    ActionListener.wrap(r -> putIndex(index.getName(), index.getAlias(), listener), listener::onFailure)
                );
            } else {
                createIndex(state, index, listener);
            }
        } else {
            createIndex(state, index, listener);
        }
    }

    @Override
    protected Iterable<ProfilingIndex> getManagedIndices() {
        return PROFILING_INDICES;
    }

    private void onCreateIndexFailure(String index, Exception ex) {
        logger.error(() -> format("error adding index [%s] for [%s]", index, ClientHelper.PROFILING_ORIGIN), ex);
    }

    private void createIndex(final ClusterState state, final ProfilingIndex index, final ActionListener<? super ActionResponse> listener) {
        if (state.metadata().hasAlias(index.getAlias())) {
            // there is an existing index from a prior version. Use the rollover API to move the write alias atomically. This has the
            // following implications:
            //
            // * A new index will be created according to the currently installed version of the matching index template.
            // * The write alias will point to that index.
            // * The prior index will continue to be managed by ILM but will advance to the next phase after rollover. As
            // rollover blocks phase transitions, the prior index may move a bit sooner than expected to the warm tier
            // after version bumps; still all conditions need to be met, it's just that due to the earlier rollover, the
            // condition will be reached sooner than without a version bump.
            rolloverIndex(index.getName(), index.getAlias(), listener);
        } else {
            // newly create index
            putIndex(index.getName(), index.getAlias(), listener);
        }
    }

    private void rolloverIndex(final String newIndex, final String alias, ActionListener<? super ActionResponse> listener) {
        logger.debug("rolling over to index [{}] for alias [{}].", newIndex, alias);
        final Executor executor = threadPool.generic();
        executor.execute(() -> {
            RolloverRequest request = new RolloverRequest(alias, newIndex);
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                ClientHelper.PROFILING_ORIGIN,
                request,
                new ActionListener<RolloverResponse>() {
                    @Override
                    public void onResponse(RolloverResponse response) {
                        if (response.isAcknowledged() == false) {
                            logger.error(
                                "error rolling over index [{}] for [{}], request was not acknowledged",
                                newIndex,
                                ClientHelper.PROFILING_ORIGIN
                            );
                        } else if (response.isShardsAcknowledged() == false) {
                            logger.warn(
                                "rolling over index [{}] for [{}], shards were not acknowledged",
                                newIndex,
                                ClientHelper.PROFILING_ORIGIN
                            );
                        } else if (response.isRolledOver() == false) {
                            logger.warn(
                                "could not rollover alias [{}] to index [{}] for [{}].",
                                alias,
                                newIndex,
                                ClientHelper.PROFILING_ORIGIN
                            );
                        } else {
                            logger.debug(
                                "rolled over alias [{}] from [{}] to index [{}] for [{}].",
                                alias,
                                response.getOldIndex(),
                                response.getNewIndex(),
                                ClientHelper.PROFILING_ORIGIN
                            );
                        }
                        listener.onResponse(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onCreateIndexFailure(newIndex, e);
                        listener.onFailure(e);
                    }
                },
                (req, l) -> client.admin().indices().rolloverIndex(req, l)
            );
        });
    }

    private void putIndex(final String index, final String alias, final ActionListener<? super ActionResponse> listener) {
        final Executor executor = threadPool.generic();
        executor.execute(() -> {
            CreateIndexRequest request = new CreateIndexRequest(index);
            if (alias != null) {
                try {
                    Map<String, Object> sourceAsMap = Map.of("aliases", Map.of(alias, Map.of("is_write_index", true)));
                    request.source(sourceAsMap, LoggingDeprecationHandler.INSTANCE);
                } catch (Exception ex) {
                    onCreateIndexFailure(index, ex);
                    listener.onFailure(ex);
                    return;
                }
            }
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                ClientHelper.PROFILING_ORIGIN,
                request,
                new ActionListener<CreateIndexResponse>() {
                    @Override
                    public void onResponse(CreateIndexResponse response) {
                        if (response.isAcknowledged() == false) {
                            logger.error(
                                "error adding index [{}] for [{}], request was not acknowledged",
                                index,
                                ClientHelper.PROFILING_ORIGIN
                            );
                        } else if (response.isShardsAcknowledged() == false) {
                            logger.warn("adding index [{}] for [{}], shards were not acknowledged", index, ClientHelper.PROFILING_ORIGIN);
                        }
                        listener.onResponse(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onCreateIndexFailure(index, e);
                        listener.onFailure(e);
                    }
                },
                (req, l) -> client.admin().indices().create(req, l)
            );
        });
    }

    private void onDeleteIndexFailure(String[] indices, Exception ex) {
        logger.error(() -> format("error deleting indices [%s] for [%s]", indices, ClientHelper.PROFILING_ORIGIN), ex);
    }

    private void deleteIndices(final String[] indices, final ActionListener<AcknowledgedResponse> listener) {
        final Executor executor = threadPool.generic();
        executor.execute(() -> {
            DeleteIndexRequest request = new DeleteIndexRequest(indices);
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                ClientHelper.PROFILING_ORIGIN,
                request,
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse response) {
                        if (response.isAcknowledged() == false) {
                            logger.error(
                                "error deleting indices [{}] for [{}], request was not acknowledged",
                                indices,
                                ClientHelper.PROFILING_ORIGIN
                            );
                        }
                        listener.onResponse(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onDeleteIndexFailure(indices, e);
                        listener.onFailure(e);
                    }
                },
                (req, l) -> client.admin().indices().delete(req, l)
            );
        });
    }

    enum OnVersionBump {
        DELETE_OLD,
        KEEP_OLD
    }

    /**
     * An index that is used by Universal Profiling.
     */
    static class ProfilingIndex implements ProfilingIndexAbstraction {
        private final String namePrefix;
        private final int version;
        private final String generation;
        private final OnVersionBump onVersionBump;

        public static ProfilingIndex regular(String name, int version, OnVersionBump onVersionBump) {
            return new ProfilingIndex(name, version, null, onVersionBump);
        }

        public static ProfilingIndex kv(String name, int version) {
            // K/V indices will age automatically as per the ILM policy, and we won't force-upgrade them on version bumps
            return new ProfilingIndex(name, version, "000001", OnVersionBump.KEEP_OLD);
        }

        private ProfilingIndex(String namePrefix, int version, String generation, OnVersionBump onVersionBump) {
            this.namePrefix = namePrefix;
            this.version = version;
            this.generation = generation;
            this.onVersionBump = onVersionBump;
        }

        public ProfilingIndex withVersion(int version) {
            return new ProfilingIndex(namePrefix, version, generation, onVersionBump);
        }

        public ProfilingIndex withGeneration(String generation) {
            return new ProfilingIndex(namePrefix, version, generation, onVersionBump);
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
}
