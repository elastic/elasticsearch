/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;

import java.io.Closeable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Creates all indices that are required for using Elastic Universal Profiling.
 */
public class ProfilingIndexManager implements ClusterStateListener, Closeable {
    private static final Logger logger = LogManager.getLogger(ProfilingIndexManager.class);
    // For testing
    public static final List<ProfilingIndex> PROFILING_INDICES = List.of(
        ProfilingIndex.regular("profiling-returnpads-private"),
        ProfilingIndex.regular("profiling-sq-executables"),
        ProfilingIndex.regular("profiling-sq-leafframes"),
        ProfilingIndex.regular("profiling-symbols-private"),
        ProfilingIndex.kv("profiling-executables"),
        ProfilingIndex.kv("profiling-stackframes"),
        ProfilingIndex.kv("profiling-stacktraces"),
        ProfilingIndex.kv("profiling-symbols-global")
    );

    private final ThreadPool threadPool;
    private final Client client;
    private final ClusterService clusterService;
    private final ConcurrentMap<String, AtomicBoolean> creationInProgressPerIndex = new ConcurrentHashMap<>();
    private volatile boolean templatesEnabled;

    public ProfilingIndexManager(ThreadPool threadPool, Client client, ClusterService clusterService) {
        this.threadPool = threadPool;
        this.client = client;
        this.clusterService = clusterService;
    }

    public void initialize() {
        clusterService.addListener(this);
    }

    @Override
    public void close() {
        clusterService.removeListener(this);
    }

    public void setTemplatesEnabled(boolean templatesEnabled) {
        this.templatesEnabled = templatesEnabled;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (templatesEnabled == false) {
            return;
        }
        // wait for the cluster state to be recovered
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        // If this node is not a master node, exit.
        if (event.state().nodes().isLocalNodeElectedMaster() == false) {
            return;
        }

        if (event.state().nodes().getMaxNodeVersion().after(event.state().nodes().getSmallestNonClientNodeVersion())) {
            logger.debug("Skipping up-to-date check as cluster has mixed versions");
            return;
        }

        // ensure that index templates are present
        if (isAllTemplatesCreated(event) == false) {
            logger.trace("Skipping index creation; not all templates are present yet");
            return;
        }

        addIndicesIfMissing(event.state());
    }

    protected boolean isAllTemplatesCreated(ClusterChangedEvent event) {
        return ProfilingIndexTemplateRegistry.areAllTemplatesCreated(event.state());
    }

    private void addIndicesIfMissing(ClusterState state) {
        Map<String, IndexMetadata> indicesMetadata = state.metadata().indices();
        for (ProfilingIndex profilingIndex : PROFILING_INDICES) {
            String index = profilingIndex.toString();
            final AtomicBoolean creationInProgress = creationInProgressPerIndex.computeIfAbsent(index, key -> new AtomicBoolean(false));
            if (creationInProgress.compareAndSet(false, true)) {
                // Do a quick (exact) check first
                boolean indexNeedsToBeCreated = indicesMetadata == null || indicesMetadata.get(index) == null;
                // for K/V indices we must not create the index if a newer generation exists
                if (indexNeedsToBeCreated && profilingIndex.isKvIndex()) {
                    indexNeedsToBeCreated = indicesMetadata != null
                        && indicesMetadata.keySet().stream().anyMatch(profilingIndex::isMatchWithoutGeneration) == false;
                }
                if (indexNeedsToBeCreated) {
                    logger.debug("adding index [{}], because it doesn't exist", index);
                    putIndex(index, profilingIndex.getAlias(), creationInProgress);
                } else {
                    logger.trace("not adding index [{}], because it already exists", index);
                    creationInProgress.set(false);
                }
            }
        }
    }

    private void onPutIndexFailure(String index, Exception ex) {
        logger.error(() -> format("error adding index [%s] for [%s]", index, ClientHelper.PROFILING_ORIGIN), ex);
    }

    private void putIndex(final String index, final String alias, final AtomicBoolean creationCheck) {
        final Executor executor = threadPool.generic();
        executor.execute(() -> {
            CreateIndexRequest request = new CreateIndexRequest(index);
            if (alias != null) {
                try {
                    Map<String, Object> sourceAsMap = Map.of("aliases", Map.of(alias, Map.of("is_write_index", true)));
                    request.source(sourceAsMap, LoggingDeprecationHandler.INSTANCE);
                } catch (Exception ex) {
                    creationCheck.set(false);
                    onPutIndexFailure(index, ex);
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
                        creationCheck.set(false);
                        if (response.isAcknowledged() == false) {
                            logger.error(
                                "error adding index [{}] for [{}], request was not acknowledged",
                                index,
                                ClientHelper.PROFILING_ORIGIN
                            );
                        } else if (response.isShardsAcknowledged() == false) {
                            logger.warn("adding index [{}] for [{}], shards were not acknowledged", index, ClientHelper.PROFILING_ORIGIN);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        creationCheck.set(false);
                        onPutIndexFailure(index, e);
                    }
                },
                (req, listener) -> client.admin().indices().create(req, listener)
            );
        });
    }

    /**
     * An index that is used by Universal Profiling.
     */
    static class ProfilingIndex {
        private final String name;
        private final String generation;

        public static ProfilingIndex regular(String name) {
            return new ProfilingIndex(name, null);
        }

        public static ProfilingIndex kv(String name) {
            return new ProfilingIndex(name, "000001");
        }

        private ProfilingIndex(String namePrefix, String generation) {
            this.name = namePrefix;
            this.generation = generation;
        }

        public boolean isMatchWithoutGeneration(String indexName) {
            return indexName.startsWith(indexPrefix());
        }

        public boolean isKvIndex() {
            return generation != null;
        }

        public String getAlias() {
            return name;
        }

        private String indexPrefix() {
            return String.format(Locale.ROOT, ".%s-v%03d", name, ProfilingIndexTemplateRegistry.INDEX_TEMPLATE_VERSION);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(indexPrefix());
            if (generation != null) {
                sb.append("-");
                sb.append(generation);
            }
            return sb.toString();
        }
    }
}
