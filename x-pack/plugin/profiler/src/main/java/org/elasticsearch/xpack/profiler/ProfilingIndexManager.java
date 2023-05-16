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
import java.util.Collections;
import java.util.HashMap;
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
    public static final Map<String, String> INDICES_AND_ALIASES;

    static {
        String versionSuffix = "-v" + ProfilingIndexTemplateRegistry.INDEX_TEMPLATE_VERSION;

        Map<String, String> indicesAndAliases = new HashMap<>();
        // TODO: Define behavior on upgrade (delete, reindex, ...), to be done after 8.9.0
        // TODO: This index will be gone with the 8.9 release. Don't bother to implement versioning support.
        indicesAndAliases.put(".profiling-ilm-lock", null);
        indicesAndAliases.put(".profiling-returnpads-private" + versionSuffix, "profiling-returnpads-private");
        indicesAndAliases.put(".profiling-sq-executables" + versionSuffix, "profiling-sq-executables");
        indicesAndAliases.put(".profiling-sq-leafframes" + versionSuffix, "profiling-sq-leafframes");
        indicesAndAliases.put(".profiling-symbols" + versionSuffix, "profiling-symbols");
        indicesAndAliases.put(".profiling-symbols-private" + versionSuffix, "profiling-symbols-private");
        // TODO: Update these to the new K/V strategy after all readers have been adjusted
        String[] kvIndices = new String[] { "profiling-executables", "profiling-stackframes", "profiling-stacktraces" };
        for (String idx : kvIndices) {
            indicesAndAliases.put(idx + versionSuffix + "-000001", idx);
            indicesAndAliases.put(idx + versionSuffix + "-000002", idx + "-next");
        }
        INDICES_AND_ALIASES = Collections.unmodifiableMap(indicesAndAliases);
    }

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
        for (Map.Entry<String, String> idxAlias : INDICES_AND_ALIASES.entrySet()) {
            String index = idxAlias.getKey();
            String alias = idxAlias.getValue();
            final AtomicBoolean creationInProgress = creationInProgressPerIndex.computeIfAbsent(index, key -> new AtomicBoolean(false));
            if (creationInProgress.compareAndSet(false, true)) {
                final boolean indexNeedsToBeCreated = indicesMetadata == null || indicesMetadata.get(index) == null;
                if (indexNeedsToBeCreated) {
                    logger.debug("adding index [{}], because it doesn't exist", index);
                    putIndex(index, alias, creationInProgress);
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
}
