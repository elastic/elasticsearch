/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.Index;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.stateless.allocation.CheapShardParkedAllocationDecider;

import java.util.Set;
import java.util.function.Supplier;

/**
 * Cheap shards, phase 3 ("both tiers to zero"), default off: a bulk (or single-document write, which runs as a bulk) to an index whose
 * indexing shard is parked ({@code index.cheap_shard.parked: true}) unparks it by clearing the setting, then lets the request proceed
 * unchanged. The write waits for the primary in {@code TransportReplicationAction.ReroutePhase} (request timeout, default 1m) while the
 * settings update reroutes and the primary recovers from the object store, so the filter never blocks the request path itself.
 * At most one unpark per index is in flight on this node; other writes just proceed and wait in the same way.
 * <p>
 * Prototype limitations: the settings update runs in the caller's thread context (no internal origin, so it needs a user allowed to
 * update index settings when security is on), and only concrete index names, aliases and data streams with a write index are resolved
 * (no date math, no wildcards).
 */
public class CheapShardUnparkOnWriteFilter implements ActionFilter {

    private static final Logger logger = LogManager.getLogger(CheapShardUnparkOnWriteFilter.class);

    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "stateless.cheap_shard.unpark_on_write.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final Supplier<ClusterService> clusterService;
    private final Supplier<Client> client;
    private final Set<Index> unparking = ConcurrentCollections.newConcurrentSet();

    /** Suppliers because action filters are created before the plugin's components. */
    public CheapShardUnparkOnWriteFilter(Supplier<ClusterService> clusterService, Supplier<Client> client) {
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    public int order() {
        return Integer.MAX_VALUE; // after security & co: only requests that are going to run trigger an unpark
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        if (request instanceof BulkRequest bulkRequest) {
            final ClusterService cs = clusterService.get();
            if (cs != null && cs.getClusterSettings().get(ENABLED_SETTING)) {
                unparkTargets(cs, bulkRequest);
            }
        }
        chain.proceed(task, action, request, listener);
    }

    private void unparkTargets(ClusterService cs, BulkRequest bulkRequest) {
        final ProjectMetadata project = cs.state().metadata().getProject();
        for (DocWriteRequest<?> item : bulkRequest.requests()) {
            final IndexMetadata indexMetadata = writeIndex(project, item.index());
            if (indexMetadata != null
                && CheapShardParkedAllocationDecider.PARKED_SETTING.get(indexMetadata.getSettings())
                && unparking.add(indexMetadata.getIndex())) {
                final Index index = indexMetadata.getIndex();
                logger.debug("{} write to a parked index, unparking its indexing shard", index);
                final var request = new UpdateSettingsRequest(
                    Settings.builder().put(CheapShardParkedAllocationDecider.PARKED_SETTING.getKey(), false).build(),
                    index.getName()
                );
                client.get()
                    .admin()
                    .indices()
                    .updateSettings(
                        request,
                        ActionListener.runAfter(
                            ActionListener.<AcknowledgedResponse>wrap(r -> {}, e -> logger.warn(() -> "failed to unpark " + index, e)),
                            () -> unparking.remove(index)
                        )
                    );
            }
        }
    }

    private static IndexMetadata writeIndex(ProjectMetadata project, String name) {
        if (name == null) {
            return null;
        }
        final IndexAbstraction abstraction = project.getIndicesLookup().get(name);
        if (abstraction == null) {
            return null;
        }
        final Index writeIndex = abstraction.getWriteIndex();
        return writeIndex == null ? null : project.index(writeIndex);
    }
}
