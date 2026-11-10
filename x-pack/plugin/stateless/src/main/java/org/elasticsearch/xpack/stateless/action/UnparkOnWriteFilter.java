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
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.Index;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.stateless.allocation.ParkedIndexAllocationDecider;
import org.elasticsearch.xpack.stateless.recovery.FastSearchActivationSettings;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Parking prototype, phase 3 ("both tiers to zero"), default off: a bulk (or single-document index/delete, which runs as a bulk) or an
 * {@code _update} (which waits for the primary before it issues its bulk) to an index whose indexing shard is parked
 * ({@code index.stateless.parked: true}) unparks it by clearing the setting, then lets the request proceed unchanged. The write
 * waits for the primary in {@code TransportReplicationAction.ReroutePhase} (request timeout, default 1m) while the settings update
 * reroutes and the primary recovers from the object store, so the filter never blocks the request path itself.
 * At most one unpark per index is in flight on this node; other writes just proceed and wait in the same way. Only the real bulk and
 * update actions act, not {@code SimulateBulkAction} (whose request is a {@link BulkRequest} too but must not change state). The index is
 * resolved in the request's project ({@link ProjectResolver}).
 * <p>
 * Prototype limitations: the settings update runs in the caller's thread context (no internal origin, so it needs a user allowed to
 * update index settings when security is on), and only concrete index names, aliases and data streams with a write index are resolved
 * (no date math, no wildcards).
 */
public class UnparkOnWriteFilter implements ActionFilter {

    private static final Logger logger = LogManager.getLogger(UnparkOnWriteFilter.class);

    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "stateless.parking.unpark_on_write.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final Supplier<ClusterService> clusterService;
    private final Supplier<Client> client;
    private final Supplier<ProjectResolver> projectResolver;
    private final Set<Index> unparking = ConcurrentCollections.newConcurrentSet();

    /** Suppliers because action filters are created before the plugin's components. */
    public UnparkOnWriteFilter(
        Supplier<ClusterService> clusterService,
        Supplier<Client> client,
        Supplier<ProjectResolver> projectResolver
    ) {
        this.clusterService = clusterService;
        this.client = client;
        this.projectResolver = projectResolver;
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
        final List<? extends DocWriteRequest<?>> writes;
        if (request instanceof BulkRequest bulkRequest && TransportBulkAction.NAME.equals(action)) {
            writes = bulkRequest.requests();
        } else if (request instanceof UpdateRequest updateRequest && TransportUpdateAction.NAME.equals(action)) {
            writes = List.of(updateRequest);
        } else {
            writes = List.of();
        }
        if (writes.isEmpty() == false) {
            final ClusterService cs = clusterService.get();
            if (cs != null
                && client.get() != null
                && projectResolver.get() != null
                && FastSearchActivationSettings.getOrDefault(cs.getClusterSettings(), ENABLED_SETTING)) {
                unparkTargets(projectResolver.get().getProjectMetadata(cs.state()), writes);
            }
        }
        chain.proceed(task, action, request, listener);
    }

    private void unparkTargets(ProjectMetadata project, List<? extends DocWriteRequest<?>> writes) {
        for (DocWriteRequest<?> item : writes) {
            final IndexMetadata indexMetadata = writeIndex(project, item.index());
            if (indexMetadata != null
                && ParkedIndexAllocationDecider.PARKED_SETTING.get(indexMetadata.getSettings())
                && unparking.add(indexMetadata.getIndex())) {
                final Index index = indexMetadata.getIndex();
                logger.debug("{} write to a parked index, unparking its indexing shard", index);
                final var request = new UpdateSettingsRequest(
                    Settings.builder().put(ParkedIndexAllocationDecider.PARKED_SETTING.getKey(), false).build(),
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
