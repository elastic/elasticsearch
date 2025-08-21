/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.task;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.plugins.SystemIndexPlugin;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Stream;

abstract sealed class SystemResourceMigrationInfo implements Comparable<SystemResourceMigrationInfo> permits SystemDataStreamMigrationInfo,
    SystemIndexMigrationInfo {
    private static final Comparator<SystemResourceMigrationInfo> SAME_CLASS_COMPARATOR = Comparator.comparing(
        SystemResourceMigrationInfo::getFeatureName
    ).thenComparing(SystemResourceMigrationInfo::getCurrentResourceName);

    protected final String featureName;
    protected final String origin;
    protected final SystemIndices.Feature owningFeature;

    SystemResourceMigrationInfo(String featureName, String origin, SystemIndices.Feature owningFeature) {
        this.featureName = featureName;
        this.origin = origin;
        this.owningFeature = owningFeature;
    }

    protected abstract String getCurrentResourceName();

    /**
     * Gets the name of the feature which owns the index to be migrated.
     */
    String getFeatureName() {
        return featureName;
    }

    /**
     * Gets the origin that should be used when interacting with this index.
     */
    String getOrigin() {
        return origin;
    }

    /**
     * Creates a client that's been configured to be able to properly access the system index to be migrated.
     *
     * @param baseClient The base client to wrap.
     * @return An {@link OriginSettingClient} which uses the origin provided by {@link SystemIndexMigrationInfo#getOrigin()}.
     */
    Client createClient(Client baseClient) {
        return new OriginSettingClient(baseClient, this.getOrigin());
    }

    abstract Stream<IndexMetadata> getIndices(ProjectMetadata metadata);

    @Override
    public int compareTo(SystemResourceMigrationInfo o) {
        return SAME_CLASS_COMPARATOR.compare(this, o);
    }

    abstract boolean isCurrentIndexClosed();

    /**
     * Invokes the pre-migration hook for the feature that owns this index.
     * See {@link SystemIndexPlugin#prepareForIndicesMigration(ClusterService, Client, ActionListener)}.
     * @param clusterService For retrieving the state.
     * @param client For performing any update operations necessary to prepare for the upgrade.
     * @param listener Call {@link ActionListener#onResponse(Object)} when preparation for migration is complete.
     */
    void prepareForIndicesMigration(ClusterService clusterService, Client client, ActionListener<Map<String, Object>> listener) {
        owningFeature.getPreMigrationFunction().prepareForIndicesMigration(clusterService, client, listener);
    }

    /**
     * Invokes the post-migration hooks for the feature that owns this index.
     * See {@link SystemIndexPlugin#indicesMigrationComplete(Map, ClusterService, Client, ActionListener)}.
     * @param metadata The metadata that was passed into the listener by the pre-migration hook.
     * @param clusterService For retrieving the state.
     * @param client For performing any update operations necessary to prepare for the upgrade.
     * @param listener Call {@link ActionListener#onResponse(Object)} when the hook is finished.
     */
    void indicesMigrationComplete(
        Map<String, Object> metadata,
        ClusterService clusterService,
        Client client,
        ActionListener<Boolean> listener
    ) {
        owningFeature.getPostMigrationFunction().indicesMigrationComplete(metadata, clusterService, client, listener);
    }
}
