/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.upgrades.SystemIndexMigrationTaskParams.SYSTEM_INDEX_UPGRADE_TASK_NAME;

/**
 * Starts the process of migrating system indices. See {@link SystemIndexMigrator} for the actual migration logic.
 */
public class SystemIndexMigrationExecutor extends PersistentTasksExecutor<SystemIndexMigrationTaskParams> {
    private final Client client; // NOTE: *NOT* an OriginSettingClient. We have to do that later.
    private final ClusterService clusterService;
    private final SystemIndices systemIndices;
    private final MetadataUpdateSettingsService metadataUpdateSettingsService;
    private final MetadataCreateIndexService metadataCreateIndexService;
    private final IndexScopedSettings indexScopedSettings;

    public SystemIndexMigrationExecutor(
        Client client,
        ClusterService clusterService,
        SystemIndices systemIndices,
        MetadataUpdateSettingsService metadataUpdateSettingsService,
        MetadataCreateIndexService metadataCreateIndexService,
        IndexScopedSettings indexScopedSettings
    ) {
        super(SYSTEM_INDEX_UPGRADE_TASK_NAME, ThreadPool.Names.GENERIC);
        this.client = client;
        this.clusterService = clusterService;
        this.systemIndices = systemIndices;
        this.metadataUpdateSettingsService = metadataUpdateSettingsService;
        this.metadataCreateIndexService = metadataCreateIndexService;
        this.indexScopedSettings = indexScopedSettings;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, SystemIndexMigrationTaskParams params, PersistentTaskState state) {
        SystemIndexMigrator upgrader = (SystemIndexMigrator) task;
        SystemIndexMigrationTaskState upgraderState = (SystemIndexMigrationTaskState) state;
        upgrader.run(upgraderState);
    }

    @Override
    protected AllocatedPersistentTask createTask(
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        PersistentTasksCustomMetadata.PersistentTask<SystemIndexMigrationTaskParams> taskInProgress,
        Map<String, String> headers
    ) {
        return new SystemIndexMigrator(
            client,
            id,
            type,
            action,
            parentTaskId,
            taskInProgress.getParams(),
            headers,
            clusterService,
            systemIndices,
            metadataUpdateSettingsService,
            metadataCreateIndexService,
            indexScopedSettings
        );
    }

    @Override
    public PersistentTasksCustomMetadata.Assignment getAssignment(
        SystemIndexMigrationTaskParams params,
        Collection<DiscoveryNode> candidateNodes,
        ClusterState clusterState
    ) {
        // This should select from master-eligible nodes because we already require all master-eligible nodes to have all plugins installed.
        // However, due to a misunderstanding, this code as-written needs to run on the master node in particular. This is not a fundamental
        // problem, but more that you can't submit cluster state update tasks from non-master nodes. If we translate the process of updating
        // the cluster state to a Transport action, we can revert this to selecting any master-eligible node.
        DiscoveryNode discoveryNode = clusterState.nodes().getMasterNode();
        if (discoveryNode == null) {
            return NO_NODE_FOUND;
        } else {
            return new PersistentTasksCustomMetadata.Assignment(discoveryNode.getId(), "");
        }
    }

    public static List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return List.of(
            new NamedXContentRegistry.Entry(
                PersistentTaskParams.class,
                new ParseField(SystemIndexMigrationTaskParams.SYSTEM_INDEX_UPGRADE_TASK_NAME),
                SystemIndexMigrationTaskParams::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                PersistentTaskState.class,
                new ParseField(SystemIndexMigrationTaskParams.SYSTEM_INDEX_UPGRADE_TASK_NAME),
                SystemIndexMigrationTaskState::fromXContent
            )
        );
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(PersistentTaskState.class, SYSTEM_INDEX_UPGRADE_TASK_NAME, SystemIndexMigrationTaskState::new),
            new NamedWriteableRegistry.Entry(
                PersistentTaskParams.class,
                SYSTEM_INDEX_UPGRADE_TASK_NAME,
                SystemIndexMigrationTaskParams::new
            )
        );
    }
}
