/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.Index;
import org.elasticsearch.persistent.PersistentTasksService;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static org.elasticsearch.indices.SystemIndexDescriptor.VERSION_META_KEY;
import static org.elasticsearch.indices.UpdateIndexMigrationVersionAction.MIGRATION_VERSION_CUSTOM_DATA_KEY;
import static org.elasticsearch.indices.UpdateIndexMigrationVersionAction.MIGRATION_VERSION_CUSTOM_KEY;

/**
 * {@link ClusterStateListener} responsible for submitting pending {@link SystemIndexMigrationTask} implementations using the
 * {@link PersistentTasksService} to be executed by the {@link SystemIndexMigrationTaskExecutor}.
 */
public class SystemIndexMigrationService implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(SystemIndexMigrationService.class);

    private final SystemIndices systemIndices;
    private final FeatureService featureService;

    private final PersistentTasksService persistentTasksService;

    private static final int MAX_SYSTEM_INDEX_MIGRATION_RETRY_COUNT = 10;

    // Node local retry count for migration jobs that's checked only on the master node to make sure
    // submit migration jobs doesn't get out of hand and retries forever if they fail. Reset by a
    // restart or master node change.
    private final Map<String, Integer> nodeLocalMigrationRetryCountByIndex = new ConcurrentHashMap<>();

    public SystemIndexMigrationService(
        SystemIndices systemIndices,
        FeatureService featureService,
        PersistentTasksService persistentTasksService
    ) {
        this.systemIndices = systemIndices;
        this.featureService = featureService;
        this.persistentTasksService = persistentTasksService;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() == false) {
            return;
        }

        getEligibleDescriptors(event.state().metadata()).forEach(systemIndexDescriptor -> {
            IndexMetadata indexMetadata = resolveConcreteIndex(systemIndexDescriptor.getAliasName(), event.state().metadata());
            String indexName = indexMetadata.getIndex().getName();
            NavigableMap<Integer, SystemIndexMigrationTask> pendingVersions = systemIndexDescriptor.getMigrationsAfter(
                versionFromIndexMetadata(indexMetadata)
            );

            List<Map.Entry<Integer, SystemIndexMigrationTask>> migrationsToApply = pendingVersions.entrySet()
                .stream()
                .takeWhile((entry) -> isMigrationEligible(event.state(), indexMetadata, entry.getValue()))
                .toList();

            if (migrationsToApply.isEmpty()) {
                nodeLocalMigrationRetryCountByIndex.put(indexName, 0);
            } else if (nodeLocalMigrationRetryCountByIndex.getOrDefault(indexName, 0) > MAX_SYSTEM_INDEX_MIGRATION_RETRY_COUNT) {
                logger.warn(
                    String.format(
                        Locale.ROOT,
                        "System index migration failed for index [%s] [%d] times and will not be retried until after node restart",
                        indexName,
                        nodeLocalMigrationRetryCountByIndex.get(indexName)
                    )
                );
            } else {
                submitMigrationTask(
                    indexName,
                    migrationsToApply.stream()
                        .takeWhile((entry) -> entry.getValue().checkPreConditions())
                        .mapToInt(Map.Entry::getKey)
                        .toArray()
                );
            }
        });
    }

    private void submitMigrationTask(String indexName, int[] migrationVersions) {
        nodeLocalMigrationRetryCountByIndex.merge(indexName, 1, Integer::sum);
        persistentTasksService.sendStartRequest(
            SystemIndexMigrationTaskParams.TASK_NAME,
            SystemIndexMigrationTaskParams.TASK_NAME,
            new SystemIndexMigrationTaskParams(indexName, migrationVersions),
            null,
            ActionListener.wrap((response) -> {
                logger.trace("System index migration task submitted");
            }, (exception) -> {
                if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                    // Do not count ResourceAlreadyExistsException as failure (task is already in progress)
                    nodeLocalMigrationRetryCountByIndex.merge(indexName, -1, Integer::sum);
                } else {
                    logger.warn("Submit system migration task failed for [" + indexName + "]: " + ExceptionsHelper.unwrapCause(exception));
                }
            })
        );
    }

    Stream<SystemIndexDescriptor> getEligibleDescriptors(Metadata metadata) {
        return this.systemIndices.getSystemIndexDescriptors()
            .stream()
            .filter(SystemIndexDescriptor::isAutomaticallyManaged)
            .filter(d -> metadata.hasIndexAbstraction(d.getPrimaryIndex()));
    }

    private int versionFromIndexMetadata(IndexMetadata indexMetadata) {
        Map<String, String> customMetadata = indexMetadata == null ? null : indexMetadata.getCustomData(MIGRATION_VERSION_CUSTOM_KEY);
        return customMetadata == null ? 0 : Integer.parseInt(customMetadata.get(MIGRATION_VERSION_CUSTOM_DATA_KEY));
    }

    protected boolean isMigrationEligible(ClusterState state, IndexMetadata indexMetadata, SystemIndexMigrationTask migrationTask) {
        boolean clusterHasAllFeatures = migrationTask.nodeFeaturesRequired()
            .stream()
            .allMatch(feature -> featureService.clusterHasFeature(state, feature));

        Integer currentIndexMappingVersion = 0;
        if (indexMetadata != null && indexMetadata.mapping() != null) {
            @SuppressWarnings("unchecked")
            Map<String, Object> meta = (Map<String, Object>) indexMetadata.mapping().sourceAsMap().get("_meta");

            if (meta != null && meta.get(VERSION_META_KEY) != null) {
                currentIndexMappingVersion = (Integer) meta.get(VERSION_META_KEY);
            }
        }
        boolean clusterHasMinimumMapping = migrationTask.minMappingVersion() <= currentIndexMappingVersion;
        return clusterHasMinimumMapping && clusterHasAllFeatures;
    }

    private static IndexMetadata resolveConcreteIndex(final String indexOrAliasName, final Metadata metadata) {
        // TODO this requires every system index alias to map to exactly one concrete index. Is there a better approach?
        final IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(indexOrAliasName);
        if (indexAbstraction != null) {
            final List<Index> indices = indexAbstraction.getIndices();
            if (indexAbstraction.getType() != IndexAbstraction.Type.CONCRETE_INDEX && indices.size() > 1) {
                throw new IllegalStateException("Alias [" + indexOrAliasName + "] points to more than one index: " + indices);
            }
            return metadata.index(indices.get(0));
        }
        // TODO is it possible that the concrete system index doesn't exist?
        throw new IllegalStateException("Concrete index not found for [" + indexOrAliasName + "]");
    }

}
