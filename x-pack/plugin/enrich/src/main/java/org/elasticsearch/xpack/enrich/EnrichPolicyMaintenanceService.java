/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Semaphore;

import static org.elasticsearch.xpack.core.ClientHelper.ENRICH_ORIGIN;

public class EnrichPolicyMaintenanceService implements LocalNodeMasterListener {

    private static final Logger logger = LogManager.getLogger(EnrichPolicyMaintenanceService.class);

    private static final String MAPPING_POLICY_FIELD_PATH = "_meta." + EnrichPolicyRunner.ENRICH_POLICY_NAME_FIELD_NAME;
    private static final IndicesOptions IGNORE_UNAVAILABLE = IndicesOptions.fromOptions(true, false, false, false);

    private final Settings settings;
    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final EnrichPolicyLocks enrichPolicyLocks;

    private volatile boolean isMaster = false;
    private volatile Scheduler.Cancellable cancellable;
    private final Semaphore maintenanceLock = new Semaphore(1);

    EnrichPolicyMaintenanceService(
        Settings settings,
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        EnrichPolicyLocks enrichPolicyLocks
    ) {
        this.settings = settings;
        this.client = new OriginSettingClient(client, ENRICH_ORIGIN);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.enrichPolicyLocks = enrichPolicyLocks;
    }

    void initialize() {
        clusterService.addLocalNodeMasterListener(this);
    }

    @Override
    public void onMaster() {
        if (cancellable == null || cancellable.isCancelled()) {
            isMaster = true;
            scheduleNext();
            clusterService.addLifecycleListener(new LifecycleListener() {
                @Override
                public void beforeStop() {
                    offMaster();
                }
            });
        }
    }

    @Override
    public void offMaster() {
        if (cancellable != null && cancellable.isCancelled() == false) {
            isMaster = false;
            cancellable.cancel();
        }
    }

    private void scheduleNext() {
        if (isMaster) {
            try {
                TimeValue waitTime = EnrichPlugin.ENRICH_CLEANUP_PERIOD.get(settings);
                cancellable = threadPool.schedule(this::execute, waitTime, ThreadPool.Names.GENERIC);
            } catch (EsRejectedExecutionException e) {
                if (e.isExecutorShutdown()) {
                    logger.debug("Failed to schedule next [enrich] maintenance task; Shutting down", e);
                } else {
                    throw e;
                }
            }
        } else {
            logger.debug("No longer master; Skipping next scheduled [enrich] maintenance task");
        }
    }

    private void execute() {
        logger.debug("Triggering scheduled [enrich] maintenance task");
        if (isMaster) {
            maybeCleanUpEnrichIndices();
            scheduleNext();
        } else {
            logger.debug("No longer master; Skipping next scheduled [enrich] maintenance task");
        }
    }

    private void maybeCleanUpEnrichIndices() {
        if (maintenanceLock.tryAcquire()) {
            cleanUpEnrichIndices();
        } else {
            logger.debug("Previous [enrich] maintenance task still in progress; Skipping this execution");
        }
    }

    void concludeMaintenance() {
        maintenanceLock.release();
    }

    void cleanUpEnrichIndices() {
        // Determine the indices to remove ONLY while there are no policies in flight and no new policy executions can be kicked off.
        String[] removeIndices = enrichPolicyLocks.attemptMaintenance(() -> {
            ClusterState clusterState = clusterService.state();
            final Map<String, EnrichPolicy> policies = EnrichStore.getPolicies(clusterState);
            return clusterState.metadata()
                .indices()
                .values()
                .stream()
                .filter(metadata -> metadata.getIndex().getName().startsWith(EnrichPolicy.ENRICH_INDEX_NAME_BASE))
                .filter(metadata -> shouldRemoveIndex(metadata, policies))
                .map(IndexMetadata::getIndex)
                .map(Index::getName)
                .toArray(String[]::new);
        });
        if (removeIndices != null) {
            deleteIndices(removeIndices);
        } else {
            logger.debug("Skipping enrich index cleanup since enrich policy was executed while gathering indices");
            concludeMaintenance();
        }
    }

    private boolean shouldRemoveIndex(IndexMetadata indexMetadata, Map<String, EnrichPolicy> policies) {
        // Find the policy on the index
        logger.debug("Checking if should remove enrich index [{}]", indexMetadata.getIndex().getName());
        MappingMetadata mappingMetadata = indexMetadata.mapping();
        Map<String, Object> mapping = mappingMetadata.getSourceAsMap();
        String policyName = ObjectPath.eval(MAPPING_POLICY_FIELD_PATH, mapping);
        // Check if index has a corresponding policy
        if (policyName == null || policies.containsKey(policyName) == false) {
            // No corresponding policy. Index should be marked for removal.
            logger.debug(
                "Enrich index [{}] does not correspond to any existing policy. Found policy name [{}]",
                indexMetadata.getIndex().getName(),
                policyName
            );
            return true;
        }
        // Check if index is currently linked to an alias
        final String aliasName = EnrichPolicy.getBaseName(policyName);
        Map<String, AliasMetadata> aliasMetadata = indexMetadata.getAliases();
        if (aliasMetadata == null || aliasMetadata.isEmpty()) {
            logger.debug(
                "Enrich index [{}] is not marked as a live index since it has no alias information",
                indexMetadata.getIndex().getName()
            );
            return true;
        }
        boolean hasAlias = aliasMetadata.containsKey(aliasName);
        // Index is not currently published to the enrich alias. Should be marked for removal.
        if (hasAlias == false) {
            logger.debug(
                "Enrich index [{}] is not marked as a live index since it lacks the alias [{}]",
                indexMetadata.getIndex().getName(),
                aliasName
            );
            return true;
        }
        logger.debug(
            "Enrich index [{}] was spared since it is associated with the valid policy [{}] and references alias [{}]",
            indexMetadata.getIndex().getName(),
            policyName,
            aliasName
        );
        return false;
    }

    private void deleteIndices(String[] removeIndices) {
        if (removeIndices.length != 0) {
            DeleteIndexRequest deleteIndices = new DeleteIndexRequest().indices(removeIndices).indicesOptions(IGNORE_UNAVAILABLE);
            client.admin().indices().delete(deleteIndices, new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    logger.debug("Completed deletion of stale enrich indices [{}]", () -> Arrays.toString(removeIndices));
                    concludeMaintenance();
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error(
                        () -> "Enrich maintenance task could not delete abandoned enrich indices [" + Arrays.toString(removeIndices) + "]",
                        e
                    );
                    concludeMaintenance();
                }
            });
        } else {
            concludeMaintenance();
        }
    }
}
