/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
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
import java.util.Set;
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

    private final SetOnce<LifecycleListener> beforeStopListener = new SetOnce<>();

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
            // Only set the lifecycle listener if it hasn't already been set.
            if (beforeStopListener.trySet(new StopMaintenanceOnLifecycleStop())) {
                clusterService.addLifecycleListener(beforeStopListener.get());
            }
        }
    }

    @Override
    public void offMaster() {
        if (cancellable != null && cancellable.isCancelled() == false) {
            isMaster = false;
            cancellable.cancel();
        }
    }

    /**
     * Lifecycle listener that halts the maintenance service when node is shutting down.
     */
    private class StopMaintenanceOnLifecycleStop extends LifecycleListener {
        @Override
        public void beforeStop() {
            offMaster();
        }
    }

    // Visible for testing
    protected void scheduleNext() {
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
        // Get cluster state first because for a new index to exist there then it has to have already been added to the inflight list
        // If the new index is in the cluster state but not in the inflight list, then that means that the index name was removed from the
        // list inflight list because it has already been promoted or because the policy execution failed.
        ClusterState clusterState = clusterService.state();
        Set<String> inflightPolicyExecutionIndices = enrichPolicyLocks.inflightPolicyIndices();
        final Map<String, EnrichPolicy> policies = EnrichStore.getPolicies(clusterState);
        logger.debug(() -> "Working enrich indices excluded from maintenance [" + String.join(", ", inflightPolicyExecutionIndices) + "]");
        String[] removeIndices = clusterState.metadata()
            .indices()
            .values()
            .stream()
            .filter(indexMetadata -> indexMetadata.getIndex().getName().startsWith(EnrichPolicy.ENRICH_INDEX_NAME_BASE))
            .filter(indexMetadata -> indexUsedByPolicy(indexMetadata, policies, inflightPolicyExecutionIndices) == false)
            .map(IndexMetadata::getIndex)
            .map(Index::getName)
            .toArray(String[]::new);
        deleteIndices(removeIndices);
    }

    private boolean indexUsedByPolicy(IndexMetadata indexMetadata, Map<String, EnrichPolicy> policies, Set<String> inflightPolicyIndices) {
        String indexName = indexMetadata.getIndex().getName();
        logger.debug("Checking if should remove enrich index [{}]", indexName);
        // First ignore the index entirely if it is in the inflightPolicyIndices set as it is actively being worked on
        if (inflightPolicyIndices.contains(indexName)) {
            logger.debug("Enrich index [{}] was spared since it is reserved for an active policy execution.", indexName);
            return true;
        }
        // Find the policy on the index
        MappingMetadata mappingMetadata = indexMetadata.mapping();
        Map<String, Object> mapping = mappingMetadata.getSourceAsMap();
        String policyName = ObjectPath.eval(MAPPING_POLICY_FIELD_PATH, mapping);
        // Check if index has a corresponding policy
        if (policyName == null || policies.containsKey(policyName) == false) {
            // No corresponding policy. Index should be marked for removal.
            logger.debug("Enrich index [{}] does not correspond to any existing policy. Found policy name [{}]", indexName, policyName);
            return false;
        }
        // Check if index is currently linked to an alias
        final String aliasName = EnrichPolicy.getBaseName(policyName);
        ImmutableOpenMap<String, AliasMetadata> aliasMetadata = indexMetadata.getAliases();
        if (aliasMetadata == null) {
            logger.debug("Enrich index [{}] is not marked as a live index since it has no alias information", indexName);
            return false;
        }
        boolean hasAlias = aliasMetadata.containsKey(aliasName);
        // Index is not currently published to the enrich alias. Should be marked for removal.
        if (hasAlias == false) {
            logger.debug("Enrich index [{}] is not marked as a live index since it lacks the alias [{}]", indexName, aliasName);
            return false;
        }
        logger.debug(
            "Enrich index [{}] was spared since it is associated with the valid policy [{}] and references alias [{}]",
            indexName,
            policyName,
            aliasName
        );
        return true;
    }

    private void deleteIndices(String[] removeIndices) {
        if (removeIndices.length != 0) {
            DeleteIndexRequest deleteIndices = new DeleteIndexRequest().indices(removeIndices).indicesOptions(IGNORE_UNAVAILABLE);
            client.admin().indices().delete(deleteIndices, new ActionListener<AcknowledgedResponse>() {
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
