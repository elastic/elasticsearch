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
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.util.Arrays;
import java.util.List;
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
        final Map<String, EnrichPolicy> policies = EnrichStore.getPolicies(clusterService.state());
        GetIndexRequest indices = new GetIndexRequest().indices(EnrichPolicy.ENRICH_INDEX_NAME_BASE + "*")
            .indicesOptions(IndicesOptions.lenientExpand());
        // Check that no enrich policies are being executed
        final EnrichPolicyLocks.EnrichPolicyExecutionState executionState = enrichPolicyLocks.captureExecutionState();
        if (executionState.isAnyPolicyInFlight() == false) {
            client.admin().indices().getIndex(indices, new ActionListener<>() {
                @Override
                public void onResponse(GetIndexResponse getIndexResponse) {
                    // Ensure that no enrich policy executions started while we were retrieving the snapshot of index data
                    // If executions were kicked off, we can't be sure that the indices we are about to process are a
                    // stable state of the system (they could be new indices created by a policy that hasn't been published yet).
                    if (enrichPolicyLocks.isSameState(executionState)) {
                        String[] removeIndices = Arrays.stream(getIndexResponse.getIndices())
                            .filter(indexName -> shouldRemoveIndex(getIndexResponse, policies, indexName))
                            .toArray(String[]::new);
                        deleteIndices(removeIndices);
                    } else {
                        logger.debug("Skipping enrich index cleanup since enrich policy was executed while gathering indices");
                        concludeMaintenance();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("Failed to get indices during enrich index maintenance task", e);
                    concludeMaintenance();
                }
            });
        } else {
            concludeMaintenance();
        }
    }

    private boolean shouldRemoveIndex(GetIndexResponse getIndexResponse, Map<String, EnrichPolicy> policies, String indexName) {
        // Find the policy on the index
        logger.debug("Checking if should remove enrich index [{}]", indexName);
        MappingMetadata mappingMetadata = getIndexResponse.getMappings().get(indexName);
        Map<String, Object> mapping = mappingMetadata.getSourceAsMap();
        String policyName = ObjectPath.eval(MAPPING_POLICY_FIELD_PATH, mapping);
        // Check if index has a corresponding policy
        if (policyName == null || policies.containsKey(policyName) == false) {
            // No corresponding policy. Index should be marked for removal.
            logger.debug("Enrich index [{}] does not correspond to any existing policy. Found policy name [{}]", indexName, policyName);
            return true;
        }
        // Check if index is currently linked to an alias
        final String aliasName = EnrichPolicy.getBaseName(policyName);
        List<AliasMetadata> aliasMetadata = getIndexResponse.aliases().get(indexName);
        if (aliasMetadata == null) {
            logger.debug("Enrich index [{}] is not marked as a live index since it has no alias information", indexName);
            return true;
        }
        boolean hasAlias = aliasMetadata.stream().anyMatch((am -> am.getAlias().equals(aliasName)));
        // Index is not currently published to the enrich alias. Should be marked for removal.
        if (hasAlias == false) {
            logger.debug("Enrich index [{}] is not marked as a live index since it lacks the alias [{}]", indexName, aliasName);
            return true;
        }
        logger.debug(
            "Enrich index [{}] was spared since it is associated with the valid policy [{}] and references alias [{}]",
            indexName,
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
