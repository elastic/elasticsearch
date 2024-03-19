/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.security.support.MigrateSecurityIndexFieldTaskParams;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.SEARCH_SHARDS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;

public class MigrateSecurityIndexFieldTaskExecutor extends PersistentTasksExecutor<MigrateSecurityIndexFieldTaskParams> {
    private static final Logger logger = LogManager.getLogger(MigrateSecurityIndexFieldTaskExecutor.class);
    private final SecuritySystemIndices securitySystemIndices;
    private final Client client;

    public MigrateSecurityIndexFieldTaskExecutor(
        String taskName,
        Executor executor,
        SecuritySystemIndices securitySystemIndices,
        Client client
    ) {
        super(taskName, executor);
        this.securitySystemIndices = securitySystemIndices;
        this.client = client;
    }

    @Override
    public PersistentTasksCustomMetadata.Assignment getAssignment(
        MigrateSecurityIndexFieldTaskParams params,
        Collection<DiscoveryNode> candidateNodes,
        ClusterState clusterState
    ) {
        // TODO Make sure task runs on a node with the security index
        return super.getAssignment(params, candidateNodes, clusterState);
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, MigrateSecurityIndexFieldTaskParams params, PersistentTaskState state) {
        logger.info("Running migrate security index field from: " + params.getSourceField() + ", to: " + params.getTargetField());
        SecurityIndexManager securityIndex = securitySystemIndices.getMainIndexManager();
        final SecurityIndexManager frozenSecurityIndex = securityIndex.defensiveCopy();
        if (frozenSecurityIndex.indexExists() == false) {
            logger.info("security index does not exist");
            notifyCompleted(); // There is nothing to migrate
        } else if (frozenSecurityIndex.isAvailable(SEARCH_SHARDS) == false) {
            // This could be handled by frozenSecurityIndex.add/remove-StateListener() and just wait for the index to become available
            logger.info("Search shards not available: " + frozenSecurityIndex.getUnavailableReason(SEARCH_SHARDS));
        } else {
            // This should be improved to:
            // - Query and update in batches
            // - Use threadpool that can afford being occupied for a while to finish the migration
            // - Only iterate users and roles
            SearchRequest searchRequest = new SearchRequest(SECURITY_MAIN_ALIAS);
            securityIndex.checkIndexVersionThenExecute(
                (exception) -> logger.warn("Couldn't query security index " + exception),
                () -> executeAsyncWithOrigin(
                    client,
                    SECURITY_ORIGIN,
                    TransportSearchAction.TYPE,
                    searchRequest,
                    ActionListener.wrap(searchResponse -> {
                        final long total = searchResponse.getHits().getTotalHits().value;
                        if (total == 0) {
                            logger.info("No data found for query [{}]", searchRequest.source().query());
                            notifyCompleted(); // There is nothing to migrate
                            return;
                        }

                        bulkUpdate(
                            searchResponse.getHits().getHits(),
                            params,
                            ActionListener.wrap((response) -> notifyCompleted(), (exception) -> {
                                logger.warn("Bulk Update failed for security index field migration " + exception);
                            })
                        );
                    }, (exception) -> logger.info("Getting models failed!" + exception))
                )
            );
        }
    }

    private void notifyCompleted() {
        logger.info("Security Index Field Migration successful, writing result to cluster state");
        securitySystemIndices.getMainIndexManager()
            .writeMetadataMigrated(
                ActionListener.wrap(
                    (res) -> logger.info("Security Index Field Migration complete written to cluster state"),
                    (exception) -> logger.warn(
                        "Security Index Field Migration completed but result couldn't be written to cluster state: " + exception
                    )
                )
            );
    }

    private void bulkUpdate(SearchHit[] hits, MigrateSecurityIndexFieldTaskParams params, ActionListener<Void> listener) {
        logger.info("Migrating [" + hits.length + "] documents");
        // This updates while iterating the index, I think that's fine since any updates happening in parallel
        // will write to both the source and the target field
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        for (SearchHit hit : hits) {
            Map<String, Object> sourceMap = hit.getSourceAsMap();
            hit.getSourceAsMap();

            if (sourceMap != null) {
                Object sourceData = sourceMap.get(params.getSourceField());
                Object targetData = sourceMap.get(params.getTargetField());
                if (sourceData != null && sourceData != targetData) {
                    UpdateRequestBuilder updateRequestBuilder = new UpdateRequestBuilder(client, SECURITY_MAIN_ALIAS, hit.getId());
                    Map<String, Object> updatedSource = new HashMap<>(sourceMap);
                    updatedSource.put(params.getTargetField(), sourceData);
                    updateRequestBuilder.setDoc(updatedSource);
                    bulkRequestBuilder.add(updateRequestBuilder);
                }
            }
        }

        BulkRequest request = bulkRequestBuilder.request();

        securitySystemIndices.getMainIndexManager()
            .checkIndexVersionThenExecute(
                (exception) -> logger.warn("Bulk Update failed for security index field migration " + exception),
                () -> executeAsyncWithOrigin(client, SECURITY_ORIGIN, BulkAction.INSTANCE, request, ActionListener.wrap(updateResponse -> {
                    logger.info("Bulk Update successful for security index field migration");
                    listener.onResponse(null);
                }, listener::onFailure))
            );
    }
}
