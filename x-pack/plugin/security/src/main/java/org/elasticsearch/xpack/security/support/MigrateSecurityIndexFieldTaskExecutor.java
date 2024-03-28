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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xpack.core.security.support.MigrateSecurityIndexFieldTaskParams;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Executor;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.PRIMARY_SHARDS;

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

        ActionListener<Void> listener = ActionListener.wrap((res) -> {
            logger.info("Security Index Field Migration complete - written to cluster state");
            task.markAsCompleted();
        }, (exception) -> {
            logger.warn("Security Index Field Migration failed: " + exception);
            task.markAsFailed(exception);
        });

        if (frozenSecurityIndex.indexExists() == false) {
            logger.info("security index does not exist");
            notifyCompleted(listener); // There is nothing to migrate
        } else if (frozenSecurityIndex.isAvailable(PRIMARY_SHARDS) == false) {
            // This could be handled by frozenSecurityIndex.add/remove-StateListener() and just wait for the index to become available
            logger.info("Primary shards not available: " + frozenSecurityIndex.getUnavailableReason(PRIMARY_SHARDS));
        } else {
            UpdateByQueryRequestBuilder updateByQueryRequestBuilder = new UpdateByQueryRequestBuilder(client);
            updateByQueryRequestBuilder.filter(
                new BoolQueryBuilder().should(QueryBuilders.termQuery("type", "user")).should(QueryBuilders.termQuery("type", "role"))
            // A filter on "metadata" exists could be added here too, but since it's not indexed it's not that straight forward, so leaving
            // it out for now
            );
            updateByQueryRequestBuilder.source(securitySystemIndices.getMainIndexManager().aliasName());
            updateByQueryRequestBuilder.script(
                new Script(ScriptType.INLINE, "painless", "ctx._source.metadata_flattened = ctx._source.metadata", Collections.emptyMap())
            );

            securityIndex.checkIndexVersionThenExecute(
                (exception) -> logger.warn("Couldn't query security index " + exception),
                () -> executeAsyncWithOrigin(
                    client,
                    SECURITY_ORIGIN,
                    UpdateByQueryAction.INSTANCE,
                    updateByQueryRequestBuilder.request(),
                    ActionListener.wrap(bulkByScrollResponse -> {
                        logger.info("Migrated [{}] security index fields", bulkByScrollResponse.getUpdated());
                        notifyCompleted(listener);
                    }, (exception) -> logger.info("Updating security index fields failed!" + exception))
                )
            );
        }
    }

    private void notifyCompleted(ActionListener<Void> listener) {
        logger.info("Security Index Field Migration successful, writing result to cluster state");
        securitySystemIndices.getMainIndexManager().writeMetadataMigrated(listener);
    }
}
