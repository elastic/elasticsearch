/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip.direct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.ingest.geoip.IngestGeoIpMetadata;
import org.elasticsearch.ingest.geoip.direct.DeleteDatabaseConfigurationAction.Request;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;

public class TransportDeleteDatabaseConfigurationAction extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDeleteDatabaseConfigurationAction.class);

    private static final SimpleBatchedExecutor<DeleteDatabaseConfigurationTask, Void> DELETE_TASK_EXECUTOR = new SimpleBatchedExecutor<>() {
        @Override
        public Tuple<ClusterState, Void> executeTask(DeleteDatabaseConfigurationTask task, ClusterState clusterState) throws Exception {
            return Tuple.tuple(task.execute(clusterState), null);
        }

        @Override
        public void taskSucceeded(DeleteDatabaseConfigurationTask task, Void unused) {
            logger.trace("Updated cluster state for deletion of database configuration [{}]", task.databaseId);
            task.listener.onResponse(AcknowledgedResponse.TRUE);
        }
    };

    private final MasterServiceTaskQueue<DeleteDatabaseConfigurationTask> deleteDatabaseConfigurationTaskQueue;

    @Inject
    public TransportDeleteDatabaseConfigurationAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(
            DeleteDatabaseConfigurationAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            AcknowledgedResponse::readFrom,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.deleteDatabaseConfigurationTaskQueue = clusterService.createTaskQueue(
            "delete-geoip-database-configuration-state-update",
            Priority.NORMAL,
            DELETE_TASK_EXECUTOR
        );
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener)
        throws Exception {
        final String id = request.getDatabaseId();
        final IngestGeoIpMetadata geoIpMeta = state.metadata().getProject().custom(IngestGeoIpMetadata.TYPE, IngestGeoIpMetadata.EMPTY);
        if (geoIpMeta.getDatabases().containsKey(id) == false) {
            throw new ResourceNotFoundException("Database configuration not found: {}", id);
        } else if (geoIpMeta.getDatabases().get(id).database().isReadOnly()) {
            throw new IllegalArgumentException("Database " + id + " is read only");
        }
        deleteDatabaseConfigurationTaskQueue.submitTask(
            Strings.format("delete-geoip-database-configuration-[%s]", id),
            new DeleteDatabaseConfigurationTask(listener, id),
            null
        );
    }

    private record DeleteDatabaseConfigurationTask(ActionListener<AcknowledgedResponse> listener, String databaseId)
        implements
            ClusterStateTaskListener {

        ClusterState execute(ClusterState currentState) throws Exception {
            final IngestGeoIpMetadata geoIpMeta = currentState.metadata()
                .getProject()
                .custom(IngestGeoIpMetadata.TYPE, IngestGeoIpMetadata.EMPTY);

            logger.debug("deleting database configuration [{}]", databaseId);
            Map<String, DatabaseConfigurationMetadata> databases = new HashMap<>(geoIpMeta.getDatabases());
            databases.remove(databaseId);

            Metadata currentMeta = currentState.metadata();
            return ClusterState.builder(currentState)
                .metadata(Metadata.builder(currentMeta).putCustom(IngestGeoIpMetadata.TYPE, new IngestGeoIpMetadata(databases)))
                .build();
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
