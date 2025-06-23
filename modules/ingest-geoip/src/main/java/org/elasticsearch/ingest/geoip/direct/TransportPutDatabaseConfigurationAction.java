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
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.ingest.geoip.IngestGeoIpMetadata;
import org.elasticsearch.ingest.geoip.direct.PutDatabaseConfigurationAction.Request;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class TransportPutDatabaseConfigurationAction extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPutDatabaseConfigurationAction.class);

    private static final SimpleBatchedExecutor<UpdateDatabaseConfigurationTask, Void> UPDATE_TASK_EXECUTOR = new SimpleBatchedExecutor<>() {
        @Override
        public Tuple<ClusterState, Void> executeTask(UpdateDatabaseConfigurationTask task, ClusterState clusterState) throws Exception {
            return Tuple.tuple(task.execute(clusterState), null);
        }

        @Override
        public void taskSucceeded(UpdateDatabaseConfigurationTask task, Void unused) {
            logger.trace("Updated cluster state for creation-or-update of database configuration [{}]", task.database.id());
            task.listener.onResponse(AcknowledgedResponse.TRUE);
        }
    };

    private final MasterServiceTaskQueue<UpdateDatabaseConfigurationTask> updateDatabaseConfigurationTaskQueue;

    @Inject
    public TransportPutDatabaseConfigurationAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(
            PutDatabaseConfigurationAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            AcknowledgedResponse::readFrom,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.updateDatabaseConfigurationTaskQueue = clusterService.createTaskQueue(
            "update-geoip-database-configuration-state-update",
            Priority.NORMAL,
            UPDATE_TASK_EXECUTOR
        );
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        final String id = request.getDatabase().id();

        updateDatabaseConfigurationTaskQueue.submitTask(
            Strings.format("update-geoip-database-configuration-[%s]", id),
            new UpdateDatabaseConfigurationTask(listener, request.getDatabase()),
            null
        );
    }

    /**
     * Returns 'true' if the database configuration is effectually the same, and thus can be a no-op update.
     */
    static boolean isNoopUpdate(@Nullable DatabaseConfigurationMetadata existingDatabase, DatabaseConfiguration newDatabase) {
        if (existingDatabase == null) {
            return false;
        } else {
            return newDatabase.equals(existingDatabase.database());
        }
    }

    static void validatePrerequisites(DatabaseConfiguration database, ClusterState state) {
        // we need to verify that the database represents a unique file (name) among the various databases for this same provider
        IngestGeoIpMetadata geoIpMeta = state.metadata().getProject().custom(IngestGeoIpMetadata.TYPE, IngestGeoIpMetadata.EMPTY);

        Optional<DatabaseConfiguration> sameName = geoIpMeta.getDatabases()
            .values()
            .stream()
            .map(DatabaseConfigurationMetadata::database)
            // .filter(d -> d.type().equals(database.type())) // of the same type (right now the type is always just 'maxmind')
            .filter(d -> d.id().equals(database.id()) == false) // and a different id
            .filter(d -> d.name().equals(database.name())) // but has the same name!
            .findFirst();

        sameName.ifPresent(d -> {
            throw new IllegalArgumentException(
                Strings.format("database [%s] is already being downloaded via configuration [%s]", database.name(), d.id())
            );
        });
    }

    private record UpdateDatabaseConfigurationTask(ActionListener<AcknowledgedResponse> listener, DatabaseConfiguration database)
        implements
            ClusterStateTaskListener {

        ClusterState execute(ClusterState currentState) throws Exception {
            final var project = currentState.metadata().getProject();
            IngestGeoIpMetadata geoIpMeta = project.custom(IngestGeoIpMetadata.TYPE, IngestGeoIpMetadata.EMPTY);

            String id = database.id();
            final DatabaseConfigurationMetadata existingDatabase = geoIpMeta.getDatabases().get(id);
            // double-check for no-op in the state update task, in case it was changed/reset in the meantime
            if (isNoopUpdate(existingDatabase, database)) {
                return currentState;
            }

            validatePrerequisites(database, currentState);

            Map<String, DatabaseConfigurationMetadata> databases = new HashMap<>(geoIpMeta.getDatabases());
            databases.put(
                id,
                new DatabaseConfigurationMetadata(
                    database,
                    existingDatabase == null ? 1 : existingDatabase.version() + 1,
                    Instant.now().toEpochMilli()
                )
            );
            geoIpMeta = new IngestGeoIpMetadata(databases);

            if (existingDatabase == null) {
                logger.debug("adding new database configuration [{}]", id);
            } else {
                logger.debug("updating existing database configuration [{}]", id);
            }

            return ClusterState.builder(currentState)
                .putProjectMetadata(ProjectMetadata.builder(project).putCustom(IngestGeoIpMetadata.TYPE, geoIpMeta))
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
