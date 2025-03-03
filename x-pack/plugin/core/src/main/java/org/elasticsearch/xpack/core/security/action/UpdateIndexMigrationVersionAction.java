/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;

/**
 * Updates the migration version in the custom metadata for an index in cluster state
 */
public class UpdateIndexMigrationVersionAction extends ActionType<UpdateIndexMigrationVersionResponse> {

    public static final UpdateIndexMigrationVersionAction INSTANCE = new UpdateIndexMigrationVersionAction();
    public static final String NAME = "internal:index/metadata/migration_version/update";
    public static final String MIGRATION_VERSION_CUSTOM_KEY = "migration_version";
    public static final String MIGRATION_VERSION_CUSTOM_DATA_KEY = "version";

    public UpdateIndexMigrationVersionAction() {
        super(NAME);
    }

    public static class Request extends MasterNodeRequest<Request> {
        private final int indexMigrationVersion;
        private final String indexName;

        public Request(TimeValue timeout, int indexMigrationVersion, String indexName) {
            super(timeout);
            this.indexMigrationVersion = indexMigrationVersion;
            this.indexName = indexName;
        }

        protected Request(StreamInput in) throws IOException {
            super(in);
            this.indexMigrationVersion = in.readInt();
            this.indexName = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(indexMigrationVersion);
            out.writeString(indexName);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public int getIndexMigrationVersion() {
            return indexMigrationVersion;
        }

        public String getIndexName() {
            return indexName;
        }
    }

    public static class TransportAction extends TransportMasterNodeAction<Request, UpdateIndexMigrationVersionResponse> {
        private final MasterServiceTaskQueue<UpdateIndexMigrationVersionTask> updateIndexMigrationVersionTaskQueue;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters
        ) {
            super(
                UpdateIndexMigrationVersionAction.NAME,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                Request::new,
                UpdateIndexMigrationVersionResponse::new,
                threadPool.executor(ThreadPool.Names.MANAGEMENT)
            );
            this.updateIndexMigrationVersionTaskQueue = clusterService.createTaskQueue(
                "update-index-migration-version-task-queue",
                Priority.LOW,
                UPDATE_INDEX_MIGRATION_VERSION_TASK_EXECUTOR
            );
        }

        private static final SimpleBatchedExecutor<UpdateIndexMigrationVersionTask, Void> UPDATE_INDEX_MIGRATION_VERSION_TASK_EXECUTOR =
            new SimpleBatchedExecutor<>() {
                @Override
                public Tuple<ClusterState, Void> executeTask(UpdateIndexMigrationVersionTask task, ClusterState clusterState) {
                    return Tuple.tuple(task.execute(clusterState), null);
                }

                @Override
                public void taskSucceeded(UpdateIndexMigrationVersionTask task, Void unused) {
                    task.listener.onResponse(null);
                }
            };

        static class UpdateIndexMigrationVersionTask implements ClusterStateTaskListener {
            private final ActionListener<Void> listener;
            private final int indexMigrationVersion;
            private final String indexName;

            UpdateIndexMigrationVersionTask(ActionListener<Void> listener, int indexMigrationVersion, String indexName) {
                this.listener = listener;
                this.indexMigrationVersion = indexMigrationVersion;
                this.indexName = indexName;
            }

            ClusterState execute(ClusterState currentState) {
                IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(
                    currentState.metadata().getProject().indices().get(indexName)
                );
                indexMetadataBuilder.putCustom(
                    MIGRATION_VERSION_CUSTOM_KEY,
                    Map.of(MIGRATION_VERSION_CUSTOM_DATA_KEY, Integer.toString(indexMigrationVersion))
                );
                indexMetadataBuilder.version(indexMetadataBuilder.version() + 1);

                final ImmutableOpenMap.Builder<String, IndexMetadata> builder = ImmutableOpenMap.builder(
                    currentState.metadata().getProject().indices()
                );
                builder.put(indexName, indexMetadataBuilder.build());

                return ClusterState.builder(currentState)
                    .metadata(Metadata.builder(currentState.metadata()).indices(builder.build()).build())
                    .build();
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        }

        @Override
        protected void masterOperation(
            Task task,
            Request request,
            ClusterState state,
            ActionListener<UpdateIndexMigrationVersionResponse> listener
        ) throws Exception {
            updateIndexMigrationVersionTaskQueue.submitTask(
                "Updating cluster state with a new index migration version",
                new UpdateIndexMigrationVersionTask(
                    ActionListener.wrap(response -> listener.onResponse(new UpdateIndexMigrationVersionResponse()), listener::onFailure),
                    request.getIndexMigrationVersion(),
                    request.getIndexName()
                ),
                null
            );
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, new String[] { request.getIndexName() });
        }
    }
}
