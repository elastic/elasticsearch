/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class SetIndexMetadataPropertyAction extends ActionType<SetIndexMetadataPropertyResponse> {

    public static final String NAME = "indices:internal/admin/metadata/custom/set";

    private SetIndexMetadataPropertyAction() {
        super(NAME);
    }

    public static final SetIndexMetadataPropertyAction INSTANCE = new SetIndexMetadataPropertyAction();

    public static class TransportAction extends TransportMasterNodeAction<
        SetIndexMetadataPropertyRequest,
        SetIndexMetadataPropertyResponse> {
        private final MasterServiceTaskQueue<
            SetIndexMetadataPropertyAction.TransportAction.SetIndexMetadataPropertyTask> setIndexMetadataPropertyTaskMasterServiceTaskQueue;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver
        ) {
            super(
                SetIndexMetadataPropertyAction.NAME,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                SetIndexMetadataPropertyRequest::new,
                indexNameExpressionResolver,
                SetIndexMetadataPropertyResponse::new,
                threadPool.executor(ThreadPool.Names.MANAGEMENT)
            );
            this.setIndexMetadataPropertyTaskMasterServiceTaskQueue = clusterService.createTaskQueue(
                "set-index-metadata-property-task-queue",
                Priority.LOW,
                SET_INDEX_METADATA_PROPERTY_TASK_VOID_SIMPLE_BATCHED_EXECUTOR
            );
        }

        private static final SimpleBatchedExecutor<
            SetIndexMetadataPropertyAction.TransportAction.SetIndexMetadataPropertyTask,
            Map<String, String>> SET_INDEX_METADATA_PROPERTY_TASK_VOID_SIMPLE_BATCHED_EXECUTOR = new SimpleBatchedExecutor<>() {
                @Override
                public Tuple<ClusterState, Map<String, String>> executeTask(
                    SetIndexMetadataPropertyAction.TransportAction.SetIndexMetadataPropertyTask task,
                    ClusterState clusterState
                ) {
                    return task.execute(clusterState);
                }

                @Override
                public void taskSucceeded(
                    SetIndexMetadataPropertyAction.TransportAction.SetIndexMetadataPropertyTask task,
                    Map<String, String> value
                ) {
                    task.success(value);
                }
            };

        static class SetIndexMetadataPropertyTask implements ClusterStateTaskListener {
            private final ActionListener<SetIndexMetadataPropertyResponse> listener;
            private final Index index;
            private final String key;
            @Nullable
            private final Map<String, String> expected;
            @Nullable
            private final Map<String, String> value;

            SetIndexMetadataPropertyTask(
                ActionListener<SetIndexMetadataPropertyResponse> listener,
                Index index,
                String key,
                @Nullable Map<String, String> expected,
                @Nullable Map<String, String> value
            ) {
                this.listener = listener;
                this.index = index;
                this.key = key;
                this.expected = expected;
                this.value = value;
            }

            Tuple<ClusterState, Map<String, String>> execute(ClusterState state) {
                IndexMetadata indexMetadata = state.metadata().getIndexSafe(index);
                Map<String, String> existingValue = indexMetadata.getCustomData(key);
                if (Objects.equals(expected, existingValue)) {
                    IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
                    if (value != null) {
                        indexMetadataBuilder.putCustom(key, value);
                    } else {
                        indexMetadataBuilder.removeCustom(key);
                    }
                    indexMetadataBuilder.version(indexMetadataBuilder.version() + 1);
                    ImmutableOpenMap.Builder<String, IndexMetadata> builder = ImmutableOpenMap.builder(state.metadata().indices());
                    builder.put(index.getName(), indexMetadataBuilder.build());
                    return new Tuple<>(
                        ClusterState.builder(state).metadata(Metadata.builder(state.metadata()).indices(builder.build()).build()).build(),
                        value
                    );
                } else {
                    // returns existing value when expectation is not met
                    return new Tuple<>(state, existingValue);
                }
            }

            void success(Map<String, String> value) {
                listener.onResponse(new SetIndexMetadataPropertyResponse(value));
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        }

        @Override
        protected void masterOperation(
            Task task,
            SetIndexMetadataPropertyRequest request,
            ClusterState state,
            ActionListener<SetIndexMetadataPropertyResponse> listener
        ) throws Exception {
            Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
            if (concreteIndices.length != 1) {
                listener.onFailure(
                    new ElasticsearchException("Exactly one concrete index expected, but resolved " + Arrays.toString(concreteIndices))
                );
                return;
            }
            IndexMetadata indexMetadata = state.metadata().getIndexSafe(concreteIndices[0]);
            Map<String, String> existingValue = indexMetadata.getCustomData(request.key());
            if (Objects.equals(request.expected(), existingValue)) {
                setIndexMetadataPropertyTaskMasterServiceTaskQueue.submitTask(
                    "Set index metadata custom value",
                    new SetIndexMetadataPropertyAction.TransportAction.SetIndexMetadataPropertyTask(
                        listener,
                        concreteIndices[0],
                        request.key(),
                        request.expected(),
                        request.value()
                    ),
                    null
                );
            } else {
                // returns existing value when expectation is not met
                listener.onResponse(new SetIndexMetadataPropertyResponse(existingValue));
            }
        }

        @Override
        protected ClusterBlockException checkBlock(SetIndexMetadataPropertyRequest request, ClusterState state) {
            return state.blocks()
                .indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indexNameExpressionResolver.concreteIndexNames(state, request));
        }
    }
}
