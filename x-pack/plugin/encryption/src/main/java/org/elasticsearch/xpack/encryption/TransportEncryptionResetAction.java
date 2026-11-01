/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedBatchedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.SimpleBatchedAckListenerTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;

import java.util.List;

/**
 * Removes the {@link ProjectEncryptionKeyMetadata} custom and applies each registered {@link EncryptedDataHandler}'s {@code
 * onDestructiveReset} decision to its owned project custom, all in a single atomic cluster-state update. After the new state is
 * published, every entry encrypted under the previous PEK is unrecoverable; features that drive their in-memory state from the
 * cluster-state listener automatically clear their caches.
 *
 */
public class TransportEncryptionResetAction extends TransportMasterNodeAction<EncryptionResetRequest, AcknowledgedResponse> {

    public static final ActionType<AcknowledgedResponse> TYPE = new ActionType<>("cluster:admin/encryption/reset");
    private static final String TASK_QUEUE_NAME = "encryption-reset";
    private static final Logger logger = LogManager.getLogger(TransportEncryptionResetAction.class);

    private final ProjectResolver projectResolver;
    private final EncryptedDataHandlerRegistry handlerRegistry;
    private final MasterServiceTaskQueue<EncryptionResetTask> taskQueue;

    @Inject
    public TransportEncryptionResetAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        EncryptedDataHandlerRegistry handlerRegistry
    ) {
        super(
            TYPE.name(),
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            EncryptionResetRequest::new,
            AcknowledgedResponse::readFrom,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.projectResolver = projectResolver;
        this.handlerRegistry = handlerRegistry;
        this.taskQueue = clusterService.createTaskQueue(TASK_QUEUE_NAME, Priority.URGENT, new EncryptionResetExecutor());
    }

    @Override
    protected void masterOperation(
        Task task,
        EncryptionResetRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        taskQueue.submitTask(
            TASK_QUEUE_NAME,
            new EncryptionResetTask(projectResolver.getProjectId(), handlerRegistry.handlers(), request.ackTimeout(), listener),
            request.masterNodeTimeout()
        );
    }

    @Override
    protected ClusterBlockException checkBlock(EncryptionResetRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_WRITE);
    }

    static ClusterState executeReset(ClusterState currentState, EncryptionResetTask task) {
        ProjectMetadata project = currentState.metadata().getProject(task.projectId());
        return currentState.copyAndUpdateProject(task.projectId(), b -> {
            if (project.custom(ProjectEncryptionKeyMetadata.TYPE) != null) {
                b.removeCustom(ProjectEncryptionKeyMetadata.TYPE);
            }
            for (EncryptedDataHandler<?> handler : task.handlers()) {
                applyDestructiveReset(b, project, handler);
            }
        });
    }

    static <T extends Metadata.ProjectCustom> void applyDestructiveReset(
        ProjectMetadata.Builder builder,
        ProjectMetadata project,
        EncryptedDataHandler<T> handler
    ) {
        T current = project.custom(handler.customName());
        T next = handler.onDestructiveReset(current);
        if (next == current) {
            return;
        }
        if (next == null) {
            builder.removeCustom(handler.customName());
            return;
        }
        if (handler.customName().equals(next.getWriteableName()) == false) {
            throw new IllegalStateException(
                "handler ["
                    + handler.getClass().getSimpleName()
                    + "] customName="
                    + handler.customName()
                    + " does not match returned custom getWriteableName="
                    + next.getWriteableName()
            );
        }
        builder.putCustom(handler.customName(), next);
    }

    static final class EncryptionResetTask extends AckedBatchedClusterStateUpdateTask {

        private final ProjectId projectId;
        private final List<EncryptedDataHandler<?>> handlers;

        EncryptionResetTask(
            ProjectId projectId,
            List<EncryptedDataHandler<?>> handlers,
            TimeValue ackTimeout,
            ActionListener<AcknowledgedResponse> listener
        ) {
            super(ackTimeout, listener);
            this.projectId = projectId;
            this.handlers = handlers;
        }

        ProjectId projectId() {
            return projectId;
        }

        List<EncryptedDataHandler<?>> handlers() {
            return handlers;
        }
    }

    private static final class EncryptionResetExecutor extends SimpleBatchedAckListenerTaskExecutor<EncryptionResetTask> {

        @Override
        public Tuple<ClusterState, ClusterStateAckListener> executeTask(EncryptionResetTask task, ClusterState state) {
            boolean pekPresent = state.metadata().getProject(task.projectId()).custom(ProjectEncryptionKeyMetadata.TYPE) != null;
            ClusterState updated = executeReset(state, task);
            if (pekPresent) {
                logger.info("reset project encryption key (project={}, registered_handlers={})", task.projectId(), task.handlers().size());
            } else {
                logger.debug(
                    "reset called but no project encryption key was present (project={}, registered_handlers={})",
                    task.projectId(),
                    task.handlers().size()
                );
            }
            return new Tuple<>(updated, task);
        }
    }
}
