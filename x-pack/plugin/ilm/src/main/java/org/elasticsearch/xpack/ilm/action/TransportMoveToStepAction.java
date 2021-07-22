/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.action.MoveToStepAction;
import org.elasticsearch.xpack.core.ilm.action.MoveToStepAction.Request;
import org.elasticsearch.xpack.ilm.IndexLifecycleService;

public class TransportMoveToStepAction extends TransportMasterNodeAction<Request, AcknowledgedResponse> {
    private static final Logger logger = LogManager.getLogger(TransportMoveToStepAction.class);

    IndexLifecycleService indexLifecycleService;
    @Inject
    public TransportMoveToStepAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                     ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                     IndexLifecycleService indexLifecycleService) {
        super(MoveToStepAction.NAME, transportService, clusterService, threadPool, actionFilters, Request::new,
            indexNameExpressionResolver, AcknowledgedResponse::readFrom, ThreadPool.Names.SAME);
        this.indexLifecycleService = indexLifecycleService;
    }

    @Override
    protected void masterOperation(Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        IndexMetadata indexMetadata = state.metadata().index(request.getIndex());
        if (indexMetadata == null) {
            listener.onFailure(new IllegalArgumentException("index [" + request.getIndex() + "] does not exist"));
            return;
        }

        final String policyName = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexMetadata.getSettings());
        if (policyName == null) {
            listener.onFailure(new IllegalArgumentException("index [" + request.getIndex() + "] is not managed by ILM"));
            return;
        }

        final Request.PartialStepKey abstractTargetKey = request.getNextStepKey();
        final String targetStr = abstractTargetKey.getPhase() + "/" + abstractTargetKey.getAction() + "/" + abstractTargetKey.getName();

        // Resolve the key that could have optional parts into one
        // that is totally concrete given the existing policy and index
        Step.StepKey concreteTargetStepKey = indexLifecycleService.resolveStepKey(state, indexMetadata.getIndex(),
            abstractTargetKey.getPhase(), abstractTargetKey.getAction(), abstractTargetKey.getName());

        // We do a pre-check here before invoking the cluster state update just so we can skip the submission if the request is bad.
        if (concreteTargetStepKey == null) {
            // This means we weren't able to find the key they specified
            String message = "cannot move index [" + indexMetadata.getIndex().getName() + "] with policy [" +
                policyName + "]: unable to determine concrete step key from target next step key: " + abstractTargetKey;
            logger.warn(message);
            listener.onFailure(new IllegalArgumentException(message));
            return;
        }

        clusterService.submitStateUpdateTask("index[" + request.getIndex() + "]-move-to-step-" + targetStr,
            new AckedClusterStateUpdateTask(request, listener) {
                final SetOnce<Step.StepKey> concreteTargetKey = new SetOnce<>();

                @Override
                public ClusterState execute(ClusterState currentState) {
                    // Resolve the key that could have optional parts into one
                    // that is totally concrete given the existing policy and index
                    Step.StepKey concreteTargetStepKey = indexLifecycleService.resolveStepKey(state, indexMetadata.getIndex(),
                        abstractTargetKey.getPhase(), abstractTargetKey.getAction(), abstractTargetKey.getName());

                    // Make one more check, because it could have changed in the meantime. If that is the case, the request is ignored.
                    if (concreteTargetStepKey == null) {
                        // This means we weren't able to find the key they specified
                        logger.error("unable to move index " + indexMetadata.getIndex() + " as we are unable to resolve a concrete " +
                            "step key from target next step key: " + abstractTargetKey);
                        return currentState;
                    }

                    concreteTargetKey.set(concreteTargetStepKey);
                    return indexLifecycleService.moveClusterStateToStep(currentState, indexMetadata.getIndex(), request.getCurrentStepKey(),
                        concreteTargetKey.get());
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    IndexMetadata newIndexMetadata = newState.metadata().index(indexMetadata.getIndex());
                    if (newIndexMetadata == null) {
                        // The index has somehow been deleted - there shouldn't be any opportunity for this to happen, but just in case.
                        logger.debug("index [" + indexMetadata.getIndex() + "] has been deleted after moving to step [" +
                            concreteTargetKey.get() + "], skipping async action check");
                        return;
                    }
                    indexLifecycleService.maybeRunAsyncAction(newState, newIndexMetadata, concreteTargetKey.get());
                }
            });
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
