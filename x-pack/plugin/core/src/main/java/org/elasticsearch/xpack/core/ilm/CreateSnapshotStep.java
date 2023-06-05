/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotNameAlreadyInUseException;

import java.util.Locale;
import java.util.Objects;

/**
 * Creates a snapshot of the managed index into the configured repository and snapshot name. The repository and snapshot names are expected
 * to be present in the lifecycle execution state (usually generated and stored by a different ILM step)
 */
public class CreateSnapshotStep extends AsyncRetryDuringSnapshotActionStep {
    public static final String NAME = "create-snapshot";

    private static final Logger logger = LogManager.getLogger(CreateSnapshotStep.class);

    private final StepKey nextKeyOnComplete;
    private final StepKey nextKeyOnIncomplete;
    private final SetOnce<Boolean> onResponseResult;

    public CreateSnapshotStep(StepKey key, StepKey nextKeyOnComplete, StepKey nextKeyOnIncomplete, Client client) {
        // super.nextStepKey is set to null since it is not used by this step
        super(key, null, client);
        this.nextKeyOnComplete = nextKeyOnComplete;
        this.nextKeyOnIncomplete = nextKeyOnIncomplete;
        this.onResponseResult = new SetOnce<>();
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    void performDuringNoSnapshot(IndexMetadata indexMetadata, ClusterState currentClusterState, ActionListener<Void> listener) {
        createSnapshot(indexMetadata, new ActionListener<>() {
            @Override
            public void onResponse(Boolean complete) {
                // based on the result of action we'll decide what the next step will be
                onResponseResult.set(complete);
                // the execution was successful from ILM's perspective ie. will go to the next step
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof SnapshotNameAlreadyInUseException snapshotNameAlreadyInUseException) {
                    // we treat a snapshot that was already created before this step as an incomplete snapshot. This scenario is triggered
                    // by a master restart or a failover which can result in a double invocation of this step.
                    logger.warn(
                        "snapshot [{}] is already in-progress or in-use for index [{}], ILM will attempt to clean it up and recreate it",
                        snapshotNameAlreadyInUseException.getSnapshotName(),
                        indexMetadata.getIndex().getName()
                    );
                    onResponse(false);
                } else {
                    listener.onFailure(e);
                }
            }
        });
    }

    void createSnapshot(IndexMetadata indexMetadata, ActionListener<Boolean> listener) {
        final String indexName = indexMetadata.getIndex().getName();

        final LifecycleExecutionState lifecycleState = indexMetadata.getLifecycleExecutionState();

        final String policyName = indexMetadata.getLifecyclePolicyName();
        final String snapshotRepository = lifecycleState.snapshotRepository();
        if (Strings.hasText(snapshotRepository) == false) {
            listener.onFailure(
                new IllegalStateException(
                    "snapshot repository is not present for policy [" + policyName + "] and index [" + indexName + "]"
                )
            );
            return;
        }

        final String snapshotName = lifecycleState.snapshotName();
        if (Strings.hasText(snapshotName) == false) {
            listener.onFailure(
                new IllegalStateException("snapshot name was not generated for policy [" + policyName + "] and index [" + indexName + "]")
            );
            return;
        }
        CreateSnapshotRequest request = new CreateSnapshotRequest(snapshotRepository, snapshotName);
        request.indices(indexName);
        // this is safe as the snapshot creation will still be async, it's just that the listener will be notified when the snapshot is
        // complete
        request.waitForCompletion(true);
        request.includeGlobalState(false);
        request.masterNodeTimeout(TimeValue.MAX_VALUE);
        getClient().admin().cluster().createSnapshot(request, listener.wrapFailure((l, response) -> {
            logger.debug(
                "create snapshot response for policy [{}] and index [{}] is: {}",
                policyName,
                indexName,
                Strings.toString(response)
            );
            final SnapshotInfo snapInfo = response.getSnapshotInfo();

            // Check that there are no failed shards, since the request may not entirely
            // fail, but may still have failures (such as in the case of an aborted snapshot)
            if (snapInfo.failedShards() == 0) {
                l.onResponse(true);
            } else {
                int failures = snapInfo.failedShards();
                int total = snapInfo.totalShards();
                String message = String.format(
                    Locale.ROOT,
                    "failed to create snapshot successfully, %s failures out of %s total shards failed",
                    failures,
                    total
                );
                logger.warn(message);
                l.onResponse(false);
            }
        }));
    }

    @Override
    public final StepKey getNextStepKey() {
        if (onResponseResult.get() == null) {
            throw new IllegalStateException("cannot call getNextStepKey before performAction");
        }
        return onResponseResult.get() ? nextKeyOnComplete : nextKeyOnIncomplete;
    }

    /**
     * The step key to be reported as the {@link #getNextStepKey} if the response of {@link #getKey()} is
     * false.
     */
    StepKey getNextKeyOnIncomplete() {
        return nextKeyOnIncomplete;
    }

    /**
     * The step key to be reported as the {@link #getNextStepKey} if the response of {@link #getKey()} is
     * true.
     */
    StepKey getNextKeyOnComplete() {
        return nextKeyOnComplete;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        CreateSnapshotStep that = (CreateSnapshotStep) o;
        return Objects.equals(nextKeyOnComplete, that.nextKeyOnComplete) && Objects.equals(nextKeyOnIncomplete, that.nextKeyOnIncomplete);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), nextKeyOnComplete, nextKeyOnIncomplete);
    }
}
