/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.TriConsumer;

import java.util.Objects;

/**
 * This step changes its {@link #getNextStepKey()} depending on the
 * outcome of a defined async predicate.
 */
public class AsyncBranchingStep extends AsyncActionStep {
    public static final String NAME = "branch";

    private final StepKey nextStepKeyOnFalse;
    private final StepKey nextStepKeyOnTrue;
    private final TriConsumer<IndexMetadata, ClusterState, ActionListener<Boolean>> asyncPredicate;
    private final SetOnce<Boolean> predicateValue;

    public AsyncBranchingStep(
        StepKey key,
        StepKey nextStepKeyOnFalse,
        StepKey nextStepKeyOnTrue,
        TriConsumer<IndexMetadata, ClusterState, ActionListener<Boolean>> asyncPredicate,
        Client client
    ) {
        // super.nextStepKey is set to null since it is not used by this step
        super(key, null, client);
        this.nextStepKeyOnFalse = nextStepKeyOnFalse;
        this.nextStepKeyOnTrue = nextStepKeyOnTrue;
        this.asyncPredicate = asyncPredicate;
        this.predicateValue = new SetOnce<>();
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public void performAction(
        IndexMetadata indexMetadata,
        ClusterState currentClusterState,
        ClusterStateObserver observer,
        ActionListener<Void> listener
    ) {
        asyncPredicate.apply(indexMetadata, currentClusterState, listener.map(value -> {
            predicateValue.set(value);
            return null;
        }));
    }

    @Override
    public final StepKey getNextStepKey() {
        if (predicateValue.get() == null) {
            throw new IllegalStateException("Cannot call getNextStepKey before performAction");
        }
        return predicateValue.get() ? nextStepKeyOnTrue : nextStepKeyOnFalse;
    }

    /**
     * @return the next step if {@code predicate} is false
     */
    final StepKey getNextStepKeyOnFalse() {
        return nextStepKeyOnFalse;
    }

    /**
     * @return the next step if {@code predicate} is true
     */
    final StepKey getNextStepKeyOnTrue() {
        return nextStepKeyOnTrue;
    }

    /**
     * @return the next step if {@code predicate} is true
     */
    final TriConsumer<IndexMetadata, ClusterState, ActionListener<Boolean>> getAsyncPredicate() {
        return asyncPredicate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        AsyncBranchingStep that = (AsyncBranchingStep) o;
        return super.equals(o)
            && Objects.equals(nextStepKeyOnFalse, that.nextStepKeyOnFalse)
            && Objects.equals(nextStepKeyOnTrue, that.nextStepKeyOnTrue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), nextStepKeyOnFalse, nextStepKeyOnTrue);
    }
}
