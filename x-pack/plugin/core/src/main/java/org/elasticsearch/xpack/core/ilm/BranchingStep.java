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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.Index;

import java.util.Objects;
import java.util.function.BiPredicate;

/**
 * This step changes its {@link #getNextStepKey()} depending on the
 * outcome of a defined predicate. It performs no changes to the
 * cluster state.
 */
public class BranchingStep extends ClusterStateActionStep {
    public static final String NAME = "branch";

    private static final Logger logger = LogManager.getLogger(BranchingStep.class);

    private StepKey nextStepKeyOnFalse;
    private StepKey nextStepKeyOnTrue;
    private BiPredicate<Index, ClusterState> predicate;
    private SetOnce<Boolean> predicateValue;

    /**
     * {@link BranchingStep} is a step whose next step is based on
     * the return value of a specific predicate.
     *
     * @param key                the step's key
     * @param nextStepKeyOnFalse the key of the step to run if predicate returns false
     * @param nextStepKeyOnTrue  the key of the step to run if predicate returns true
     * @param predicate          the condition to check when deciding which step to run next
     */
    public BranchingStep(StepKey key, StepKey nextStepKeyOnFalse, StepKey nextStepKeyOnTrue, BiPredicate<Index, ClusterState> predicate) {
        // super.nextStepKey is set to null since it is not used by this step
        super(key, null);
        this.nextStepKeyOnFalse = nextStepKeyOnFalse;
        this.nextStepKeyOnTrue = nextStepKeyOnTrue;
        this.predicate = predicate;
        this.predicateValue = new SetOnce<>();
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public ClusterState performAction(Index index, ClusterState clusterState) {
        IndexMetadata indexMetadata = clusterState.metadata().index(index);
        if (indexMetadata == null) {
            // Index must have been since deleted, ignore it
            logger.debug("[{}] lifecycle action for index [{}] executed but index no longer exists", getKey().getAction(), index.getName());
            return clusterState;
        }
        predicateValue.set(predicate.test(index, clusterState));
        return clusterState;
    }

    /**
     * This method returns the next step to execute based on the predicate. If
     * the predicate returned true, then nextStepKeyOnTrue is the key of the
     * next step to run, otherwise nextStepKeyOnFalse is.
     *
     * throws {@link UnsupportedOperationException} if performAction was not called yet
     *
     * @return next step to execute
     */
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

    public final BiPredicate<Index, ClusterState> getPredicate() {
        return predicate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        BranchingStep that = (BranchingStep) o;
        return super.equals(o)
            && Objects.equals(nextStepKeyOnFalse, that.nextStepKeyOnFalse)
            && Objects.equals(nextStepKeyOnTrue, that.nextStepKeyOnTrue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), nextStepKeyOnFalse, nextStepKeyOnTrue);
    }
}
