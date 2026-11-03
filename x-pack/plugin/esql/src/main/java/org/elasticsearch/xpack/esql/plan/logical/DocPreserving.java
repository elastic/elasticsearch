/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

/**
 * Marks commands that keep every output row tied to one input document. Filtering, reordering, adding columns, and
 * expanding a multivalued field preserve that tie; combining multiple documents into one row does not.
 * <p>
 * {@link InlineStats} is doc-preserving because it joins aggregate values back onto the input rows. {@link Aggregate}
 * is not because each output row summarizes multiple documents. {@code IN} / {@code NOT IN} subquery joins are
 * doc-preserving through their left side only: the subquery decides which rows survive but contributes none.
 * Commands that synthesize rows or combine inputs, such as {@code FUSE}, {@code FORK}, {@code LOOKUP JOIN}, and
 * {@code FROM} subqueries, are deliberately excluded.
 * Consumers should follow {@link #preservingInput()} instead of maintaining their own command allowlist.
 * <p>
 * Whether the {@code _doc} column itself can pass through a command is a separate question, answered by
 * {@link #carriesDocOnDataNodes()}.
 */
public interface DocPreserving {

    /**
     * Returns the nearest upstream plan with the same document rows. {@link InlineStats} overrides this to skip its
     * internal aggregate.
     */
    default LogicalPlan preservingInput() {
        if (this instanceof UnaryPlan unaryPlan) {
            return unaryPlan.child();
        }
        throw new IllegalStateException(getClass().getName() + " must override preservingInput()");
    }

    /**
     * Whether the {@code _doc} column can pass through this command on the data nodes, each row keeping the {@code _doc}
     * of its document. There the command runs inside the fragment, and {@code _doc} points at the shards of the node.
     * <p>
     * Keeping rows tied to documents is not enough. {@link ChangePoint} and
     * {@link org.elasticsearch.xpack.esql.plan.logical.inference.Rerank} do, but they run only on the coordinator, where
     * {@code _doc} has left the node that read the documents. There a physical exec answers, through
     * {@link org.elasticsearch.xpack.esql.plan.physical.CarriesDetachedDoc}. When the physical optimizer runs, the plan
     * of the data nodes is still a logical fragment, and the plan of the coordinator is already physical.
     * <p>
     * The default is {@code false}: the query loads its columns before the command, as it does without the fetch phase,
     * which is always correct. Answer {@code true} only once a test shows that the fetch phase returns the same rows
     * through the command.
     */
    default boolean carriesDocOnDataNodes() {
        return false;
    }
}
