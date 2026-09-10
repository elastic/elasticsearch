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
 * is not because each output row summarizes multiple documents.
 * Commands that synthesize rows or combine inputs, such as {@code FUSE}, {@code FORK}, and {@code JOIN}, are also
 * deliberately excluded.
 * Consumers should follow {@link #preservingInput()} instead of maintaining their own command allowlist.
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
}
