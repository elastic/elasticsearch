/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.core.Nullable;

/**
 * Marks a command that the {@code _doc} column can pass through on the data nodes, each output row keeping the
 * {@code _doc} of the input row it comes from. There the command runs inside the fragment, and {@code _doc} points at
 * the shards of the node. Several output rows can keep the {@code _doc} of one input row.
 * <p>
 * It is a separate question from {@link DocPreserving}. {@link ChangePoint} and
 * {@link org.elasticsearch.xpack.esql.plan.logical.inference.Rerank} keep rows tied to documents, but they run only on
 * the coordinator, where {@code _doc} has left the node that read the documents. There a physical exec answers, through
 * {@link org.elasticsearch.xpack.esql.plan.physical.CarriesDetachedDoc}. When the physical optimizer runs, the plan of
 * the data nodes is still a logical fragment, and the plan of the coordinator is already physical. A {@code LOOKUP JOIN}
 * adds the columns of another index to each row, so it isn't {@link DocPreserving}, but the {@code _doc} of its left
 * rows passes through it.
 * <p>
 * A command without the marker keeps the query loading its columns before the command, as it does without the fetch
 * phase, which is always correct. Add it only once a test shows that the fetch phase returns the same rows through the
 * command.
 */
public interface CarriesDocOnDataNodes {

    /**
     * The input whose {@code _doc} each output row keeps, or {@code null} when this command, as configured, doesn't let
     * {@code _doc} through.
     */
    @Nullable
    default LogicalPlan docInput() {
        if (this instanceof UnaryPlan unaryPlan) {
            return unaryPlan.child();
        }
        throw new IllegalStateException(getClass().getName() + " must override docInput()");
    }
}
