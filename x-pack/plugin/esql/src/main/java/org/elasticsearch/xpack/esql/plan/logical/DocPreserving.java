/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.plan.logical.fuse.Fuse;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;

/**
 * Marker interface for commands where every output row still corresponds to exactly one document of the input. Adding columns to a row,
 * dropping whole rows, reordering rows, and fanning one row out into several that all describe the same document all preserve that
 * binding; deriving a row from more than one document does not.
 * <p>
 * {@link InlineStats} <b>is</b> doc-preserving even though it runs an aggregation: its output rows are the input rows
 * with the aggregate columns joined back on, so every row still maps to one document. Plain {@link Aggregate} is not,
 * because it collapses many documents into one summary row.
 * <p>
 * Commands that are <b>not</b> doc-preserving:
 * <ul>
 *     <li>{@link Aggregate}: a row summarizes many documents.</li>
 *     <li>{@link Fuse}: merges rows originating from different branches.</li>
 *     <li>{@link Fork} and {@link Join}: rows come from more than one input, so "the" document is ambiguous.</li>
 *     <li>Nodes that synthesize rows rather than carry them through, such as {@link InsertEmptyBuckets}.</li>
 * </ul>
 * <p>
 * Implement this on the command. Do not maintain a consumer-side allowlist: walkers should follow
 * {@link #preservingInput()} instead of special-casing individual commands.
 */
public interface DocPreserving {

    /**
     * The nearest upstream plan that still produces the same per-document rows this command emits.
     * Unary commands default to {@link UnaryPlan#child()}. {@link InlineStats} overrides this to skip its
     * wrapped {@link Aggregate}, which is the computation rather than a row-producing stage.
     */
    default LogicalPlan preservingInput() {
        assert this instanceof UnaryPlan : getClass().getName() + " is DocPreserving but not a UnaryPlan";
        return ((UnaryPlan) this).child();
    }
}
