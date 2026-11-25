/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

/**
 * Marks a physical exec that maps its child's rows one-to-one — it may add or drop <em>columns</em>, and reorder rows,
 * but it never changes how <em>many</em> rows flow through. It is the physical-tree counterpart of the logical
 * {@link org.elasticsearch.xpack.esql.plan.logical.Streaming} marker.
 *
 * <p>The distinction is what lets a filter above such a node be pushed <em>below</em> it: if the row count is
 * untouched, a predicate that excludes a source can be applied at the source instead, changing nothing about the
 * result. A node that selects rows by cardinality — a {@code LIMIT}, {@code TOP N}, {@code STATS}, {@code SAMPLE},
 * {@code MV_EXPAND}, a join — must <em>not</em> carry this marker: pushing a filter past it changes which rows survive.
 *
 * <p>Currently consulted only by partition-pruning seed propagation during split discovery
 * ({@code PartitionPruningRule#rowPreserving(PhysicalPlan)}). It is deliberately opt-in and fails closed: an exec
 * without the marker is treated as cardinality-sensitive, so forgetting it costs a missed optimization, never a wrong
 * result. Add it only to a node you are certain preserves row count.
 */
public interface RowCountPreserving {}
