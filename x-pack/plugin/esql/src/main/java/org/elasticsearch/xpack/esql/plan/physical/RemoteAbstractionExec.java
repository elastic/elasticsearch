/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.List;

/**
 * The shape shared by the two physical remote-abstraction leaves — {@link RemoteViewExec} and {@link RemoteDatasetExec} —
 * so {@code LocalExecutionPlanner} lowers them through a single kind-blind {@code planRemoteAbstraction} branch. The two
 * differ only in what they are named after (a view vs a dataset); the coordinator treats them identically because the
 * home cluster resolves either name through the same kind-blind {@code SchemaService} umbrella (see
 * {@code AbstractionComputeHandler}). This interface exposes exactly what the leaf's source-operator dispatch needs: the
 * {@link #abstractionName()} to ship, the {@link #handle()} of the home cluster to ship it to, and the resolved
 * {@link #output()} the coordinator built the leaf's layout from and the home cluster validates its fresh resolution
 * against (the B1 schema-drift guard).
 */
public interface RemoteAbstractionExec {

    /** The view/dataset name to resolve and run on the home cluster — the abstraction's identity, never query text. */
    String abstractionName();

    /** The cluster alias of the abstraction's home cluster (empty string for a local/same-cluster abstraction). */
    String handle();

    /** The output schema the coordinator resolved against; the layout is built from this and drift is checked against it. */
    List<Attribute> output();
}
