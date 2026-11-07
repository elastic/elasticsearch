/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import java.util.Map;

/**
 * The node stage of a {@link TableFunction}: implemented only by a function whose leaf reaches a data node and must
 * derive what it reads there, rather than take it from the plan.
 * <p>
 * A separate interface, not a method on {@link TableFunction} with a default that fails, so that having a node stage
 * is something the type says. The dispatcher asks whether a function implements this; it never calls and hopes. A
 * generator, or anything built from the request itself, finishes on the coordinator and does not implement it — and
 * so cannot be asked a question it has no answer to.
 * <p>
 * <b>Why a node stage exists at all.</b> A data node does not trust a plan's configuration. For
 * {@code FROM <dataset>} it takes the dataset's name off the plan and rebuilds the read from its own cluster state,
 * which is what stops a crafted plan choosing credentials, a role or an endpoint. A table function has no registered
 * name to play that part, so the call plays it: the call travels, and this interface is where the function turns it
 * back into a read against the state of whichever node is asking.
 * <p>
 * <b>Runs more than once.</b> The rebind is applied at the top of {@code PlannerUtils.localPlan}, again inside its
 * {@code FragmentExec} transform, and again in {@code ComputeService.runCompute} — so an implementation is
 * idempotent, has no side effects, and does no blocking I/O.
 * <p>
 * <b>It is also a consistency check.</b> The coordinator's listing, schema and splits were computed under its own
 * binding, so a {@link Read} that disagrees with what the plan carried is refused rather than read: a data source
 * edited between planning and execution would otherwise have this node read the coordinator's splits under a
 * different endpoint or region. {@code DatasetConfigResolver.rebind} makes exactly that comparison for a registered
 * dataset, against {@code DefinitionVersion.CONFIG_KEY}.
 * <p>
 * <b>The rule.</b> Everything needed here must be derivable from {@link TableFunctionRequest} and the node's own
 * cluster state. A function must not compute something on the coordinator, leave it on the plan and read it back —
 * what it reads back is what an attacker gets to write. Deriving twice from one input is the point: two derivations
 * of one configuration would be two things to keep in step, and the plan-carried one would be the weaker.
 */
public interface BindsOnDataNode {

    /**
     * What a read resolves to on this node.
     *
     * @param resource the resource the read addresses
     * @param config the configuration to read it under, built from this node's own state
     */
    record Read(String resource, Map<String, Object> config) {}

    /**
     * Derives the read from the call and {@code context}'s project — this node's project, not the coordinator's.
     * <p>
     * Runs after the shape has crossed the wire, so it re-establishes whatever it needs rather than assuming the
     * coordinator's checks still hold for the bytes that arrived.
     */
    Read bindOnDataNode(TableFunctionRequest request, TableFunctionContext context);
}
