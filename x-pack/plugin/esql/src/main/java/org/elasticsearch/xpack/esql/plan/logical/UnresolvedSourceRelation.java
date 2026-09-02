/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

/**
 * Marker for the unresolved leaf plans the parser produces for {@code FROM}-style source commands. The interface is
 * {@code sealed}: every shape must be listed in the {@code permits} clause, so adding one is a compile-time decision
 * rather than a silent drift. Today there are two shapes:
 * <ul>
 *   <li>{@link UnresolvedRelation} — the index-pattern path ({@code FROM <index>}, {@code TS}, etc.).</li>
 *   <li>{@link UnresolvedExternalRelation} — the external/dataset path ({@code FROM <dataset>} and inline
 *       {@code EXTERNAL}).</li>
 * </ul>
 *
 * <p>This is a pure marker with no methods. The two implementations expose different "primary identifiers"
 * ({@link UnresolvedRelation#indexPattern()} returns an {@code IndexPattern},
 * {@link UnresolvedExternalRelation#tablePath()} returns an {@code Expression}), so there is no clean common
 * accessor to extract. The marker exists solely so a traversal that wants <em>any</em> FROM-style leaf can match on
 * {@code instanceof UnresolvedSourceRelation} (or {@link Class#isInstance}) instead of enumerating the concrete
 * classes and silently drifting whenever a new shape is added.
 *
 * <p>Note this is a plain interface, not a {@code LogicalPlan} subtype, so it cannot be passed to the typed
 * {@code forEachUp(Class, Consumer)} / {@code transformUp(Class, ...)} overloads (those require a
 * {@code LogicalPlan} bound). Match it with an {@code instanceof} test inside a plain
 * {@code forEachUp(Consumer)} / {@code collect(Predicate)} traversal instead.
 *
 * <p><strong>Adding a new traversal site?</strong> Decide deliberately whether it should fire on every source
 * shape or only one:
 * <ul>
 *   <li>If it cares about any FROM-style leaf (e.g. pre-analysis schema/field collection), match on this marker.
 *       Cast to the concrete type inside the handler if you need a shape-specific accessor.</li>
 *   <li>If it is genuinely specific to one shape (e.g. index resolution, time-series-mode detection, view
 *       resolution), keep matching the concrete class. The shape-specific accessor it calls — {@code indexMode()},
 *       {@code indexPattern()}, {@code metadataFields()}, etc. — already signals the intent; only add a comment
 *       where the single-shape choice is not self-evident from the surrounding code.</li>
 * </ul>
 *
 * @see UnresolvedRelation
 * @see UnresolvedExternalRelation
 */
public sealed interface UnresolvedSourceRelation permits UnresolvedRelation, UnresolvedExternalRelation {}
