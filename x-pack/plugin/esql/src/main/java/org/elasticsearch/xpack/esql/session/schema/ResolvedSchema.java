/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session.schema;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.List;
import java.util.Map;

/**
 * The per-abstraction result of {@code resolve_schema}, one variant per kind. Rich, not flattened: the index variant
 * carries the full {@link IndexResolution} (modes, concrete indices, failures) plus the minimum transport version the
 * field-caps fetch observed (as a {@link Versioned}) so routing indices through the umbrella is not lossy — the session
 * still accumulates that per-pattern version into its overall minimum. Views and datasets carry what their kinds need.
 * {@link Attribute} is the merge currency the analyzer consumes, rebuilt from these.
 *
 * <p>This is the local carrier; the cross-cluster wire form (a {@code Writeable} response built from these) lands with
 * the federation leg. The {@link View} variant will additionally carry a remote-execution handle once the view
 * schema-return shape is settled.
 */
public sealed interface ResolvedSchema permits ResolvedSchema.Index, ResolvedSchema.View, ResolvedSchema.Dataset {

    /** The resolved abstraction's name (as written in the query, qualified for remotes). */
    String name();

    /**
     * An index / alias / data stream — the field-caps result behind the index provider, wrapped in the {@link Versioned}
     * the fetch returns so the session can thread the per-pattern minimum transport version into its accumulation.
     */
    record Index(String name, Versioned<IndexResolution> resolution) implements ResolvedSchema {}

    /**
     * A view — its result schema (the contract callers plan against) plus the unwrapped plan of its stored query
     * (the implementation; produced by reusing the existing view rewrite so we never unwrap twice, and what
     * execution runs). Splitting schema from implementation here is what decouples schema resolution from the
     * view-into-query rewrite while keeping that rewrite as the optimization.
     */
    record View(String name, List<Attribute> schema, LogicalPlan implementation) implements ResolvedSchema {}

    /** A dataset — the external-source config the coordinator turns into an external relation. */
    record Dataset(String name, Map<String, Object> config) implements ResolvedSchema {}
}
