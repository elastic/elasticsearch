/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session.schema;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.index.IndexResolution;

import java.util.List;
import java.util.Map;

/**
 * The per-abstraction result of {@code resolve_schema}, one variant per kind. Rich, not flattened: the index variant
 * carries the full {@link IndexResolution} (modes, concrete indices, failures, min-transport-version) so routing
 * indices through the umbrella is not lossy; views and datasets carry what their kinds need. {@link Attribute} is the
 * merge currency the analyzer consumes, rebuilt from these.
 *
 * <p>This is the local carrier; the cross-cluster wire form (a {@code Writeable} response built from these) lands with
 * the federation leg. The {@link View} variant will additionally carry a remote-execution handle once the view
 * schema-return shape is settled.
 */
public sealed interface ResolvedSchema permits ResolvedSchema.Index, ResolvedSchema.View, ResolvedSchema.Dataset {

    /** The resolved abstraction's name (as written in the query, qualified for remotes). */
    String name();

    /** An index / alias / data stream — the field-caps result behind the index provider. */
    record Index(String name, IndexResolution resolution) implements ResolvedSchema {}

    /** A view — its result schema (the output of its stored query). Remote-exec handle to follow. */
    record View(String name, List<Attribute> schema) implements ResolvedSchema {}

    /** A dataset — the external-source config the coordinator turns into an external relation. */
    record Dataset(String name, Map<String, Object> config) implements ResolvedSchema {}
}
