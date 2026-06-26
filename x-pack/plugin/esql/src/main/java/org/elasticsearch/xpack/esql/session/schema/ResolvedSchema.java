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
public sealed interface ResolvedSchema permits ResolvedSchema.Index, ResolvedSchema.View, ResolvedSchema.Dataset, ResolvedSchema.Remote {

    /** The resolved abstraction's name (as written in the query, qualified for remotes). */
    String name();

    /**
     * The resolved abstraction's columns as {@link Attribute}s — the single merge currency the analyzer consumes and
     * the only thing that crosses a cluster boundary (a remote cluster's internal {@link IndexResolution}/expanded-view
     * plan stays home). Each variant projects its native shape onto this list: an index flattens its field-caps mapping,
     * a view returns its resolved output, a dataset its external-source columns (none derived yet — POC stub).
     */
    List<Attribute> attributes();

    /**
     * An index / alias / data stream — the field-caps result behind the index provider, wrapped in the {@link Versioned}
     * the fetch returns so the session can thread the per-pattern minimum transport version into its accumulation.
     */
    record Index(String name, Versioned<IndexResolution> resolution) implements ResolvedSchema {
        @Override
        public List<Attribute> attributes() {
            IndexResolution inner = resolution.inner();
            if (inner.isValid() == false) {
                return List.of();
            }
            return org.elasticsearch.xpack.esql.analysis.Analyzer.mappingAsAttributes(
                org.elasticsearch.xpack.esql.core.tree.Source.EMPTY,
                inner.get().mapping()
            );
        }
    }

    /**
     * A view — its result schema (the contract callers plan against) plus the unwrapped plan of its stored query
     * (the implementation; produced by reusing the existing view rewrite so we never unwrap twice, and what
     * execution runs). Splitting schema from implementation here is what decouples schema resolution from the
     * view-into-query rewrite while keeping that rewrite as the optimization.
     */
    record View(String name, List<Attribute> schema, LogicalPlan implementation) implements ResolvedSchema {
        @Override
        public List<Attribute> attributes() {
            return schema;
        }
    }

    /** A dataset — the external-source config the coordinator turns into an external relation. */
    record Dataset(String name, Map<String, Object> config) implements ResolvedSchema {
        @Override
        public List<Attribute> attributes() {
            // The dataset's external-source schema is resolved later (external-source resolve); the POC remote leg
            // carries the config-bearing local variant, so no flat attribute projection exists here yet.
            return List.of();
        }
    }

    /**
     * The wire form a remote cluster returns: a kind tag, the qualified name, the resolved columns as {@link Attribute}s
     * (the merge currency), and — for datasets — the external-source {@code config} the coordinator turns into an external
     * relation (per the federation design, a dataset's config is what the coordinator needs, not flat attributes). This is
     * what {@code resolve_schema} sends back across a cluster boundary; the coordinator cannot (and must not) receive a
     * remote's internal {@link IndexResolution} or expanded-view body, only schema + (for datasets) config. {@code kind}
     * preserves which provider resolved it on the remote so the coordinator can branch (a view additionally carries a
     * remote-execution handle once that lands — POC stub for now).
     */
    record Remote(String name, Kind kind, List<Attribute> attributes, Map<String, Object> config) implements ResolvedSchema {
        public enum Kind {
            INDEX,
            VIEW,
            DATASET
        }
    }
}
