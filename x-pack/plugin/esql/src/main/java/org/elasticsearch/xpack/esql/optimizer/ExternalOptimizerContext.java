/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;

import java.util.Map;

/**
 * Container for external-source planning state attached to {@link LocalPhysicalOptimizerContext}.
 * <p>
 * It carries the {@link FormatReaderRegistry}, which a small set of optimizer rules consult to
 * discover what a file-based external source's underlying reader supports (filter pushdown,
 * aggregate pushdown, deferred column extraction), and the connector {@code sourceFactories} map
 * (keyed by the source's compound scheme, e.g. {@code jdbc:postgresql}), which lets those same
 * rules reach a connector's {@link ExternalSourceFactory#filterPushdownSupport()} — connectors are
 * not registered in the {@link FormatReaderRegistry}, so this is the only path from the optimizer
 * to their pushdown support. Encapsulating both here keeps the parent context's signature stable
 * as new external-source-only fields appear: future additions land on this record, never on
 * {@link LocalPhysicalOptimizerContext}.
 * <p>
 * Instances are constructed once per local-plan invocation by
 * {@code PlannerUtils.localPlan(... FormatReaderRegistry, Map<String, ExternalSourceFactory> ...)};
 * rules read through {@link LocalPhysicalOptimizerContext#external()}. Use {@link #NONE} for callers
 * (e.g. coordinator-side optimization, lookup-service planning, tests) that have no external sources
 * in scope.
 */
public record ExternalOptimizerContext(FormatReaderRegistry formatReaderRegistry, Map<String, ExternalSourceFactory> sourceFactories) {

    /**
     * Convenience constructor for callers (chiefly tests) that supply only a
     * {@link FormatReaderRegistry} and have no connector factory map in scope. Connector pushdown
     * resolution is then unavailable (the map is {@code null}), exactly as before this field existed.
     */
    public ExternalOptimizerContext(FormatReaderRegistry formatReaderRegistry) {
        this(formatReaderRegistry, null);
    }

    /**
     * Sentinel for callers without any external-source state. Rules that consult external
     * capabilities must treat {@code formatReaderRegistry == null} (and {@code sourceFactories == null})
     * as "no information" and bail out of the optimization, mirroring the previous behavior when the
     * registry field was unset on the parent context.
     */
    public static final ExternalOptimizerContext NONE = new ExternalOptimizerContext(null, null);
}
