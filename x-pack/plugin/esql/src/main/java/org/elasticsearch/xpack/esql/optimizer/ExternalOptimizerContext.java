/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;

/**
 * Container for external-source planning state attached to {@link LocalPhysicalOptimizerContext}.
 * <p>
 * Today it carries only the {@link FormatReaderRegistry}, which a small set of optimizer rules
 * consult to discover what an external source's underlying reader supports (filter pushdown,
 * aggregate pushdown, deferred column extraction). Encapsulating it here keeps the parent
 * context's signature stable as new external-source-only fields appear (e.g. capability sets,
 * per-source statistics caches): future additions land on this record, never on
 * {@link LocalPhysicalOptimizerContext}.
 * <p>
 * Instances are constructed once per local-plan invocation by
 * {@code PlannerUtils.localPlan(... FormatReaderRegistry ...)}; rules read through
 * {@link LocalPhysicalOptimizerContext#external()}. Use {@link #NONE} for callers (e.g.
 * coordinator-side optimization, lookup-service planning, tests) that have no external sources
 * in scope.
 */
public record ExternalOptimizerContext(FormatReaderRegistry formatReaderRegistry) {

    /**
     * Sentinel for callers without any external-source state. Rules that consult external
     * capabilities must treat {@code formatReaderRegistry == null} as "no information" and bail
     * out of the optimization, mirroring the previous behavior when the registry field was
     * unset on the parent context.
     */
    public static final ExternalOptimizerContext NONE = new ExternalOptimizerContext(null);
}
