/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.metrics;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Registers APM instruments for the node-level analyzer cache so operators can observe the sharing
 * factor delivered by {@link AnalysisRegistry}'s recipe-keyed cache.
 *
 * <p>Two gauges are the core of "shared vs total":
 * <ul>
 *   <li>{@code es.analysis.analyzers.unique.current} — number of unique cached analyzer instances
 *       (one entry per distinct recipe).</li>
 *   <li>{@code es.analysis.analyzers.references.current} — total live references held by every
 *       index on the node, read directly from the cache entries' reference counts. Each index
 *       holds one reference per cache entry it uses; ratio (references / unique) is the average
 *       sharing factor.</li>
 * </ul>
 *
 * <p>Additional observability instruments: a normalizer-cache-size gauge, plus monotonic cache
 * hit / miss counters accumulated since process start (useful for sharing-rate over time).
 */
public class AnalyzerMetrics extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(AnalyzerMetrics.class);

    public static final String UNIQUE_ANALYZERS_METRIC = "es.analysis.analyzers.unique.current";
    public static final String TOTAL_REFERENCES_METRIC = "es.analysis.analyzers.references.current";
    public static final String UNIQUE_NORMALIZERS_METRIC = "es.analysis.normalizers.unique.current";
    public static final String CACHE_HITS_METRIC = "es.analysis.cache.hits.total";
    public static final String CACHE_MISSES_METRIC = "es.analysis.cache.misses.total";

    private final MeterRegistry registry;
    private final AnalysisRegistry analysisRegistry;
    private final List<AutoCloseable> metrics = new ArrayList<>();

    public AnalyzerMetrics(MeterRegistry registry, AnalysisRegistry analysisRegistry) {
        this.registry = registry;
        this.analysisRegistry = analysisRegistry;
    }

    @Override
    protected void doStart() {
        metrics.add(
            registry.registerLongGauge(
                UNIQUE_ANALYZERS_METRIC,
                "unique cached analyzer instances on this node (one per distinct recipe)",
                "1",
                () -> new LongWithAttributes(analysisRegistry.analyzerCacheSize())
            )
        );
        metrics.add(
            registry.registerLongGauge(
                TOTAL_REFERENCES_METRIC,
                "total references to cached analyzers across every index on this node (analyzer cache "
                    + "only; sharing factor = references / unique; 1.0 = no sharing, higher = more sharing)",
                "1",
                () -> new LongWithAttributes(analysisRegistry.totalReferences())
            )
        );
        metrics.add(
            registry.registerLongGauge(
                UNIQUE_NORMALIZERS_METRIC,
                "unique cached normalizer instances on this node (sum of keyword and whitespace variants)",
                "1",
                () -> new LongWithAttributes(analysisRegistry.normalizerCacheSize())
            )
        );
        // Hits/misses only ever increase (their {@code .total} suffix and monotonic semantics make them
        // counters, not gauges), so register them as async counters rather than gauges.
        metrics.add(
            registry.registerLongAsyncCounter(
                CACHE_HITS_METRIC,
                "cumulative analyzer-cache hits since process start",
                "1",
                () -> new LongWithAttributes(analysisRegistry.cacheHits())
            )
        );
        metrics.add(
            registry.registerLongAsyncCounter(
                CACHE_MISSES_METRIC,
                "cumulative analyzer-cache misses (fresh builds) since process start",
                "1",
                () -> new LongWithAttributes(analysisRegistry.cacheMisses())
            )
        );
    }

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() throws IOException {
        for (AutoCloseable c : metrics) {
            try {
                c.close();
            } catch (Exception e) {
                // gauges only hold a registry handle; nothing to recover from a close failure
                logger.debug("failed to close analyzer metric", e);
            }
        }
        metrics.clear();
    }
}
