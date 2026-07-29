/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics;
import org.elasticsearch.core.TimeValue;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The write-path form of {@link DataStreamDerivedMetrics}: predicates are compiled, built-in selectors are expanded, dimensions are
 * resolved per metric, and the set of source paths that must be read is known up front.
 *
 * <p>Compiling once per configuration keeps the per-document work down to evaluating already-built predicates and reading a small,
 * known set of source paths. When {@link #requiredPaths()} is empty the write path never touches {@code _source} at all, which is the
 * common case for a stream that only asks for the built-in ingest metrics.
 *
 * <p>{@link #triggers()} is precomputed for the same reason: it lets the write path decide whether a given write is interesting at all
 * without walking the metric list.
 */
public record CompiledDerivedMetrics(
    List<Interval> intervals,
    List<CompiledMetric> metrics,
    Set<String> requiredPaths,
    List<String> unsupportedMetrics,
    EnumSet<Trigger> triggers
) {

    /**
     * A configured interval, keeping the user's rendering of it around because it becomes a dimension on the emitted documents.
     */
    public record Interval(String name, long millis) {}

    /**
     * How the values observed within one interval are reduced into the single value that gets emitted.
     */
    public enum Reduction {
        SUM,
        MIN,
        MAX,
        AVG,
        FIRST,
        LAST,
        /**
         * Sum divided by the interval length in seconds. Used by the built-in {@code *.rate} metrics.
         */
        RATE
    }

    /**
     * What a document contributes to a metric, once the predicate matched.
     */
    public sealed interface Source {
        /**
         * A fixed contribution, used by counters with a constant value and by the document/failure counting built-ins.
         */
        record Constant(double value) implements Source {}

        /**
         * The numeric value of a source field. Documents where the field is absent or not numeric contribute nothing.
         */
        record Field(String path) implements Source {}

        /**
         * The size of the document's source in bytes, used by the {@code ingest.bytes.*} built-ins.
         */
        record DocumentSize() implements Source {}
    }

    /**
     * Whether a metric observes successful writes, failed writes, or both.
     */
    public enum Trigger {
        SUCCESS,
        FAILURE
    }

    public record CompiledMetric(
        String name,
        Trigger trigger,
        Reduction reduction,
        DerivedMetricsPredicate predicate,
        Source source,
        List<String> dimensions
    ) {}

    private static final String INGEST_DOCS_COUNT = "ingest.docs.count";
    private static final String INGEST_DOCS_RATE = "ingest.docs.rate";
    private static final String INGEST_BYTES_COUNT = "ingest.bytes.count";
    private static final String INGEST_BYTES_RATE = "ingest.bytes.rate";
    private static final String INGEST_FAILURES_COUNT = "ingest.failures.count";
    private static final String INGEST_FAILURES_RATE = "ingest.failures.rate";

    private static final List<String> ALL_BUILTINS = List.of(
        INGEST_DOCS_COUNT,
        INGEST_DOCS_RATE,
        INGEST_BYTES_COUNT,
        INGEST_BYTES_RATE,
        INGEST_FAILURES_COUNT,
        INGEST_FAILURES_RATE
    );

    public static CompiledDerivedMetrics compile(DataStreamDerivedMetrics config) {
        List<Interval> intervals = new ArrayList<>(config.intervals().size());
        for (TimeValue interval : config.intervals()) {
            intervals.add(new Interval(interval.getStringRep(), interval.millis()));
        }

        Set<String> requiredPaths = new LinkedHashSet<>(config.dimensions());
        List<CompiledMetric> metrics = new ArrayList<>();
        for (String builtin : expandBuiltins(config.builtin())) {
            metrics.add(compileBuiltin(builtin, config.dimensions()));
        }

        List<String> unsupported = new ArrayList<>();
        for (DataStreamDerivedMetrics.Metric metric : config.metrics()) {
            if (metric.type() == DataStreamDerivedMetrics.MetricType.HISTOGRAM) {
                // histogram emission needs a histogram representation that this module cannot map yet, see docs/internal/DerivedMetrics.md
                unsupported.add(metric.name());
                continue;
            }
            List<String> dimensions = mergeDimensions(config.dimensions(), metric.dimensions());
            requiredPaths.addAll(dimensions);
            DerivedMetricsPredicate.collectPaths(metric.when(), requiredPaths);
            Source source;
            if (metric.value().field() != null) {
                source = new Source.Field(metric.value().field());
                requiredPaths.add(metric.value().field());
            } else {
                source = new Source.Constant(metric.value().constant());
            }
            metrics.add(
                new CompiledMetric(
                    metric.name(),
                    Trigger.SUCCESS,
                    reductionFor(metric),
                    DerivedMetricsPredicate.compile(metric.when()),
                    source,
                    dimensions
                )
            );
        }

        EnumSet<Trigger> triggers = EnumSet.noneOf(Trigger.class);
        for (CompiledMetric metric : metrics) {
            triggers.add(metric.trigger());
        }

        return new CompiledDerivedMetrics(
            List.copyOf(intervals),
            List.copyOf(metrics),
            Set.copyOf(requiredPaths),
            List.copyOf(unsupported),
            triggers
        );
    }

    private static List<String> expandBuiltins(List<String> builtin) {
        Set<String> expanded = new LinkedHashSet<>();
        for (String selector : builtin) {
            if ("ingest.*".equals(selector)) {
                expanded.addAll(ALL_BUILTINS);
            } else {
                expanded.add(selector);
            }
        }
        return List.copyOf(expanded);
    }

    private static CompiledMetric compileBuiltin(String name, List<String> dimensions) {
        return switch (name) {
            case INGEST_DOCS_COUNT -> builtin(name, Trigger.SUCCESS, Reduction.SUM, new Source.Constant(1.0), dimensions);
            case INGEST_DOCS_RATE -> builtin(name, Trigger.SUCCESS, Reduction.RATE, new Source.Constant(1.0), dimensions);
            case INGEST_BYTES_COUNT -> builtin(name, Trigger.SUCCESS, Reduction.SUM, new Source.DocumentSize(), dimensions);
            case INGEST_BYTES_RATE -> builtin(name, Trigger.SUCCESS, Reduction.RATE, new Source.DocumentSize(), dimensions);
            case INGEST_FAILURES_COUNT -> builtin(name, Trigger.FAILURE, Reduction.SUM, new Source.Constant(1.0), dimensions);
            case INGEST_FAILURES_RATE -> builtin(name, Trigger.FAILURE, Reduction.RATE, new Source.Constant(1.0), dimensions);
            default -> throw new IllegalArgumentException("unsupported derived metrics builtin [" + name + "]");
        };
    }

    private static CompiledMetric builtin(String name, Trigger trigger, Reduction reduction, Source source, List<String> dimensions) {
        return new CompiledMetric(name, trigger, reduction, DerivedMetricsPredicate.MATCH_ALL, source, List.copyOf(dimensions));
    }

    private static Reduction reductionFor(DataStreamDerivedMetrics.Metric metric) {
        return switch (metric.type()) {
            case COUNTER -> Reduction.SUM;
            case GAUGE -> switch (metric.aggregation()) {
                case FIRST_VALUE -> Reduction.FIRST;
                case LAST_VALUE -> Reduction.LAST;
                case MIN -> Reduction.MIN;
                case MAX -> Reduction.MAX;
                case AVG -> Reduction.AVG;
                case SUM -> Reduction.SUM;
            };
            case HISTOGRAM -> throw new IllegalArgumentException("histogram metrics are not emitted yet");
        };
    }

    private static List<String> mergeDimensions(List<String> global, List<String> metric) {
        Set<String> merged = new LinkedHashSet<>(global);
        merged.addAll(metric);
        return List.copyOf(merged);
    }

    /**
     * Whether any metric needs values read from the document's source. When false the write path can skip source parsing entirely.
     */
    public boolean needsSource() {
        return requiredPaths.isEmpty() == false;
    }
}
