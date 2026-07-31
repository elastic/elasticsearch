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
import org.elasticsearch.xcontent.XContentParserConfiguration;

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
    List<CompiledMetric> metrics,
    Set<String> requiredPaths,
    XContentParserConfiguration sourceFilter,
    int dimensionSets,
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
        RATE,
        /**
         * The distribution of the values rather than any single number. This is the one reduction that does not produce a double, so it
         * is accumulated and emitted separately from the rest.
         */
        HISTOGRAM;

        /** Whether this reduction produces a distribution rather than a single value, which changes both storage and emission. */
        public boolean isHistogram() {
            return this == HISTOGRAM;
        }
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
         *
         * @param segments the path split once, at compile time, because splitting it per document is a regular expression match per
         *                 document
         */
        record Field(String path, String[] segments) implements Source {
            static Field of(String path) {
                return new Field(path, splitPath(path));
            }
        }

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

    /**
     * A metric is accumulated at exactly one interval: its own override, or the stream's default. The interval determines which
     * destination data stream the metric is written to.
     *
     * @param dimensions     dimension names, in the order they are emitted
     * @param dimensionPaths the same names split once, at compile time, for reading out of {@code _source}
     * @param dimensionSet   which distinct dimension list this metric uses. Metrics that configure the same dimensions — the normal case,
     *                       since global dimensions apply to every metric — share a slot, so the write path resolves those values once per
     *                       document instead of once per metric. Reading a value out of {@code _source} is not free: it allocates a list
     *                       per path level and concatenates keys, so the multiplier matters.
     */
    public record CompiledMetric(
        String name,
        Trigger trigger,
        Reduction reduction,
        DerivedMetricsPredicate predicate,
        Source source,
        List<String> dimensions,
        String[][] dimensionPaths,
        int dimensionSet,
        Interval interval
    ) {
        public CompiledMetric(
            String name,
            Trigger trigger,
            Reduction reduction,
            DerivedMetricsPredicate predicate,
            Source source,
            List<String> dimensions,
            Interval interval
        ) {
            this(name, trigger, reduction, predicate, source, dimensions, splitPaths(dimensions), 0, interval);
        }
    }

    private static String[][] splitPaths(List<String> paths) {
        String[][] split = new String[paths.size()][];
        for (int i = 0; i < paths.size(); i++) {
            split[i] = splitPath(paths.get(i));
        }
        return split;
    }

    private static String[] splitPath(String path) {
        return path.split("\\.");
    }

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
        Interval defaultInterval = intervalOf(config.defaultInterval());

        Set<String> requiredPaths = new LinkedHashSet<>(config.dimensions());
        List<CompiledMetric> metrics = new ArrayList<>();
        for (String builtin : expandBuiltins(config.builtin())) {
            metrics.add(compileBuiltin(builtin, config.dimensions(), defaultInterval));
        }

        List<String> unsupported = new ArrayList<>();
        for (DataStreamDerivedMetrics.Metric metric : config.metrics()) {
            List<String> dimensions = mergeDimensions(config.dimensions(), metric.dimensions());
            requiredPaths.addAll(dimensions);
            DerivedMetricsPredicate.collectPaths(metric.when(), requiredPaths);
            Source source;
            if (metric.value().field() != null) {
                source = Source.Field.of(metric.value().field());
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
                    dimensions,
                    intervalOf(config.intervalOf(metric))
                )
            );
        }

        EnumSet<Trigger> triggers = EnumSet.noneOf(Trigger.class);
        for (CompiledMetric metric : metrics) {
            triggers.add(metric.trigger());
        }

        // Compiling the source filter here rather than per document is the point: withFiltering runs FilterPath.compile, which parses
        // every path. The paths are a property of the configuration, so this is done once per configuration change.
        Set<String> paths = Set.copyOf(requiredPaths);
        XContentParserConfiguration sourceFilter = paths.isEmpty()
            ? XContentParserConfiguration.EMPTY
            : XContentParserConfiguration.EMPTY.withFiltering(null, paths, null, true);

        List<List<String>> dimensionSets = new ArrayList<>();
        List<CompiledMetric> assigned = new ArrayList<>(metrics.size());
        for (CompiledMetric metric : metrics) {
            int set = dimensionSets.indexOf(metric.dimensions());
            if (set < 0) {
                set = dimensionSets.size();
                dimensionSets.add(metric.dimensions());
            }
            assigned.add(
                new CompiledMetric(
                    metric.name(),
                    metric.trigger(),
                    metric.reduction(),
                    metric.predicate(),
                    metric.source(),
                    metric.dimensions(),
                    metric.dimensionPaths(),
                    set,
                    metric.interval()
                )
            );
        }

        return new CompiledDerivedMetrics(
            List.copyOf(assigned),
            paths,
            sourceFilter,
            dimensionSets.size(),
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

    private static Interval intervalOf(TimeValue interval) {
        return new Interval(interval.getStringRep(), interval.millis());
    }

    private static CompiledMetric compileBuiltin(String name, List<String> dimensions, Interval interval) {
        return switch (name) {
            case INGEST_DOCS_COUNT -> builtin(name, Trigger.SUCCESS, Reduction.SUM, new Source.Constant(1.0), dimensions, interval);
            case INGEST_DOCS_RATE -> builtin(name, Trigger.SUCCESS, Reduction.RATE, new Source.Constant(1.0), dimensions, interval);
            case INGEST_BYTES_COUNT -> builtin(name, Trigger.SUCCESS, Reduction.SUM, new Source.DocumentSize(), dimensions, interval);
            case INGEST_BYTES_RATE -> builtin(name, Trigger.SUCCESS, Reduction.RATE, new Source.DocumentSize(), dimensions, interval);
            case INGEST_FAILURES_COUNT -> builtin(name, Trigger.FAILURE, Reduction.SUM, new Source.Constant(1.0), dimensions, interval);
            case INGEST_FAILURES_RATE -> builtin(name, Trigger.FAILURE, Reduction.RATE, new Source.Constant(1.0), dimensions, interval);
            default -> throw new IllegalArgumentException("unsupported derived metrics builtin [" + name + "]");
        };
    }

    private static CompiledMetric builtin(
        String name,
        Trigger trigger,
        Reduction reduction,
        Source source,
        List<String> dimensions,
        Interval interval
    ) {
        return new CompiledMetric(name, trigger, reduction, DerivedMetricsPredicate.MATCH_ALL, source, List.copyOf(dimensions), interval);
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
            case HISTOGRAM -> Reduction.HISTOGRAM;
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
