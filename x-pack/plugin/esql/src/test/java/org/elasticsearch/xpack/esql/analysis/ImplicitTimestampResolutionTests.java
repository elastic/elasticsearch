/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static java.util.Map.entry;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Checks which column the implicit {@code @timestamp} argument of an aggregation resolves to. It must be the
 * {@code @timestamp} column of the {@code STATS} input, also when a grouping of the same {@code STATS} is named
 * {@code @timestamp}. Otherwise, for example, {@code rate()} reads the time bucket as its sample timestamp and returns
 * wrong values that change between runs.
 */
public class ImplicitTimestampResolutionTests extends AnalyzerTestCase {

    /**
     * Time-series aggregations whose per-time-series aggregation reads the implicit {@code @timestamp}, keyed by
     * function name.
     */
    private static final Map<String, String> TIMESTAMP_READING_TIME_SERIES_AGGREGATIONS = Map.ofEntries(
        entry("rate", "rate(network.total_bytes_in)"),
        entry("irate", "irate(network.total_bytes_in)"),
        entry("increase", "increase(network.total_bytes_in)"),
        entry("delta", "delta(network.cost)"),
        entry("idelta", "idelta(network.cost)"),
        entry("deriv", "deriv(network.cost)"),
        entry("changes", "changes(network.cost)"),
        entry("first_over_time", "first_over_time(network.cost)"),
        entry("last_over_time", "last_over_time(network.cost)")
    );

    /**
     * Time-series aggregations that take an implicit {@code @timestamp} but drop it when they are translated to their
     * per-time-series aggregation, so the column it resolves to does not change their results.
     */
    private static final Map<String, String> TIMESTAMP_DROPPING_TIME_SERIES_AGGREGATIONS = Map.ofEntries(
        entry("max_over_time", "max_over_time(network.cost)"),
        entry("min_over_time", "min_over_time(network.cost)"),
        entry("sum_over_time", "sum_over_time(network.cost)"),
        entry("count_over_time", "count_over_time(network.cost)"),
        entry("avg_over_time", "avg_over_time(network.cost)"),
        entry("percentile_over_time", "percentile_over_time(network.cost, 50)")
    );

    /** Aggregations outside {@code TS} that take an implicit {@code @timestamp}, keyed by function name. */
    private static final Map<String, String> REGULAR_AGGREGATIONS = Map.ofEntries(
        entry("earliest", "earliest(message)"),
        entry("latest", "latest(message)")
    );

    public ImplicitTimestampResolutionTests(VersionMode versionMode) {
        super(versionMode);
    }

    /**
     * Fails when an aggregation with an implicit {@code @timestamp} is added without being added to one of the lists
     * the other tests use.
     */
    public void testEveryAggregationWithImplicitTimestampIsCovered() {
        Set<String> registered = new EsqlFunctionRegistry().snapshotRegistry()
            .listFunctions()
            .stream()
            .filter(def -> AggregateFunction.class.isAssignableFrom(def.clazz()) && TimestampAware.class.isAssignableFrom(def.clazz()))
            .map(FunctionDefinition::name)
            .collect(Collectors.toSet());
        Set<String> covered = new HashSet<>();
        covered.addAll(TIMESTAMP_READING_TIME_SERIES_AGGREGATIONS.keySet());
        covered.addAll(TIMESTAMP_DROPPING_TIME_SERIES_AGGREGATIONS.keySet());
        covered.addAll(REGULAR_AGGREGATIONS.keySet());

        assertThat(covered, equalTo(registered));
    }

    /**
     * Fails when one of these aggregations starts to read the implicit {@code @timestamp}. It then belongs to
     * {@link #TIMESTAMP_READING_TIME_SERIES_AGGREGATIONS}.
     */
    public void testTimestampDroppingTimeSeriesAggregationsReadNoTimestamp() {
        Map<String, List<Expression>> timestampsByQuery = new TreeMap<>();
        for (String aggregation : TIMESTAMP_DROPPING_TIME_SERIES_AGGREGATIONS.values()) {
            String query = "TS k8s | STATS v = " + aggregation + " BY bucket = TBUCKET(1 minute) | LIMIT 10";
            List<Expression> timestamps = implicitTimestamps(k8s().query(query));
            if (timestamps.isEmpty() == false) {
                timestampsByQuery.put(query, timestamps);
            }
        }

        assertThat(timestampsByQuery, equalTo(Map.of()));
    }

    public void testTimeSeriesAggregationsReadSourceTimestampWhenTimeBucketHasOtherName() {
        assertImplicitTimestampsReadSourceTimestamp(
            k8s(),
            TIMESTAMP_READING_TIME_SERIES_AGGREGATIONS.values()
                .stream()
                .map(aggregation -> "TS k8s | STATS v = " + aggregation + " BY bucket = TBUCKET(1 minute) | LIMIT 10")
                .toList()
        );
    }

    public void testBareTimeSeriesAggregationsIgnoreTimeBucketNamedTimestamp() {
        assertImplicitTimestampsReadSourceTimestamp(
            k8s(),
            TIMESTAMP_READING_TIME_SERIES_AGGREGATIONS.values()
                .stream()
                .map(aggregation -> "TS k8s | STATS v = " + aggregation + " BY @timestamp = TBUCKET(1 minute) | LIMIT 10")
                .toList()
        );
    }

    public void testNestedTimeSeriesAggregationsIgnoreTimeBucketNamedTimestamp() {
        assertImplicitTimestampsReadSourceTimestamp(
            k8s(),
            TIMESTAMP_READING_TIME_SERIES_AGGREGATIONS.values()
                .stream()
                .map(aggregation -> "TS k8s | STATS v = max(" + aggregation + ") BY cluster, @timestamp = TBUCKET(1 minute) | LIMIT 10")
                .toList()
        );
    }

    /**
     * The time bucket function does not matter: any grouping named {@code @timestamp} shadows the field. The nested form
     * is used because {@code DATE_TRUNC} is not accepted as a grouping of a bare time-series aggregation.
     */
    public void testRateIgnoresTimeBucketNamedTimestampForEveryTimeBucketFunction() {
        List<String> timeBuckets = new ArrayList<>(
            List.of("TBUCKET(1 minute)", "BUCKET(@timestamp, 1 minute)", "DATE_TRUNC(1 minute, @timestamp)")
        );
        if (EsqlCapabilities.Cap.TSTEP.isEnabled()) {
            timeBuckets.add("TSTEP(1 minute)");
        }
        var bounds = new QueryDslTimestampBoundsExtractor.TimestampBounds(
            Instant.parse("2024-05-10T00:00:00Z"),
            Instant.parse("2024-05-10T01:00:00Z")
        );

        assertImplicitTimestampsReadSourceTimestamp(
            k8s().timestampBounds(bounds),
            timeBuckets.stream()
                .map(timeBucket -> "TS k8s | STATS v = max(rate(network.total_bytes_in)) BY @timestamp = " + timeBucket + " | LIMIT 10")
                .toList()
        );
    }

    /**
     * A grouping that is not a date must not be read as the timestamp either. {@code rate()} would read keywords as sample
     * timestamps, and aggregations that check the type of their timestamp would fail verification.
     */
    public void testTimeSeriesAggregationsIgnoreNonTemporalGroupingNamedTimestamp() {
        assertImplicitTimestampsReadSourceTimestamp(
            k8s(),
            TIMESTAMP_READING_TIME_SERIES_AGGREGATIONS.values()
                .stream()
                .map(aggregation -> "TS k8s | STATS v = max(" + aggregation + ") BY @timestamp = cluster | LIMIT 10")
                .toList()
        );
    }

    public void testEarliestAndLatestReadSourceTimestampWhenTimeBucketHasOtherName() {
        assertImplicitTimestampsReadSourceTimestamp(
            sampleData(),
            REGULAR_AGGREGATIONS.values()
                .stream()
                .map(aggregation -> "FROM sample_data | STATS v = " + aggregation + " BY bucket = BUCKET(@timestamp, 1 hour) | LIMIT 10")
                .toList()
        );
    }

    public void testEarliestAndLatestIgnoreTimeBucketNamedTimestamp() {
        assertImplicitTimestampsReadSourceTimestamp(
            sampleData(),
            REGULAR_AGGREGATIONS.values()
                .stream()
                .map(
                    aggregation -> "FROM sample_data | STATS v = " + aggregation + " BY @timestamp = BUCKET(@timestamp, 1 hour) | LIMIT 10"
                )
                .toList()
        );
    }

    /** Reading the {@code ip} grouping as the timestamp would fail verification. */
    public void testEarliestAndLatestIgnoreNonTemporalGroupingNamedTimestamp() {
        assertImplicitTimestampsReadSourceTimestamp(
            sampleData(),
            REGULAR_AGGREGATIONS.values()
                .stream()
                .map(aggregation -> "FROM sample_data | STATS v = " + aggregation + " BY @timestamp = client_ip | LIMIT 10")
                .toList()
        );
    }

    public void testEarliestAndLatestIgnoreTimeBucketNamedTimestampInInlineStats() {
        assertImplicitTimestampsReadSourceTimestamp(
            sampleData(),
            REGULAR_AGGREGATIONS.values()
                .stream()
                .map(
                    aggregation -> "FROM sample_data | INLINE STATS v = "
                        + aggregation
                        + " BY @timestamp = BUCKET(@timestamp, 1 hour) | LIMIT 10"
                )
                .toList()
        );
    }

    /** A grouping named {@code @timestamp} does not stand in for a {@code @timestamp} column the input no longer has. */
    public void testLatestFailsWhenOnlyGroupingIsNamedTimestamp() {
        sampleData().error(
            "FROM sample_data | RENAME @timestamp AS ts | STATS v = latest(message) BY @timestamp = BUCKET(ts, 1 hour) | LIMIT 10",
            containsString(
                "[latest(message)] requires the [@timestamp] field, which was either not present in the source index, "
                    + "or has been dropped or renamed"
            )
        );
    }

    /**
     * Analyzes every query and reports, in one failure, each query that fails verification or has an aggregation whose
     * implicit {@code @timestamp} is not the {@code @timestamp} field of the source.
     */
    private static void assertImplicitTimestampsReadSourceTimestamp(TestAnalyzer analyzer, List<String> queries) {
        Map<String, String> problemsByQuery = new TreeMap<>();
        for (String query : queries) {
            String problem = implicitTimestampProblem(analyzer, query);
            if (problem != null) {
                problemsByQuery.put(query, problem);
            }
        }

        assertThat(problemsByQuery, equalTo(Map.of()));
    }

    /** Describes what is wrong with the implicit timestamps of {@code query}, or returns {@code null} if nothing is. */
    private static String implicitTimestampProblem(TestAnalyzer analyzer, String query) {
        LogicalPlan plan;
        try {
            plan = analyzer.query(query);
        } catch (VerificationException e) {
            return "fails verification: " + e.getMessage();
        }
        List<Expression> timestamps = implicitTimestamps(plan);
        if (timestamps.isEmpty()) {
            return "has no aggregation with an implicit timestamp";
        }
        Attribute sourceTimestamp = sourceTimestamp(plan);
        List<Expression> otherTimestamps = timestamps.stream().filter(timestamp -> refersTo(timestamp, sourceTimestamp) == false).toList();
        return otherTimestamps.isEmpty() ? null : "reads " + otherTimestamps + " instead of " + sourceTimestamp;
    }

    private static boolean refersTo(Expression expression, Attribute attribute) {
        return expression instanceof Attribute other && other.id().equals(attribute.id());
    }

    private static List<Expression> implicitTimestamps(LogicalPlan plan) {
        List<Expression> timestamps = new ArrayList<>();
        plan.forEachDown(Aggregate.class, aggregate -> {
            for (NamedExpression aggregateExpression : aggregate.aggregates()) {
                aggregateExpression.forEachDown(AggregateFunction.class, function -> {
                    if (function instanceof TimestampAware timestampAware) {
                        timestamps.add(timestampAware.timestamp());
                    }
                });
            }
        });
        return timestamps;
    }

    private static Attribute sourceTimestamp(LogicalPlan plan) {
        List<Attribute> sourceTimestamps = new ArrayList<>();
        plan.forEachDown(EsRelation.class, relation -> {
            for (Attribute attribute : relation.output()) {
                if (attribute.name().equals(MetadataAttribute.TIMESTAMP_FIELD)) {
                    sourceTimestamps.add(attribute);
                }
            }
        });
        assertThat("expected one source @timestamp in " + plan, sourceTimestamps.size(), equalTo(1));
        return sourceTimestamps.getFirst();
    }

    private TestAnalyzer k8s() {
        return analyzer().addK8s();
    }

    private TestAnalyzer sampleData() {
        return analyzer().addSampleData();
    }
}
