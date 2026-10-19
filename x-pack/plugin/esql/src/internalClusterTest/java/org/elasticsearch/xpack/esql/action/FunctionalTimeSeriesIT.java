/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongMapperPlugin;
import org.junit.BeforeClass;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/** Executes selector and aggregation metadata plans across data nodes, including named and derived label boundaries. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class FunctionalTimeSeriesIT extends AbstractEsqlIntegTestCase {
    private static final String INDEX = "metrics-functional";
    private static final String QUERY_TIME = "2024-05-10T00:01:00Z";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), UnsignedLongMapperPlugin.class);
    }

    @BeforeClass
    public static void checkCapability() {
        assumeTrue("requires functional time-series metadata", EsqlCapabilities.Cap.PROMQL_ENABLE_TIMESERIES_TYPE.isEnabled());
    }

    @Override
    protected QueryPragmas getPragmas() {
        // Small pages exercise metadata ownership and aggregation across page boundaries.
        return canUseQueryPragmas() ? new QueryPragmas(Settings.builder().put("page_size", 1).build()) : super.getPragmas();
    }

    public void testMissingSourceIndicesPreserveEmptyVectors() {
        String missing = "metrics-nonexistent-*";
        for (String expression : List.of("tx", "sum without(host)(tx)", "sum by(host)(sum without(host)(tx))")) {
            assertRows(query(missing, expression, QUERY_TIME) + " | KEEP result", List.of());
        }
    }

    public void testRawLabels() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        refresh(INDEX);

        assertRows(
            query("tx") + " | KEEP result, _timeseries",
            List.of(List.of(10.0, "{\"cluster\":\"prod\",\"host\":\"a\"}"), List.of(20.0, "{\"cluster\":\"prod\",\"host\":\"b\"}"))
        );
    }

    /** Multivalued dimensions remain complete label values when the selector renders its metadata. */
    public void testMultivaluedDimensionsInSelectorMetadata() {
        indexMultivaluedAggregationGroups();

        assertRows(
            query("tx") + " | KEEP result, _timeseries",
            List.of(
                List.of(10.0, "{\"cluster\":\"prod\",\"host\":\"a\",\"region\":[\"east\",\"west\"],\"shard\":[\"1\",\"2\"]}"),
                List.of(20.0, "{\"cluster\":\"prod\",\"host\":\"b\",\"region\":[\"east\",\"west\"],\"shard\":[\"1\",\"2\"]}"),
                List.of(5.0, "{\"cluster\":\"prod\",\"host\":\"c\",\"region\":\"west\",\"shard\":\"2\"}"),
                List.of(7.0, "{\"cluster\":\"dev\",\"host\":\"d\",\"region\":[\"east\",\"west\"],\"shard\":[\"1\",\"2\"]}")
            )
        );
    }

    /** Excluding a multivalued dimension removes its entire value before the current aggregation groups series. */
    public void testWithoutExcludesMultivaluedDimensions() {
        indexMultivaluedAggregationGroups();

        assertRows(
            query("sum without(host,region,shard)(tx)") + " | KEEP result, _timeseries",
            List.of(List.of(35.0, "{\"cluster\":\"prod\"}"), List.of(7.0, "{\"cluster\":\"dev\"}"))
        );
        assertRows(
            query("max without(region,shard)(sum without(host)(tx))") + " | KEEP result, _timeseries",
            List.of(List.of(30.0, "{\"cluster\":\"prod\"}"), List.of(7.0, "{\"cluster\":\"dev\"}"))
        );
        assertRows(
            query("sum by(region,shard)(sum without(host,region,shard)(tx))") + " | KEEP result, region, shard",
            List.of(Arrays.asList(42.0, null, null))
        );
    }

    /** Carrying multivalued labels through named and metadata groupings must not multiply the samples. */
    public void testMultivaluedDimensionsDoNotDuplicateNestedTotals() {
        indexMultivaluedAggregationGroups();

        for (String expression : List.of(
            "sum by(cluster)(sum by(cluster,region,shard,host)(tx))",
            "sum by(cluster)(sum without(host)(sum by(cluster,region,shard,host)(tx)))",
            "sum by(cluster)(sum without(host)(tx))",
            "sum without(region,shard)(sum by(cluster,region,shard)(tx))"
        )) {
            assertRows(query(expression) + " | KEEP result, cluster", List.of(List.of(35.0, "prod"), List.of(7.0, "dev")));
        }
        assertRows(
            query("count by(cluster)(sum without(host)(tx))") + " | KEEP result, cluster",
            List.of(List.of(2.0, "prod"), List.of(1.0, "dev"))
        );
    }

    /** BY over WITHOUT restores all values from metadata, including the native type of numeric dimensions. */
    public void testByOverWithoutRestoresMultivaluedDimensions() {
        indexMultivaluedAggregationGroups();

        for (String expression : List.of("sum by(region,shard)(tx)", "sum by(region,shard)(sum without(host)(tx))")) {
            try (EsqlQueryResponse response = run(query(expression) + " | KEEP result, region, shard")) {
                assertOk(response);
                assertEquals(
                    List.of("double", "keyword", "long"),
                    response.columns().stream().map(column -> column.type().typeName()).toList()
                );
                assertThat(
                    EsqlTestUtils.getValuesList(response),
                    containsInAnyOrder(List.of(37.0, List.of("east", "west"), List.of(1L, 2L)), List.of(5.0, "west", 2L))
                );
            }
        }
    }

    private void indexMultivaluedAggregationGroups() {
        createMetricsIndex(INDEX, Settings.builder().put("index.mapping.source.mode", "stored").build());
        index(INDEX, "cluster", "prod", "host", "a", "region", List.of("east", "west"), "shard", List.of(1L, 2L), "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "region", List.of("west", "east"), "shard", List.of(2L, 1L), "tx", 20.0);
        index(INDEX, "cluster", "prod", "host", "c", "region", "west", "shard", 2L, "tx", 5.0);
        index(INDEX, "cluster", "dev", "host", "d", "region", List.of("east", "west"), "shard", List.of(1L, 2L), "tx", 7.0);
        refresh(INDEX);
    }

    /** Each WITHOUT aggregates at its own label grain, even when its parent requests fewer labels. */
    public void testWithoutPreservesAggregationGrain() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        index(INDEX, "cluster", "dev", "host", "c", "tx", 5.0);
        refresh(INDEX);

        assertRows(query("sum without(cluster)(max without(host)(tx))") + " | KEEP result, _timeseries", List.of(List.of(25.0, "{}")));
        assertRows(
            query("avg by(cluster)(sum without()(tx))") + " | KEEP result, cluster",
            List.of(List.of(15.0, "prod"), List.of(5.0, "dev"))
        );
        assertRows(
            query("max without(host)(sum by(cluster,host)(tx))") + " | KEEP result, cluster",
            List.of(List.of(20.0, "prod"), List.of(5.0, "dev"))
        );
    }

    /** Disjoint, repeated, and overlapping exclusions must act on the immediately preceding aggregation's label set. */
    public void testNestedWithoutExclusionsApplyAtEachLevel() {
        indexUnevenAggregationGroups();

        assertRows(
            query("max without(region)(sum without(host)(tx))") + " | KEEP result, _timeseries",
            List.of(List.of(40.0, "{\"cluster\":\"prod\"}"), List.of(30.0, "{\"cluster\":\"dev\"}"))
        );
        assertRows(
            query("max without(host)(sum without(host)(tx))") + " | KEEP result, _timeseries",
            List.of(
                List.of(40.0, "{\"cluster\":\"prod\",\"region\":\"east\"}"),
                List.of(5.0, "{\"cluster\":\"prod\",\"region\":\"west\"}"),
                List.of(9.0, "{\"cluster\":\"prod\",\"region\":\"south\"}"),
                List.of(7.0, "{\"cluster\":\"dev\",\"region\":\"east\"}"),
                List.of(30.0, "{\"cluster\":\"dev\",\"region\":\"west\"}")
            )
        );
        assertRows(
            query("avg without(region,cluster)(sum without(host,region)(tx))") + " | KEEP result, _timeseries",
            List.of(List.of(45.5, "{}"))
        );
    }

    /** Empty WITHOUT retains all current labels, whether it occurs before or after another aggregation. */
    public void testNestedEmptyWithoutRetainsSeriesGrain() {
        indexUnevenAggregationGroups();

        assertRows(
            query("avg without(host)(sum without()(tx))") + " | KEEP result, _timeseries",
            List.of(
                List.of(20.0, "{\"cluster\":\"prod\",\"region\":\"east\"}"),
                List.of(5.0, "{\"cluster\":\"prod\",\"region\":\"west\"}"),
                List.of(9.0, "{\"cluster\":\"prod\",\"region\":\"south\"}"),
                List.of(7.0, "{\"cluster\":\"dev\",\"region\":\"east\"}"),
                List.of(15.0, "{\"cluster\":\"dev\",\"region\":\"west\"}")
            )
        );
        assertRows(
            query("sum without()(max without(host)(tx))") + " | KEEP result, _timeseries",
            List.of(
                List.of(30.0, "{\"cluster\":\"prod\",\"region\":\"east\"}"),
                List.of(5.0, "{\"cluster\":\"prod\",\"region\":\"west\"}"),
                List.of(9.0, "{\"cluster\":\"prod\",\"region\":\"south\"}"),
                List.of(7.0, "{\"cluster\":\"dev\",\"region\":\"east\"}"),
                List.of(17.0, "{\"cluster\":\"dev\",\"region\":\"west\"}")
            )
        );
    }

    /** Uneven groups make counts and averages of intermediate rows differ from aggregating the original samples. */
    public void testNestedWithoutPreservesNonlinearAggregationOrder() {
        indexUnevenAggregationGroups();

        assertRows(
            query("avg without(region)(sum without(host)(tx))") + " | KEEP result, _timeseries",
            List.of(List.of(18.0, "{\"cluster\":\"prod\"}"), List.of(18.5, "{\"cluster\":\"dev\"}"))
        );
        assertRows(
            query("count without(region)(max without(host)(tx))") + " | KEEP result, _timeseries",
            List.of(List.of(3.0, "{\"cluster\":\"prod\"}"), List.of(2.0, "{\"cluster\":\"dev\"}"))
        );
        assertRows(
            query("avg without(cluster)(sum without(region)(max without(host)(tx)))") + " | KEEP result, _timeseries",
            List.of(List.of(34.0, "{}"))
        );
    }

    /** Count consumes series at each aggregation boundary, including zero-valued samples and named or metadata groupings. */
    public void testCountPreservesAggregationBoundaries() {
        createMetricsIndex(INDEX);
        client().prepareIndex(INDEX).setSource("@timestamp", "2024-05-10T00:00:00Z", "cluster", "prod", "host", "a", "tx", 10.0).get();
        index(INDEX, "cluster", "prod", "host", "a", "tx", 0.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        index(INDEX, "cluster", "dev", "host", "c", "tx", 5.0);
        index(INDEX, "cluster", "dev", "host", "missing");
        refresh(INDEX);

        assertRows(query("count(tx)") + " | KEEP result", List.of(List.of(3.0)));
        for (String expression : List.of(
            "count by(cluster)(tx)",
            "count by(cluster)(count by(cluster,host)(tx))",
            "count without(host)(count by(cluster,host)(tx))"
        )) {
            assertRows(query(expression) + " | KEEP result, cluster", List.of(List.of(2.0, "prod"), List.of(1.0, "dev")));
        }
        for (String expression : List.of("count without(host)(tx)", "count without(host)(max without()(tx))")) {
            assertRows(
                query(expression) + " | KEEP result, _timeseries",
                List.of(List.of(2.0, "{\"cluster\":\"prod\"}"), List.of(1.0, "{\"cluster\":\"dev\"}"))
            );
        }
        assertRows(query("sum(count by(cluster)(tx))") + " | KEEP result", List.of(List.of(3.0)));
        assertRows(query("count(count by(cluster)(tx))") + " | KEEP result", List.of(List.of(2.0)));
    }

    /** A range with one sample gives rate no value; count must not turn that absent element into zero for a parent. */
    public void testCountDoesNotInventElementsForEmptyGroups() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "missing");
        refresh(INDEX);

        for (String expression : List.of(
            "count(tx{host=\"missing\"})",
            "count(rate(tx[1m]))",
            "count by(cluster)(rate(tx[1m]))",
            "count without(host)(rate(tx[1m]))",
            "count(count by(cluster)(rate(tx[1m])))",
            "sum(count without(host)(rate(tx[1m])))"
        )) {
            assertRows(query(expression) + " | KEEP result", List.of());
        }
        // Ordinary ES|QL COUNT retains its zero result for an empty input.
        assertRows("FROM " + INDEX + " | WHERE host == \"missing\" | STATS result = COUNT(tx)", List.of(List.of(0L)));
    }

    /** A changing winning cluster distinguishes per-series range averages from averaging cross-series maxima. */
    public void testNestedWithoutFollowsWithinSeriesAggregation() {
        createMetricsIndex(INDEX);
        client().prepareIndex(INDEX).setSource("@timestamp", "2024-05-10T00:00:00Z", "cluster", "prod", "host", "a", "tx", 10.0).get();
        client().prepareIndex(INDEX).setSource("@timestamp", "2024-05-10T00:00:00Z", "cluster", "prod", "host", "b", "tx", 30.0).get();
        client().prepareIndex(INDEX).setSource("@timestamp", "2024-05-10T00:00:00Z", "cluster", "dev", "host", "c", "tx", 4.0).get();
        index(INDEX, "cluster", "prod", "host", "a", "tx", 2.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 10.0);
        index(INDEX, "cluster", "dev", "host", "c", "tx", 60.0);
        refresh(INDEX);

        // Per-series averages are 6, 20, and 32. The cluster sums are 26 and 32, so the final maximum is 32.
        // Taking the cluster maximum at each timestamp first would instead average 40 and 60 to produce 50.
        assertRows(
            query("sum without(host)(avg_over_time(tx[5m]))") + " | KEEP result, _timeseries",
            List.of(List.of(26.0, "{\"cluster\":\"prod\"}"), List.of(32.0, "{\"cluster\":\"dev\"}"))
        );
        assertRows(
            query("max without(cluster)(sum without(host)(avg_over_time(tx[5m])))") + " | KEEP result, _timeseries",
            List.of(List.of(32.0, "{}"))
        );
    }

    /** Alternating BY and WITHOUT must use only the labels and grouped values produced by each child. */
    public void testByWithoutBoundaryChainsPreserveGrouping() {
        indexUnevenAggregationGroups();

        assertRows(query("max without(cluster)(avg by(cluster)(sum without(host)(tx)))") + " | KEEP result", List.of(List.of(18.5)));
        assertRows(
            query("sum by(cluster)(max without(host)(sum by(cluster,region,host)(tx)))") + " | KEEP result, cluster",
            List.of(List.of(44.0, "prod"), List.of(24.0, "dev"))
        );
        assertRows(query("sum by(host)(sum without(host)(tx))") + " | KEEP result, host", List.of(Arrays.asList(91.0, null)));
    }

    private void indexUnevenAggregationGroups() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "region", "east", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "region", "east", "host", "b", "tx", 30.0);
        index(INDEX, "cluster", "prod", "region", "west", "host", "c", "tx", 5.0);
        index(INDEX, "cluster", "prod", "region", "south", "host", "d", "tx", 9.0);
        index(INDEX, "cluster", "dev", "region", "east", "host", "e", "tx", 7.0);
        index(INDEX, "cluster", "dev", "region", "west", "host", "f", "tx", 13.0);
        index(INDEX, "cluster", "dev", "region", "west", "host", "g", "tx", 17.0);
        refresh(INDEX);
    }

    /** WITHOUT removes the metric name from metadata and named child outputs. */
    public void testWithoutRemovesMetricName() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", LabelMatcher.NAME, "left", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "a", LabelMatcher.NAME, "right", "tx", 20.0);
        refresh(INDEX);

        assertRows(query("sum without(host)(tx)") + " | KEEP result, _timeseries", List.of(List.of(30.0, "{\"cluster\":\"prod\"}")));
        assertRows(
            query("sum without(host)(sum by(cluster,host," + LabelMatcher.NAME + ")(tx))") + " | KEEP result, cluster",
            List.of(List.of(30.0, "prod"))
        );
        assertRows(
            query("sum by(" + LabelMatcher.NAME + ")(sum without(host)(sum by(cluster,host," + LabelMatcher.NAME + ")(tx)))")
                + " | KEEP result, "
                + LabelMatcher.NAME,
            List.of(Arrays.asList(30.0, null))
        );
    }

    public void testNumericAggregationCombinesEmptyAndAbsentLabels() {
        for (double secondValue : List.of(7.0, 9.0)) {
            String indexName = "metrics-collision-" + (int) secondValue;
            createMetricsIndex(indexName);
            index(indexName, "cluster", "prod", "host", "a", "optional", "", "tx", 7.0);
            index(indexName, "cluster", "prod", "host", "a", "tx", secondValue);
            refresh(indexName);

            assertRows("FROM " + indexName + " | STATS count = COUNT(*)", List.of(List.of(2L)));
            String metadata = "{\"cluster\":\"prod\",\"host\":\"a\"}";
            try (EsqlQueryResponse response = run(query(indexName, "tx", QUERY_TIME) + " | KEEP result, _timeseries")) {
                assertOk(response);
                assertEquals(List.of("result", "_timeseries"), response.columns().stream().map(column -> column.name()).toList());
                // Rendering an empty dimension as absent must not merge distinct source series, even for equal values.
                assertThat(
                    EsqlTestUtils.getValuesList(response),
                    containsInAnyOrder(List.of(7.0, metadata), List.of(secondValue, metadata))
                );
            }
            assertRows(
                query(indexName, "sum without(optional)(tx)", QUERY_TIME) + " | KEEP result, _timeseries",
                List.of(List.of(7.0 + secondValue, metadata))
            );
            assertRows(
                query(indexName, "sum by(cluster) (tx)", QUERY_TIME) + " | KEEP result, cluster",
                List.of(List.of(7.0 + secondValue, "prod"))
            );
        }
    }

    /** Reject result names that collide with labels/metadata before the plan consistency checker. */
    public void testResultNameMustNotShadowLabelsOrMetadata() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 7.0);
        refresh(INDEX);
        for (String query : List.of(
            query("sum by(cluster)(tx)").replace(" result=", " cluster="),
            query("tx").replace(" result=", " _timeseries=")
        )) {
            var error = expectThrows(VerificationException.class, () -> run(query));
            assertThat(error.getMessage(), containsString("result column"));
        }
    }

    public void testPromotedLabelRetainsSelectorFilter() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        refresh(INDEX);

        assertRows(query("sum by(host) (tx{host=\"a\"})") + " | KEEP result, host", List.of(List.of(10.0, "a")));
    }

    /** Matchers select source series before either WITHOUT removes the matching label or merges its groups. */
    public void testRemovedLabelRetainsSelectorFilter() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 90.0);
        index(INDEX, "cluster", "dev", "host", "c", "tx", 30.0);
        refresh(INDEX);

        assertRows(
            query("sum without(host)(tx{host=\"a\"})") + " | KEEP result, _timeseries",
            List.of(List.of(10.0, "{\"cluster\":\"prod\"}"))
        );
        assertRows(
            query("max without(cluster)(sum without(host)(tx{host!=\"b\"}))") + " | KEEP result, _timeseries",
            List.of(List.of(30.0, "{}"))
        );
    }

    /** A selected source document without the metric is not a selected metric series or a synthetic absent sample. */
    public void testRangeFunctionsDoNotInventMissingMetricSamples() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b");
        refresh(INDEX);

        for (String function : List.of("absent_over_time", "count_over_time", "present_over_time")) {
            assertRows(query("max without(host)(" + function + "(tx{host=\"b\"}[1m]))") + " | KEEP result", List.of());
        }
        assertRows(query("max without(host)(absent_over_time(tx{host=\"a\"}[1m]))") + " | KEEP result", List.of());
        assertRows(
            query("sum without(host)(count_over_time(tx{host=\"a\"}[1m]))") + " | KEEP result, _timeseries",
            List.of(List.of(1.0, "{\"cluster\":\"prod\"}"))
        );
    }

    /** Offset clocks shift samples once, while explicit source bounds exclude samples that open bounds retain. */
    public void testOffsetRangeAggregationWithDateAndDateNanos() {
        for (String timestampType : List.of("date", "date_nanos")) {
            String indexName = "metrics-clock-" + timestampType;
            createMetricsIndex(indexName, Settings.EMPTY, timestampType);
            for (int minute = 0; minute < 3; minute++) {
                double value = List.of(7.0, 13.0, 100.0).get(minute);
                client().prepareIndex(indexName)
                    .setSource("@timestamp", "2024-05-10T00:0" + minute + ":00Z", "cluster", "prod", "host", "a", "tx", value)
                    .get();
            }
            index(indexName, "cluster", "prod", "host", "b", "tx", 1000.0);
            refresh(indexName);

            String expression = "sum without(host)(sum_over_time(tx{host=\"a\"}[1m] offset 1m))";
            String columns = " | EVAL millis = TO_LONG(step) | KEEP result, millis, _timeseries";
            long firstStep = Instant.parse(QUERY_TIME).toEpochMilli();
            List<List<Object>> bounded = List.of(
                List.of(7.0, firstStep, "{\"cluster\":\"prod\"}"),
                List.of(13.0, firstStep + 60_000, "{\"cluster\":\"prod\"}")
            );
            assertRows(query(indexName, expression, "2024-05-10T00:02:00Z") + columns, bounded);
            assertRows(
                "PROMQL index=" + indexName + " step=1m result=(" + expression + ")" + columns,
                List.of(bounded.get(0), bounded.get(1), List.of(100.0, firstStep + 120_000, "{\"cluster\":\"prod\"}"))
            );
        }
    }

    public void testWithinSeriesFunctionsDropMetricNameAfterEvaluatingEachSeries() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", LabelMatcher.NAME, "metric", "tx", 10.0);
        refresh(INDEX);

        assertRows(
            query("sum_over_time(tx[1m])") + " | KEEP result, _timeseries",
            List.of(List.of(10.0, "{\"cluster\":\"prod\",\"host\":\"a\"}"))
        );
        assertRows(
            query("last_over_time(tx[1m])") + " | KEEP result, _timeseries",
            List.of(List.of(10.0, "{\"" + LabelMatcher.NAME + "\":\"metric\",\"cluster\":\"prod\",\"host\":\"a\"}"))
        );

        // A second series that still differs after __name__ drops (same labels would collapse under typed rest).
        index(INDEX, "cluster", "prod", "host", "b", LabelMatcher.NAME, "other", "tx", 20.0);
        refresh(INDEX);
        assertRows(
            query("sum_over_time(tx[1m])") + " | KEEP result, _timeseries",
            List.of(List.of(10.0, "{\"cluster\":\"prod\",\"host\":\"a\"}"), List.of(20.0, "{\"cluster\":\"prod\",\"host\":\"b\"}"))
        );
    }

    public void testKeywordNumericSourceTokensMatchIndexedLabelIdentity() {
        createMetricsIndex(INDEX, Settings.builder().put("index.mapping.source.mode", "stored").build());
        client().prepareIndex(INDEX).setSource("""
            {"@timestamp":"2024-05-10T00:01:00Z","cluster":"prod","host":1e3,"tx":7.0}
            """, XContentType.JSON).get();
        client().prepareIndex(INDEX).setSource("""
            {"@timestamp":"2024-05-10T00:01:00Z","cluster":"prod","host":1000.0,"tx":9.0}
            """, XContentType.JSON).get();
        refresh(INDEX);

        // Keyword indexing retains numeric token spelling; residual metadata must expose exactly the same labels.
        assertRows(query("sum by(host) (tx)") + " | KEEP result, host", List.of(List.of(7.0, "1e3"), List.of(9.0, "1000.0")));
        assertRows(
            query("tx") + " | KEEP result, _timeseries",
            List.of(List.of(7.0, "{\"cluster\":\"prod\",\"host\":\"1e3\"}"), List.of(9.0, "{\"cluster\":\"prod\",\"host\":\"1000.0\"}"))
        );
        assertRows(
            query("sum by(host)(sum without(cluster)(tx))") + " | KEEP result, host",
            List.of(List.of(7.0, "1e3"), List.of(9.0, "1000.0"))
        );
    }

    public void testResidualLabelsUseIndexedDimensionsWhenStoredSourceOmitsThem() {
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put("index.mode", "time_series")
                        .put("index.mapping.source.mode", "stored")
                        .putList("index.routing_path", "cluster")
                        .put("index.time_series.start_time", "2024-05-10T00:00:00Z")
                        .put("index.time_series.end_time", "2024-05-10T01:00:00Z")
                        .put("index.number_of_shards", 2)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.total_shards_per_node", 1)
                )
                .setMapping("""
                    {
                      "_source": {"excludes": ["host"]},
                      "properties": {
                        "@timestamp": {"type": "date"},
                        "cluster": {"type": "keyword", "time_series_dimension": true},
                        "host": {"type": "keyword", "time_series_dimension": true},
                        "tx": {"type": "double", "time_series_metric": "gauge"}
                      }
                    }
                    """)
        );
        ensureGreen(INDEX);
        var indexed = client().prepareIndex(INDEX).setSource("""
            {"@timestamp":"2024-05-10T00:01:00Z","cluster":"prod","host":1e3,"tx":7.0}
            """, XContentType.JSON).get();
        refresh(INDEX);
        assertFalse(client().prepareGet(INDEX, indexed.getId()).get().getSourceAsMap().containsKey("host"));

        assertRows(query("sum by(host) (tx)") + " | KEEP result, host", List.of(List.of(7.0, "1e3")));
        assertRows(query("tx") + " | KEEP result, _timeseries", List.of(List.of(7.0, "{\"cluster\":\"prod\",\"host\":\"1e3\"}")));
        assertRows(query("sum by(host)(sum without(cluster)(tx))") + " | KEEP result, host", List.of(List.of(7.0, "1e3")));
    }

    /** Reading labels from a derived dictionary must restore their declared source types. */
    public void testByOverWithoutPreservesNativeLabelTypes() {
        createMetricsIndex(INDEX, Settings.builder().put("index.mapping.source.mode", "stored").build());
        String maxUnsigned = "18446744073709551615";
        index(
            INDEX,
            "cluster",
            "prod",
            "host",
            "a",
            "shard",
            "01",
            "enabled",
            true,
            "ip",
            "2001:0db8:0:0:0:0:0:1",
            "sequence",
            maxUnsigned,
            "tx",
            7.0
        );
        index(INDEX, "cluster", "prod", "host", "b", "shard", 1L, "enabled", true, "ip", "2001:db8::1", "sequence", maxUnsigned, "tx", 9.0);
        index(
            INDEX,
            "cluster",
            "prod",
            "host",
            "c",
            "shard",
            2L,
            "enabled",
            false,
            "ip",
            "192.0.2.1",
            "sequence",
            "9223372036854775808",
            "tx",
            5.0
        );
        refresh(INDEX);

        String query = query("sum by(shard,enabled,ip,sequence)(sum without(host)(tx))") + " | KEEP result, shard, enabled, ip, sequence";
        try (EsqlQueryResponse response = run(query)) {
            assertOk(response);
            assertEquals(
                List.of("double", "long", "boolean", "ip", "unsigned_long"),
                response.columns().stream().map(column -> column.type().typeName()).toList()
            );
            assertThat(
                EsqlTestUtils.getValuesList(response),
                containsInAnyOrder(
                    List.of(16.0, 1L, true, "2001:db8::1", new BigInteger(maxUnsigned)),
                    List.of(5.0, 2L, false, "192.0.2.1", new BigInteger("9223372036854775808"))
                )
            );
        }
        assertRows(
            query("sum by(shard)(sum without(host)(tx))") + " | EVAL next_shard = shard + 1 | KEEP result, next_shard",
            List.of(List.of(16.0, 2L), List.of(5.0, 3L))
        );
    }

    /** An inner named aggregation defines the available labels even when the source still contains other dimensions. */
    public void testNamedMissingLabelsCannotReadSourceDimensions() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "shard", 1L, "tx", 7.0);
        index(INDEX, "cluster", "prod", "host", "b", "shard", 2L, "tx", 9.0);
        index(INDEX, "cluster", "dev", "host", "c", "shard", 3L, "tx", 5.0);
        refresh(INDEX);

        assertRows(query("sum by(host)(sum by(cluster)(tx))") + " | KEEP result, host", List.of(Arrays.asList(21.0, null)));
        assertRows(query("sum by(shard)(sum by(cluster)(tx))") + " | KEEP result, shard", List.of(Arrays.asList(21.0, null)));
        assertRows(
            query("sum by(host)(max without(host)(sum by(cluster,host)(tx)))") + " | KEEP result, host",
            List.of(Arrays.asList(14.0, null))
        );
    }

    public void testEmptyAndAbsentLabelsHaveSameIdentity() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "optional", "", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        refresh(INDEX);

        assertRows(query("sum without(host) (tx)") + " | KEEP result, _timeseries", List.of(List.of(30.0, "{\"cluster\":\"prod\"}")));
        assertRows(query("sum by(optional) (tx)") + " | KEEP result, optional", List.of(Arrays.asList(30.0, null)));
        assertRows(query("sum by(optional)(sum without(host)(tx))") + " | KEEP result, optional", List.of(Arrays.asList(30.0, null)));
    }

    /** Cross-metric default match ignores __name__: each side packs rest without it, then joins. */
    public void testDefaultMatchAcrossMetricsIgnoresMetricName() {
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put("index.mode", "time_series")
                        .putList("index.routing_path", "labels.host")
                        .put("index.time_series.start_time", "2024-05-10T00:00:00Z")
                        .put("index.time_series.end_time", "2024-05-10T01:00:00Z")
                        .put("index.number_of_shards", 2)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.total_shards_per_node", 1)
                )
                .setMapping("""
                    {
                      "properties": {
                        "@timestamp": {"type": "date"},
                        "labels": {
                          "type": "object",
                          "properties": {
                            "__name__": {"type": "keyword", "time_series_dimension": true},
                            "host": {"type": "keyword", "time_series_dimension": true}
                          }
                        },
                        "available": {"type": "double", "time_series_metric": "gauge"},
                        "total": {"type": "double", "time_series_metric": "gauge"}
                      }
                    }
                    """)
        );
        ensureGreen(INDEX);
        index(INDEX, "labels", Map.of("__name__", "available", "host", "a"), "available", 75.0);
        index(INDEX, "labels", Map.of("__name__", "total", "host", "a"), "total", 100.0);
        index(INDEX, "labels", Map.of("__name__", "available", "host", "b"), "available", 40.0);
        index(INDEX, "labels", Map.of("__name__", "total", "host", "b"), "total", 80.0);
        refresh(INDEX);

        assertRows(
            query("available / total") + " | KEEP result, _timeseries",
            List.of(List.of(0.75, "{\"host\":\"a\"}"), List.of(0.5, "{\"host\":\"b\"}"))
        );
        // query_range wraps PROMQL in TS_COLLAPSE; dimensions must be the public _timeseries column.
        assertRows(
            query("available / total") + " | TS_COLLAPSE | KEEP result, _timeseries",
            List.of(List.of(0.75, "{\"host\":\"a\"}"), List.of(0.5, "{\"host\":\"b\"}"))
        );
    }

    /** Ordinary object labels still drop the reserved metric name after their logical names are normalized. */
    public void testObjectLabelWithoutDropsNormalizedMetricName() {
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put("index.mode", "time_series")
                        .putList("index.routing_path", "labels.host")
                        .put("index.time_series.start_time", "2024-05-10T00:00:00Z")
                        .put("index.time_series.end_time", "2024-05-10T01:00:00Z")
                        .put("index.number_of_shards", 2)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.total_shards_per_node", 1)
                )
                .setMapping("""
                    {
                      "properties": {
                        "@timestamp": {"type": "date"},
                        "labels": {
                          "type": "object",
                          "properties": {
                            "__name__": {"type": "keyword", "time_series_dimension": true},
                            "host": {"type": "keyword", "time_series_dimension": true}
                          }
                        },
                        "tx": {"type": "double", "time_series_metric": "gauge"}
                      }
                    }
                    """)
        );
        ensureGreen(INDEX);
        index(INDEX, "labels", Map.of("__name__", "first_metric", "host", "a"), "tx", 7.0);
        index(INDEX, "labels", Map.of("__name__", "second_metric", "host", "b"), "tx", 9.0);
        refresh(INDEX);

        for (String operand : List.of("tx", "avg_over_time(tx[1m])")) {
            assertRows(query("sum without(labels.host)(" + operand + ")") + " | KEEP result, _timeseries", List.of(List.of(16.0, "{}")));
        }
    }

    /** Short and qualified passthrough labels exclude the same source dimension before grouping raw samples. */
    public void testPassthroughWithoutExcludesResolvedSourceLabels() {
        indexPassthroughAggregationGroups();
        for (String operand : List.of("tx", "avg_over_time(tx[1m])")) {
            for (String cpu : List.of("cpu", "attributes.cpu")) {
                assertRows(
                    query("sum without(" + cpu + ")(" + operand + ")") + " | KEEP result, _timeseries",
                    List.of(
                        List.of(3.0, "{\"attributes.state\":\"idle\"}"),
                        List.of(5.0, "{\"attributes.state\":\"system\"}"),
                        List.of(10.0, "{\"attributes.state\":\"user\"}")
                    )
                );
            }
            for (String state : List.of("state", "attributes.state")) {
                assertRows(
                    query("sum without(" + state + ")(" + operand + ")") + " | KEEP result, _timeseries",
                    List.of(List.of(6.0, "{\"attributes.cpu\":\"0\"}"), List.of(12.0, "{\"attributes.cpu\":\"1\"}"))
                );
            }
        }
    }

    /** A qualified exclusion above an aggregate cannot erase the grouping used by that aggregate. */
    public void testPassthroughWithoutPreservesNestedAggregationOrder() {
        indexPassthroughAggregationGroups();
        assertRows(
            query("sum without(attributes.state)(max without(cpu)(tx))") + " | KEEP result, _timeseries",
            List.of(List.of(17.0, "{}"))
        );
    }

    /** A concrete root field wins over the short passthrough alias without hiding its qualified dimension. */
    public void testPassthroughWithoutRespectsRootFieldPrecedence() {
        createPassthroughMetricsIndex("\"cpu\": {\"type\": \"keyword\", \"time_series_dimension\": true},");
        index(INDEX, "cpu", "a", "attributes", Map.of("cpu", "0", "state", "idle"), "tx", 1.0);
        index(INDEX, "cpu", "a", "attributes", Map.of("cpu", "1", "state", "idle"), "tx", 2.0);
        index(INDEX, "cpu", "b", "attributes", Map.of("cpu", "0", "state", "idle"), "tx", 5.0);
        index(INDEX, "cpu", "b", "attributes", Map.of("cpu", "1", "state", "idle"), "tx", 10.0);
        refresh(INDEX);

        assertRows(
            query("sum without(cpu)(tx)") + " | KEEP result, _timeseries",
            List.of(
                List.of(6.0, "{\"attributes.cpu\":\"0\",\"attributes.state\":\"idle\"}"),
                List.of(12.0, "{\"attributes.cpu\":\"1\",\"attributes.state\":\"idle\"}")
            )
        );
        assertRows(
            query("sum without(attributes.cpu)(tx)") + " | KEEP result, _timeseries",
            List.of(
                List.of(3.0, "{\"attributes.state\":\"idle\",\"cpu\":\"a\"}"),
                List.of(15.0, "{\"attributes.state\":\"idle\",\"cpu\":\"b\"}")
            )
        );
    }

    /** A short alias removes only the winning passthrough source, while both qualified names remain distinct. */
    public void testPassthroughWithoutRespectsSourcePriority() {
        createPassthroughMetricsIndex("""
            "resource": {
              "properties": {
                "attributes": {
                  "type": "passthrough", "priority": 2, "time_series_dimension": true,
                  "properties": {"cpu": {"type": "keyword"}}
                }
              }
            },
            """);
        index(INDEX, "attributes", Map.of("cpu", "0", "state", "idle"), "resource", Map.of("attributes", Map.of("cpu", "a")), "tx", 1.0);
        index(INDEX, "attributes", Map.of("cpu", "1", "state", "idle"), "resource", Map.of("attributes", Map.of("cpu", "a")), "tx", 2.0);
        index(INDEX, "attributes", Map.of("cpu", "0", "state", "idle"), "resource", Map.of("attributes", Map.of("cpu", "b")), "tx", 5.0);
        index(INDEX, "attributes", Map.of("cpu", "1", "state", "idle"), "resource", Map.of("attributes", Map.of("cpu", "b")), "tx", 10.0);
        refresh(INDEX);

        for (String cpu : List.of("cpu", "resource.attributes.cpu")) {
            assertRows(
                query("sum without(" + cpu + ")(tx)") + " | KEEP result, _timeseries",
                List.of(
                    List.of(6.0, "{\"attributes.cpu\":\"0\",\"attributes.state\":\"idle\"}"),
                    List.of(12.0, "{\"attributes.cpu\":\"1\",\"attributes.state\":\"idle\"}")
                )
            );
        }
        assertRows(
            query("sum without(attributes.cpu)(tx)") + " | KEEP result, _timeseries",
            List.of(
                List.of(3.0, "{\"attributes.state\":\"idle\",\"resource.attributes.cpu\":\"a\"}"),
                List.of(15.0, "{\"attributes.state\":\"idle\",\"resource.attributes.cpu\":\"b\"}")
            )
        );
    }

    private void indexPassthroughAggregationGroups() {
        createPassthroughMetricsIndex("");
        index(INDEX, "attributes", Map.of("cpu", "0", "state", "idle"), "tx", 1.0);
        index(INDEX, "attributes", Map.of("cpu", "1", "state", "idle"), "tx", 2.0);
        index(INDEX, "attributes", Map.of("cpu", "0", "state", "system"), "tx", 5.0);
        index(INDEX, "attributes", Map.of("cpu", "1", "state", "user"), "tx", 10.0);
        refresh(INDEX);
    }

    private void createPassthroughMetricsIndex(String extraProperties) {
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put("index.mode", "time_series")
                        .putList("index.routing_path", "attributes.cpu")
                        .put("index.time_series.start_time", "2024-05-10T00:00:00Z")
                        .put("index.time_series.end_time", "2024-05-10T01:00:00Z")
                        .put("index.number_of_shards", 2)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.total_shards_per_node", 1)
                )
                .setMapping("""
                    {
                      "properties": {
                        "@timestamp": {"type": "date"},
                        "attributes": {
                          "type": "passthrough", "priority": 1, "time_series_dimension": true,
                          "properties": {"cpu": {"type": "keyword"}, "state": {"type": "keyword"}}
                        },
                        %s
                        "tx": {"type": "double", "time_series_metric": "gauge"}
                      }
                    }
                    """.formatted(extraProperties))
        );
        ensureGreen(INDEX);
    }

    /** or ignores __name__: name-keeping left preferred over name-dropping right of the same labels. */
    public void testOrNameKeepingVsNameDroppingRest() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", LabelMatcher.NAME, "tx", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", LabelMatcher.NAME, "tx", "tx", 20.0);
        refresh(INDEX);
        List<List<Object>> leftOnly = List.of(
            List.of(10.0, "{\"__name__\":\"tx\",\"cluster\":\"prod\",\"host\":\"a\"}"),
            List.of(20.0, "{\"__name__\":\"tx\",\"cluster\":\"prod\",\"host\":\"b\"}")
        );
        assertRows(query("(tx) or (ceil(tx))") + " | KEEP result, _timeseries", leftOnly);
        assertRows(query("(tx) or (sum_over_time(tx[1m]))") + " | KEEP result, _timeseries", leftOnly);
        // ceil over the collapsed or drops __name__ from the kept left limb.
        assertRows(
            query("ceil((tx) or (sum_over_time(tx[1m])))") + " | KEEP result, _timeseries",
            List.of(List.of(10.0, "{\"cluster\":\"prod\",\"host\":\"a\"}"), List.of(20.0, "{\"cluster\":\"prod\",\"host\":\"b\"}"))
        );
    }

    /** (… or …) * scalar must pin UnionAll layout before the composed Eval (#158164). */
    public void testOrUnderArithmetic() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        refresh(INDEX);
        assertRows(query("(tx or (tx * 0)) * 2") + " | KEEP result", List.of(List.of(20.0), List.of(40.0)));
    }

    /** (agg or agg) under arithmetic / join must keep a pin-safe UnionAll layout. */
    public void testOrUnderArithmeticJoin() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        refresh(INDEX);
        // join path: or × by-agg vector (series-preserving so labels match)
        assertRows(
            query("((max by (cluster)(tx)) or (min by (cluster)(tx))) * (min by (cluster) (floor(tx)))") + " | KEEP result, cluster",
            List.of(List.of(200.0, "prod"))
        );
        // grafanalabs-shaped ratio of or-chains
        assertRows(
            query(
                "((max by (cluster)(tx)) or (min by (cluster)(tx)))"
                    + " / (((max by (cluster)(tx)) or (min by (cluster)(tx)))"
                    + " + ((max by (cluster)(tx)) or (min by (cluster)(tx))))"
            ) + " | KEEP result, cluster",
            List.of(List.of(0.5, "prod"))
        );
    }

    /** Opaque label_replace: keep series; empty expansion deletes destination (Prometheus absent). */
    public void testLabelReplaceOpaqueKeepsSeries() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        refresh(INDEX);
        // capture group required for a non-empty $1 expansion
        assertRows(
            query("label_replace(sum by (cluster)(tx), \"role\", \"$1\", \"cluster\", \"(.+)\")") + " | KEEP result, cluster, role",
            List.of(List.of(30.0, "prod", "prod"))
        );
        // nomatch: destination unset / absent, series still present
        assertRows(
            query("label_replace(sum by (cluster)(tx), \"role\", \"v\", \"cluster\", \"nomatch\")") + " | KEEP result, cluster",
            List.of(List.of(30.0, "prod"))
        );
        // top-level on collapsed scalar-like: min(sum by) then replace
        assertRows(
            query("label_replace(min(sum by (cluster)(tx)), \"role\", \"v\", \"host\", \"nomatch\")") + " | KEEP result",
            List.of(List.of(30.0))
        );
        // "$1" with no capture → empty expansion → delete dst; must still match unreplac'd left (promfuzz-vm 493 shape)
        assertRows(
            query("(sum by (cluster)(tx))" + " / label_replace(sum by (cluster)(tx), \"role\", \"$1\", \"cluster\", \".+\")")
                + " | KEEP result, cluster",
            List.of(List.of(1.0, "prod"))
        );
        // promfuzz-vm 685-ish: nested across-series of within-series under label_replace(nomatch)
        assertRows(
            query("label_replace(min(ceil(avg(tx))), \"role\", \"v\", \"host\", \"nomatch\")") + " | KEEP result",
            List.of(List.of(15.0))
        );
    }

    /** * on (host) group_left () keeps left labels only. */
    public void testGroupLeftEmptyIncludeKeepsLeftLabels() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        refresh(INDEX);
        assertRows(
            query("(floor(tx)) * on (host) group_left () (ceil(tx))") + " | KEEP result, _timeseries",
            List.of(List.of(100.0, "{\"cluster\":\"prod\",\"host\":\"a\"}"), List.of(400.0, "{\"cluster\":\"prod\",\"host\":\"b\"}"))
        );
    }

    /** raw or rate(...) of the same non-name labels keeps only the left limb. */
    public void testOrCrossMetricRawOrRateKeepsLeftOnly() {
        createMetricsIndexWithRx(INDEX);
        // Two samples so rate() is defined — still dropped by or-match ignoring __name__.
        indexAt(INDEX, "2024-05-10T00:00:00Z", "cluster", "prod", "host", "a", LabelMatcher.NAME, "rx", "rx", 0.0);
        indexAt(INDEX, "2024-05-10T00:00:00Z", "cluster", "prod", "host", "a", LabelMatcher.NAME, "tx", "tx", 0.0);
        indexAt(INDEX, "2024-05-10T00:01:00Z", "cluster", "prod", "host", "a", LabelMatcher.NAME, "rx", "rx", 60.0);
        indexAt(INDEX, "2024-05-10T00:01:00Z", "cluster", "prod", "host", "a", LabelMatcher.NAME, "tx", "tx", 120.0);
        refresh(INDEX);
        assertRows(
            query("(rx) or (rate(tx[1m]))") + " | KEEP result, _timeseries",
            List.of(List.of(60.0, "{\"__name__\":\"rx\",\"cluster\":\"prod\",\"host\":\"a\"}"))
        );
    }

    /** Rate limb survives or when non-name labels do not overlap the left. */
    public void testOrCrossMetricRawOrRateKeepsDisjointRight() {
        createMetricsIndexWithRx(INDEX);
        indexAt(INDEX, "2024-05-10T00:00:00Z", "cluster", "prod", "host", "a", LabelMatcher.NAME, "rx", "rx", 0.0);
        indexAt(INDEX, "2024-05-10T00:00:00Z", "cluster", "prod", "host", "b", LabelMatcher.NAME, "tx", "tx", 0.0);
        indexAt(INDEX, "2024-05-10T00:01:00Z", "cluster", "prod", "host", "a", LabelMatcher.NAME, "rx", "rx", 60.0);
        indexAt(INDEX, "2024-05-10T00:01:00Z", "cluster", "prod", "host", "b", LabelMatcher.NAME, "tx", "tx", 120.0);
        refresh(INDEX);
        assertRows(
            query("(rx) or (rate(tx[1m]))") + " | KEEP result, _timeseries",
            List.of(
                List.of(60.0, "{\"__name__\":\"rx\",\"cluster\":\"prod\",\"host\":\"a\"}"),
                List.of(2.0, "{\"cluster\":\"prod\",\"host\":\"b\"}")
            )
        );
    }

    /** Empty rate limb of or must not alter left fingerprints. */
    public void testOrWithEmptyRateLimbPreservesLeftFingerprints() {
        createMetricsIndexWithRx(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", LabelMatcher.NAME, "rx", "rx", 60.0);
        index(INDEX, "cluster", "prod", "host", "b", LabelMatcher.NAME, "rx", "rx", 80.0);
        // tx present but only one sample → rate(tx[1m]) empty
        index(INDEX, "cluster", "prod", "host", "a", LabelMatcher.NAME, "tx", "tx", 120.0);
        refresh(INDEX);
        List<List<Object>> expected = List.of(
            List.of(60.0, "{\"__name__\":\"rx\",\"cluster\":\"prod\",\"host\":\"a\"}"),
            List.of(80.0, "{\"__name__\":\"rx\",\"cluster\":\"prod\",\"host\":\"b\"}")
        );
        assertRows(query("rx") + " | KEEP result, _timeseries", expected);
        assertRows(query("(rx) or (rate(tx[1m]))") + " | KEEP result, _timeseries", expected);
    }

    /** Cross-metric group_left () result labels are left-only. */
    public void testGroupLeftEmptyIncludeCrossMetricLeftLabelsOnly() {
        createMetricsIndexWithRx(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", LabelMatcher.NAME, "tx", "tx", 10.0, "rx", 2.0);
        index(INDEX, "cluster", "prod", "host", "b", LabelMatcher.NAME, "tx", "tx", 20.0, "rx", 4.0);
        refresh(INDEX);
        assertRows(
            query("(floor(tx)) * on (host) group_left () (ceil(rx))") + " | KEEP result, _timeseries",
            List.of(List.of(20.0, "{\"cluster\":\"prod\",\"host\":\"a\"}"), List.of(80.0, "{\"cluster\":\"prod\",\"host\":\"b\"}"))
        );
    }

    /** Empty left of or must not shadow the raw right limb. */
    public void testOrAfterEmptyIgnoringArithKeepsRawRightLabels() {
        createMetricsIndexWithRx(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", LabelMatcher.NAME, "rx", "rx", 60.0);
        index(INDEX, "cluster", "prod", "host", "b", LabelMatcher.NAME, "rx", "rx", 80.0);
        index(INDEX, "cluster", "prod", "host", "a", LabelMatcher.NAME, "tx", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", LabelMatcher.NAME, "tx", "tx", 20.0);
        refresh(INDEX);
        List<List<Object>> expected = List.of(
            List.of(60.0, "{\"__name__\":\"rx\",\"cluster\":\"prod\",\"host\":\"a\"}"),
            List.of(80.0, "{\"__name__\":\"rx\",\"cluster\":\"prod\",\"host\":\"b\"}")
        );
        // Empty selector × ignoring: must be empty, not the right operand.
        assertRows(query("(rx{host=\"nope\"}) * ignoring (host) (tx)") + " | KEEP result, _timeseries", List.of());
        // Empty rate limb on the left (single tx sample) must not strip labels from the raw right.
        assertRows(query("(rate(tx[1m])) or (rx)") + " | KEEP result, _timeseries", expected);
        // Empty matcher left of or → raw right with full fingerprints.
        assertRows(query("(rx{host=\"nope\"}) or (rx)") + " | KEEP result, _timeseries", expected);
        // Empty arith/compare left of or → same (promfuzz-vm 436).
        assertRows(query("(((rx{host=\"nope\"}) * ignoring (host) (tx)) == (rx)) or (rx)") + " | KEEP result, _timeseries", expected);
    }

    /** Empty right of or (disjoint label_replace/and) must keep left fingerprints. */
    public void testOrWithLabelReplaceRightKeepsTierWhenDisjoint() {
        createMetricsIndexWithRx(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", LabelMatcher.NAME, "tx", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", LabelMatcher.NAME, "rx", "rx", 20.0);
        refresh(INDEX);
        // Destination is a named column beside rest (not necessarily inside the _timeseries blob).
        assertRows(
            query("label_replace(rx, \"tier\", \"x-$1\", \"host\", \"(.*)\")") + " | KEEP result, tier, _timeseries",
            List.of(List.of(20.0, "x-b", "{\"__name__\":\"rx\",\"cluster\":\"prod\",\"host\":\"b\"}"))
        );
        // Different hosts → or keeps both; right still exposes tier.
        assertRows(
            query("(floor(tx)) or (label_replace(rx, \"tier\", \"x-$1\", \"host\", \"(.*)\"))") + " | KEEP result, tier, _timeseries",
            List.of(
                Arrays.asList(10.0, null, "{\"cluster\":\"prod\",\"host\":\"a\"}"),
                Arrays.asList(20.0, "x-b", "{\"__name__\":\"rx\",\"cluster\":\"prod\",\"host\":\"b\"}")
            )
        );
        // Fuzz 050 shape: empty right (label_replace × and-ignoring does not match on tier) must not wipe left labels.
        assertRows(
            query(
                "(floor(tx)) or (label_replace(rx, \"tier\", \"x-$1\", \"host\", \".+\") " + "and ((rx) and ignoring (host, cluster) (tx)))"
            ) + " | KEEP result, _timeseries",
            List.of(List.of(10.0, "{\"cluster\":\"prod\",\"host\":\"a\"}"))
        );
        // Same with bottomk on the left (fuzz wraps bottomk in math); union must still expose left rest.
        assertRows(
            query(
                "(bottomk(1, tx)) or (label_replace(rx, \"tier\", \"x-$1\", \"host\", \".+\") "
                    + "and ((rx) and ignoring (host, cluster) (tx)))"
            ) + " | KEEP result, _timeseries",
            List.of(List.of(10.0, "{\"__name__\":\"tx\",\"cluster\":\"prod\",\"host\":\"a\"}"))
        );
    }

    /** or with name-dropping label_replace right keeps both limbs; listener finds _timeseries. */
    public void testOrWithLabelReplaceIncreaseKeepsBothLimbs() {
        createMetricsIndexWithRx(INDEX);
        indexAt(INDEX, "2024-05-10T00:00:00Z", "cluster", "prod", "host", "a", LabelMatcher.NAME, "tx", "tx", 10.0);
        indexAt(INDEX, QUERY_TIME, "cluster", "prod", "host", "a", LabelMatcher.NAME, "tx", "tx", 10.0);
        indexAt(INDEX, "2024-05-10T00:00:00Z", "cluster", "prod", "host", "b", LabelMatcher.NAME, "rx", "rx", 0.0);
        indexAt(INDEX, QUERY_TIME, "cluster", "prod", "host", "b", LabelMatcher.NAME, "rx", "rx", 60.0);
        refresh(INDEX);
        assertRows(
            query("(floor(tx)) or (label_replace(round(increase(rx[5m])), \"tier\", \"x-$1\", \"host\", \"(.*)\"))")
                + " | KEEP result, tier, _timeseries",
            List.of(
                Arrays.asList(10.0, null, "{\"cluster\":\"prod\",\"host\":\"a\"}"),
                Arrays.asList(60.0, "x-b", "{\"cluster\":\"prod\",\"host\":\"b\"}")
            )
        );
    }

    /** topk + on (...) keeps matched series labels. */
    public void testTopkPlusOnRangeFnKeepsMatchedLabels() {
        createMetricsIndex(INDEX);
        indexAt(INDEX, "2024-05-10T00:00:00Z", "cluster", "prod", "host", "a", "tx", 10.0);
        indexAt(INDEX, "2024-05-10T00:00:00Z", "cluster", "prod", "host", "b", "tx", 30.0);
        indexAt(INDEX, "2024-05-10T00:01:00Z", "cluster", "prod", "host", "a", "tx", 10.0);
        indexAt(INDEX, "2024-05-10T00:01:00Z", "cluster", "prod", "host", "b", "tx", 30.0);
        refresh(INDEX);
        // topk(1) keeps host=b (higher value); stddev_over_time is 0 for flat series; sum = 30.
        // Must expose _timeseries after on() join (fuzz 855 was label-fingerprint empty).
        assertRows(
            query("(topk(1, tx)) + on (host, cluster) (ceil(stddev_over_time(tx[2m])))") + " | KEEP result, host, cluster",
            List.of(List.of(30.0, "b", "prod"))
        );
    }

    /** Default matching: labeled × labelless is empty (not scalar broadcast). */
    public void testArithLabeledTimesUnlabeledIsEmpty() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        refresh(INDEX);
        // Controls: without must drop the listed labels; labelless sum is one series.
        assertRows(
            query("max without (cluster) (tx)") + " | KEEP result, _timeseries",
            List.of(List.of(10.0, "{\"host\":\"a\"}"), List.of(20.0, "{\"host\":\"b\"}"))
        );
        assertRows(query("sum without (cluster, host) (tx)") + " | KEEP result, _timeseries", List.of(List.of(30.0, "{}")));
        // Default matching: {host} × {} is empty (not scalar broadcast).
        try (
            EsqlQueryResponse response = run(
                query("(max without (cluster) (tx)) * (sum without (cluster, host) (tx))") + " | KEEP result, _timeseries"
            )
        ) {
            assertOk(response);
            var actual = EsqlTestUtils.getValuesList(response);
            assertThat("labeled×labelless must be empty; actual=" + actual, actual, equalTo(List.of()));
        }
    }

    /** unless with empty right keeps left. */
    public void testUnlessEmptyRightKeepsLeft() {
        createMetricsIndexWithRx(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", LabelMatcher.NAME, "rx", "rx", 60.0);
        index(INDEX, "cluster", "prod", "host", "b", LabelMatcher.NAME, "rx", "rx", 80.0);
        // tx exists but host filter matches nothing → empty right
        index(INDEX, "cluster", "prod", "host", "a", LabelMatcher.NAME, "tx", "tx", 1.0);
        refresh(INDEX);
        List<List<Object>> expected = List.of(
            List.of(60.0, "{\"__name__\":\"rx\",\"cluster\":\"prod\",\"host\":\"a\"}"),
            List.of(80.0, "{\"__name__\":\"rx\",\"cluster\":\"prod\",\"host\":\"b\"}")
        );
        assertRows(query("rx") + " | KEEP result, _timeseries", expected);
        assertRows(query("(rx) unless (tx{host=\"nope\"})") + " | KEEP result, _timeseries", expected);
    }

    /** without (...) over a within-series fn drops the listed labels. */
    public void testWithoutOverWithinSeriesFn() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        refresh(INDEX);
        // without(host) over ceil: both hosts collapse to one series per remaining labels
        assertRows(
            query("max without (host) (ceil(tx))") + " | KEEP result, _timeseries",
            List.of(List.of(20.0, "{\"cluster\":\"prod\"}"))
        );
    }

    /** Labelless agg or vector(0) left-prefers; empty by-group must not invent keys. */
    public void testOrVectorZeroFallback() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "staging", "host", "b", "tx", 20.0);
        index(INDEX, "cluster", "qa", "host", "c", "tx", 5.0);
        refresh(INDEX);
        assertRows(query("sum(tx) or vector(0)") + " | KEEP result", List.of(List.of(35.0)));
        assertRows(query("sum(tx{host=\"missing\"}) or vector(0)") + " | KEEP result", List.of(List.of(0.0)));
        assertRows(query("(max(sum by (cluster)(tx))) or vector(0)") + " | KEEP result", List.of(List.of(20.0)));
        // csv-spec set_operator_union_vector_zero_fallback: empty by-grouped left + vector(0) → one labelless 0
        assertRows(
            query("max by (cluster) (tx{cluster=\"does_not_exist\"}) or vector(0)") + " | KEEP result, cluster",
            List.of(Arrays.asList(0.0, null))
        );
        // csv-spec shape (step=1h): empty by-grouped left + vector(0) must stay labelless at every step
        assertRows(
            "PROMQL index="
                + INDEX
                + " start=\"2024-05-10T00:00:00Z\" end=\"2024-05-10T01:00:00Z\" step=1h result=("
                + "max by (cluster) (tx{cluster=\"does_not_exist\"}) or vector(0))"
                + " | KEEP result, cluster",
            List.of(Arrays.asList(0.0, null), Arrays.asList(0.0, null))
        );
    }

    /** Arithmetic mask with and executes on the typed path. */
    public void testAndWithArithmeticMask() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        refresh(INDEX);
        assertRows(
            query("((max by (cluster)(tx) * 0 + 1) and ((max by (cluster)(tx) / max by (cluster)(tx)) >= 0.8))")
                + " | KEEP result, cluster",
            List.of(List.of(1.0, "prod"))
        );
    }

    private void createMetricsIndex(String name) {
        createMetricsIndex(name, Settings.EMPTY);
    }

    private void createMetricsIndex(String name, Settings overrides) {
        createMetricsIndex(name, overrides, "date");
    }

    private void createMetricsIndex(String name, Settings overrides, String timestampType) {
        createMetricsIndex(name, overrides, timestampType, false);
    }

    private void createMetricsIndexWithRx(String name) {
        createMetricsIndex(name, Settings.EMPTY, "date", true);
    }

    private void createMetricsIndex(String name, Settings overrides, String timestampType, boolean withRx) {
        var mapping = new java.util.LinkedHashMap<String, Object>();
        mapping.put("@timestamp", "type=" + timestampType);
        mapping.put("cluster", "type=keyword,time_series_dimension=true");
        mapping.put("host", "type=keyword,time_series_dimension=true");
        mapping.put("region", "type=keyword,time_series_dimension=true");
        mapping.put("optional", "type=keyword,time_series_dimension=true");
        mapping.put(LabelMatcher.NAME, "type=keyword,time_series_dimension=true");
        mapping.put("shard", "type=long,time_series_dimension=true");
        mapping.put("ip", "type=ip,time_series_dimension=true");
        mapping.put("enabled", "type=boolean,time_series_dimension=true");
        mapping.put("sequence", "type=unsigned_long,time_series_dimension=true");
        mapping.put("tx", "type=double,time_series_metric=gauge");
        if (withRx) {
            mapping.put("rx", "type=double,time_series_metric=gauge");
        }
        assertAcked(
            indicesAdmin().prepareCreate(name)
                .setSettings(
                    Settings.builder()
                        .put("index.mode", "time_series")
                        .putList("index.routing_path", "cluster", "host")
                        .put("index.time_series.start_time", "2024-05-10T00:00:00Z")
                        .put("index.time_series.end_time", "2024-05-10T01:00:00Z")
                        .put("index.number_of_shards", 2)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.total_shards_per_node", 1)
                        .put(overrides)
                )
                .setMapping(
                    mapping.entrySet()
                        .stream()
                        .flatMap(e -> java.util.stream.Stream.of(e.getKey(), e.getValue().toString()))
                        .toArray(String[]::new)
                )
        );
        ensureGreen(name);
    }

    private void index(String index, Object... fields) {
        indexAt(index, QUERY_TIME, fields);
    }

    private void indexAt(String index, String timestamp, Object... fields) {
        Object[] source = new Object[fields.length + 2];
        source[0] = "@timestamp";
        // Range queries read from their start time, so keep samples on the evaluated step by default.
        source[1] = timestamp;
        System.arraycopy(fields, 0, source, 2, fields.length);
        client().prepareIndex(index).setSource(source).get();
    }

    private static String query(String expression) {
        return query(INDEX, expression, QUERY_TIME);
    }

    private static String query(String index, String expression, String end) {
        return "PROMQL "
            + (index == null ? "" : "index=" + index + " ")
            + "start=\""
            + QUERY_TIME
            + "\" end=\""
            + end
            + "\" step=1m result=("
            + expression
            + ")";
    }

    private void assertRows(String query, List<List<Object>> expected) {
        try (EsqlQueryResponse response = run(query)) {
            assertOk(response);
            assertThat(EsqlTestUtils.getValuesList(response), containsInAnyOrder(expected.toArray()));
        }
    }
}
