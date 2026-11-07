/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongMapperPlugin;
import org.junit.BeforeClass;

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
        assumeTrue("requires functional time-series metadata", EsqlCapabilities.Cap.ENABLE_TIMESERIES_DATA_TYPE.isEnabled());
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

    /**
     * topk, bottomk, limitk and limit_ratio with without(...) partition by every label but the listed ones and the metric
     * name, and keep each selected series with all of its labels, as Prometheus does.
     */
    public void testReductionsWithoutPartitionByTheRestOfTheLabels() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 1.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 2.0);
        index(INDEX, "cluster", "dev", "host", "a", "tx", 3.0);
        index(INDEX, "cluster", "dev", "host", "b", "tx", 4.0);
        refresh(INDEX);
        String prodA = "{\"cluster\":\"prod\",\"host\":\"a\"}";
        String prodB = "{\"cluster\":\"prod\",\"host\":\"b\"}";
        String devA = "{\"cluster\":\"dev\",\"host\":\"a\"}";
        String devB = "{\"cluster\":\"dev\",\"host\":\"b\"}";

        assertRows(query("topk without (host) (1, tx)") + " | KEEP result, _timeseries", List.of(List.of(2.0, prodB), List.of(4.0, devB)));
        assertRows(
            query("bottomk without (host) (1, tx)") + " | KEEP result, _timeseries",
            List.of(List.of(1.0, prodA), List.of(3.0, devA))
        );
        assertRows(query("topk without (host, cluster) (1, tx)") + " | KEEP result, _timeseries", List.of(List.of(4.0, devB)));
        assertRows(
            query("count by (cluster) (limitk without (host) (1, tx))") + " | KEEP result, cluster",
            List.of(List.of(1.0, "prod"), List.of(1.0, "dev"))
        );
        assertRows(
            query("limit_ratio without (host) (1, tx)") + " | KEEP result, _timeseries",
            List.of(List.of(1.0, prodA), List.of(2.0, prodB), List.of(3.0, devA), List.of(4.0, devB))
        );
    }

    /**
     * A filter comparison against a scalar computed from the source keeps the vector's value, whichever side the scalar is
     * on, as Prometheus does; with bool it keeps every series with 0 or 1.
     */
    public void testComparisonWithComputedScalarKeepsTheVectorValue() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 1.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 3.0);
        refresh(INDEX);
        String hostB = "{\"cluster\":\"prod\",\"host\":\"b\"}";

        assertRows(query("tx > scalar(avg(tx))") + " | KEEP result, _timeseries", List.of(List.of(3.0, hostB)));
        assertRows(query("scalar(avg(tx)) < tx") + " | KEEP result, _timeseries", List.of(List.of(3.0, hostB)));
        assertRows(query("tx > bool scalar(avg(tx))") + " | KEEP result", List.of(List.of(0.0), List.of(1.0)));
    }

    /**
     * A filter comparison keeps or drops each series on its own, whatever its consumer groups or matches by: series sharing
     * the labels a by() or an on() reads are compared one by one, not merged first. bool yields 1 or 0 to aggregate.
     */
    public void testComparisonFiltersEachSeriesUnderGroupingAndMatching() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "region", "east", "tx", 1.0);
        index(INDEX, "cluster", "prod", "host", "b", "region", "east", "tx", 3.0);
        index(INDEX, "cluster", "prod", "host", "c", "region", "west", "tx", 5.0);
        refresh(INDEX);
        String a = "{\"cluster\":\"prod\",\"host\":\"a\",\"region\":\"east\"}";
        String b = "{\"cluster\":\"prod\",\"host\":\"b\",\"region\":\"east\"}";
        String c = "{\"cluster\":\"prod\",\"host\":\"c\",\"region\":\"west\"}";

        assertRows(query("sum by (region) (tx > 2)") + " | KEEP result, region", List.of(List.of(3.0, "east"), List.of(5.0, "west")));
        assertRows(query("sum by (region) (tx > bool 2)") + " | KEEP result, region", List.of(List.of(1.0, "east"), List.of(1.0, "west")));
        assertRows(
            query("tx and on(region) (tx > 2)") + " | KEEP result, _timeseries",
            List.of(List.of(1.0, a), List.of(3.0, b), List.of(5.0, c))
        );
        assertRows(query("tx and on(region) (tx > 4)") + " | KEEP result, _timeseries", List.of(List.of(5.0, c)));
        assertRows(query("tx unless on(region) (tx > 4)") + " | KEEP result, _timeseries", List.of(List.of(1.0, a), List.of(3.0, b)));
        assertRows(query("tx unless ignoring(host) (tx > 2)") + " | KEEP result, _timeseries", List.of());
    }

    /** unless between operands whose series never share a group keeps the left operand whole, as in Prometheus. */
    public void testUnlessAcrossLabelSets() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "region", "east", "tx", 1.0);
        index(INDEX, "cluster", "prod", "host", "b", "region", "east", "tx", 3.0);
        index(INDEX, "cluster", "prod", "host", "c", "region", "west", "tx", 5.0);
        refresh(INDEX);

        assertRows(
            query("sum by (host) (tx) unless sum by (region) (tx)") + " | KEEP result, host",
            List.of(List.of(1.0, "a"), List.of(3.0, "b"), List.of(5.0, "c"))
        );
        assertRows(query("sum(tx) unless tx") + " | KEEP result", List.of(List.of(9.0)));
        assertRows(query("sum by (region) (tx) unless sum by (region) (tx > 4)") + " | KEEP result, region", List.of(List.of(4.0, "east")));
    }

    /**
     * or with on/ignoring keeps every left series, several sharing a match signature included, and adds the right series
     * no left series matches on the listed labels, as in Prometheus.
     */
    public void testOrWithMatching() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "region", "east", "tx", 1.0);
        index(INDEX, "cluster", "prod", "host", "b", "region", "east", "tx", 3.0);
        index(INDEX, "cluster", "prod", "host", "c", "region", "west", "tx", 5.0);
        refresh(INDEX);
        String a = "{\"cluster\":\"prod\",\"host\":\"a\",\"region\":\"east\"}";
        String b = "{\"cluster\":\"prod\",\"host\":\"b\",\"region\":\"east\"}";
        String c = "{\"cluster\":\"prod\",\"host\":\"c\",\"region\":\"west\"}";

        assertRows(
            query("tx{region=\"east\"} or on(region) tx{region=\"west\"}") + " | KEEP result, _timeseries | SORT result",
            List.of(List.of(1.0, a), List.of(3.0, b), List.of(5.0, c))
        );
        // a and b share region east: both stay, and the right operand adds only c.
        assertRows(
            query("tx{region=\"east\"} or on(region) tx") + " | KEEP result, _timeseries | SORT result",
            List.of(List.of(1.0, a), List.of(3.0, b), List.of(5.0, c))
        );
        // b shares cluster and region with a, so only c is added.
        assertRows(
            query("tx{host=\"a\"} or ignoring(host) tx{host=~\"b|c\"}") + " | KEEP result, _timeseries | SORT result",
            List.of(List.of(1.0, a), List.of(5.0, c))
        );
        assertRows(query("tx{host=\"a\"} or ignoring(host, region) tx") + " | KEEP result, _timeseries", List.of(List.of(1.0, a)));
        // on () adds the right operand only at steps where the left one has no series.
        assertRows(query("tx{host=\"nope\"} or on() tx{host=\"c\"}") + " | KEEP result, _timeseries", List.of(List.of(5.0, c)));
        assertRows(query("tx{host=\"a\"} or on() tx{host=\"c\"}") + " | KEEP result, _timeseries", List.of(List.of(1.0, a)));
        // vector(7) has no region: its signature never matches a series that has one. Its constant holds on its own rows only.
        assertRows(query("tx{host=\"a\"} unless on(region) vector(7)") + " | KEEP result, _timeseries", List.of(List.of(1.0, a)));
        assertRows(
            query("vector(7) or on(region) tx{host=\"c\"}") + " | KEEP result, _timeseries | SORT result",
            List.of(List.of(5.0, c), List.of(7.0, "{}"))
        );
    }

    /**
     * group_left copies the one side's labels over the many side's: two many side series left with the same labels fail
     * the query, as in Prometheus, rather than come back as two series with one label set.
     */
    public void testGroupLeftLabelCollisionFails() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "region", "east", "tx", 1.0);
        index(INDEX, "cluster", "prod", "host", "b", "region", "east", "tx", 3.0);
        refresh(INDEX);
        // The one side has no host, so both series lose theirs.
        var error = expectThrows(Exception.class, () -> run(query("tx * on(cluster) group_left(host) sum by (cluster) (tx)")));
        assertThat(error.getMessage(), containsString("input must not have duplicates"));
        // Prometheus matches before it compares: a comparison no series passes fails too.
        var filtered = expectThrows(Exception.class, () -> run(query("tx > on(cluster) group_left(host) (sum by (cluster) (tx) * 100)")));
        assertThat(filtered.getMessage(), containsString("input must not have duplicates"));
        // Copying a label the series agree on keeps them apart.
        assertRows(
            query("tx * on(cluster) group_left(region) sum by (cluster, region) (tx)") + " | KEEP result, region | SORT result",
            List.of(List.of(4.0, "east"), List.of(12.0, "east"))
        );
    }

    /**
     * label_replace and label_join over a by(...) aggregation may overwrite one of its labels. Two series left with the same
     * labels fail the query, as in Prometheus, rather than come back as two series with one label set.
     */
    public void testRelabelOverwritingGroupingLabel() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "region", "east", "tx", 1.0);
        index(INDEX, "cluster", "prod", "host", "b", "region", "east", "tx", 3.0);
        refresh(INDEX);
        assertRows(
            query("label_replace(sum by (host) (tx), \"host\", \"h-$1\", \"host\", \"(.*)\")") + " | KEEP result, host | SORT result",
            List.of(List.of(1.0, "h-a"), List.of(3.0, "h-b"))
        );
        assertRows(
            query("label_join(sum by (host, region) (tx), \"host\", \"-\", \"host\", \"region\")") + " | KEEP result, host | SORT result",
            List.of(List.of(1.0, "a-east"), List.of(3.0, "b-east"))
        );
        assertRows(
            query("topk(1, label_replace(sum by (host) (tx), \"host\", \"h-$1\", \"host\", \"(.*)\"))") + " | KEEP result, host",
            List.of(List.of(3.0, "h-b"))
        );
        var merged = expectThrows(
            Exception.class,
            () -> run(query("label_replace(sum by (host) (tx), \"host\", \"x\", \"host\", \".*\")"))
        );
        assertThat(merged.getMessage(), containsString("input must not have duplicates"));
        // Over raw series too, below an aggregation: Prometheus fails the relabel before the aggregation reads it.
        var raw = expectThrows(Exception.class, () -> run(query("sum by (region) (label_replace(tx, \"host\", \"x\", \"host\", \".*\"))")));
        assertThat(raw.getMessage(), containsString("input must not have duplicates"));
        // A label the source does not map cannot be overwritten.
        assertRows(
            query("sum by (tier) (label_replace(tx, \"tier\", \"t-$1\", \"host\", \"(.*)\"))") + " | KEEP result, tier | SORT result",
            List.of(List.of(1.0, "t-a"), List.of(3.0, "t-b"))
        );
    }

    /**
     * Matching on __name__ is rejected even where the series store it, as here: which series store it is known only from
     * the data, and one whose metric is a field reads it as absent.
     */
    public void testMatchingOnMetricNameIsRejected() {
        createMetricsIndexWithRx(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", LabelMatcher.NAME, "rx", "rx", 60.0);
        index(INDEX, "cluster", "prod", "host", "a", LabelMatcher.NAME, "tx", "tx", 10.0);
        refresh(INDEX);
        for (String q : List.of("tx or on(__name__, host) rx", "tx / on(__name__, host) rx")) {
            var error = expectThrows(Exception.class, () -> run(query(q)));
            assertThat(error.getMessage(), containsString("matching on __name__ is not supported at this time"));
        }
    }

    /** Reject result names that collide with labels/metadata before the plan consistency checker. */
    public void testResultNameMustNotShadowLabelsOrMetadata() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 7.0);
        refresh(INDEX);
        var label = expectThrows(VerificationException.class, () -> run(query("sum by(cluster)(tx)").replace(" result=", " cluster=")));
        assertThat(label.getMessage(), containsString("label [cluster] collides with the built-in [cluster] output column"));
        var metadata = expectThrows(VerificationException.class, () -> run(query("tx").replace(" result=", " _timeseries=")));
        assertThat(metadata.getMessage(), containsString("result column [_timeseries] collides with the time-series metadata column"));
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
                .setMapping(Strings.format("""
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
                    """, extraProperties))
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

    /** (... or ...) * scalar must pin UnionAll layout before the composed Eval (#158164). */
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
        // join path: or x by-agg vector (series-preserving so labels match)
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
        // "$1" with no capture -> empty expansion -> delete dst; must still match unreplac'd left (promfuzz-vm 493 shape)
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
        // Two samples so rate() is defined - still dropped by or-match ignoring __name__.
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
        // tx present but only one sample -> rate(tx[1m]) empty
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
        // Empty selector x ignoring: must be empty, not the right operand. One tx series, as ignoring (host) makes the two
        // a duplicate match group, which fails the query.
        assertRows(query("(rx{host=\"nope\"}) * ignoring (host) (tx{host=\"a\"})") + " | KEEP result, _timeseries", List.of());
        // Empty rate limb on the left (single tx sample) must not strip labels from the raw right.
        assertRows(query("(rate(tx[1m])) or (rx)") + " | KEEP result, _timeseries", expected);
        // Empty matcher left of or -> raw right with full fingerprints.
        assertRows(query("(rx{host=\"nope\"}) or (rx)") + " | KEEP result, _timeseries", expected);
        // Empty arith/compare left of or -> same (promfuzz-vm 436).
        assertRows(
            query("(((rx{host=\"nope\"}) * ignoring (host) (tx{host=\"a\"})) == (rx)) or (rx)") + " | KEEP result, _timeseries",
            expected
        );
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
        // Different hosts -> or keeps both; right still exposes tier.
        assertRows(
            query("(floor(tx)) or (label_replace(rx, \"tier\", \"x-$1\", \"host\", \"(.*)\"))") + " | KEEP result, tier, _timeseries",
            List.of(
                Arrays.asList(10.0, null, "{\"cluster\":\"prod\",\"host\":\"a\"}"),
                Arrays.asList(20.0, "x-b", "{\"__name__\":\"rx\",\"cluster\":\"prod\",\"host\":\"b\"}")
            )
        );
        // Fuzz 050 shape: empty right (label_replace x and-ignoring does not match on tier) must not wipe left labels.
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

    /** Default matching: labeled x labelless is empty (not scalar broadcast). */
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
        // Default matching: {host} x {} is empty (not scalar broadcast).
        try (
            EsqlQueryResponse response = run(
                query("(max without (cluster) (tx)) * (sum without (cluster, host) (tx))") + " | KEEP result, _timeseries"
            )
        ) {
            assertOk(response);
            var actual = EsqlTestUtils.getValuesList(response);
            assertThat("labeled x labelless must be empty; actual=" + actual, actual, equalTo(List.of()));
        }
    }

    /** unless with empty right keeps left. */
    public void testUnlessEmptyRightKeepsLeft() {
        createMetricsIndexWithRx(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", LabelMatcher.NAME, "rx", "rx", 60.0);
        index(INDEX, "cluster", "prod", "host", "b", LabelMatcher.NAME, "rx", "rx", 80.0);
        // tx exists but host filter matches nothing -> empty right
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
        // csv-spec set_operator_union_vector_zero_fallback: empty by-grouped left + vector(0) -> one labelless 0
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
