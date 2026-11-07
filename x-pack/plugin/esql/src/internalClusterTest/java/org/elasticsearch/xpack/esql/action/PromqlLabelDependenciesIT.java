/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * Label dependencies of PromQL operations that the result labels alone do not show: grouping keys no parent reads, closed
 * BY grains, non-dimension fields, metadata view identity, and histogram families.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class PromqlLabelDependenciesIT extends AbstractEsqlIntegTestCase {
    private static final String INDEX = "promql-label-dependencies";
    private static final String TIME = "2024-05-10T00:01:00Z";

    @Override
    protected QueryPragmas getPragmas() {
        return canUseQueryPragmas() ? new QueryPragmas(Settings.builder().put("page_size", 1).build()) : super.getPragmas();
    }

    public void testUnusedByKeyStillDeterminesRows() {
        createIndex();
        index("host", "a", "tx", 1.0);
        index("host", "b", "tx", 10.0);
        refresh(INDEX);
        assertRows("sum(max by(host)(tx))", " | KEEP result", List.of(List.of(11.0)));
    }

    public void testMissingLabelCannotCrossClosedBy() {
        createIndex();
        index("host", "a", "tx", 1.0);
        index("host", "b", "tx", 10.0);
        refresh(INDEX);
        assertRows("sum by(host)(sum by(cluster)(tx))", " | KEEP result, host", List.of(Arrays.asList(11.0, null)));
    }

    public void testNonDimensionUsesNativeColumnAndRespectsClosedBy() {
        createIndex();
        index("host", "a", "event", "e1", "tx", 1.0);
        index("host", "b", "event", "e2", "tx", 10.0);
        refresh(INDEX);
        assertRows("sum by(event)(tx)", " | KEEP result, event", List.of(List.of(1.0, "e1"), List.of(10.0, "e2")));
        assertRows("sum by(event)(sum by(cluster)(tx))", " | KEEP result, event", List.of(Arrays.asList(11.0, null)));
    }

    /** A label named {@code a$b} leaves out a different view than labels {@code a} and {@code b}. */
    public void testDistinctMetadataViewsDoNotCollideOnLabelSeparator() {
        assumeTrue("requires typed time-series metadata", EsqlCapabilities.Cap.ENABLE_TIMESERIES_DATA_TYPE.isEnabled());
        createIndex();
        index("host", "a", "tx", 1.0, "rx", 2.0);
        refresh(INDEX);
        String expression = "label_join(tx, \"a$b\", \"-\", \"host\") or "
            + "label_join(label_join(rx, \"a\", \"-\", \"host\"), \"b\", \"-\", \"host\")";
        assertRows(expression, " | KEEP result", List.of(List.of(1.0), List.of(2.0)));
    }

    /** Removing the metric name from output must not merge histogram families before computing their quantiles. */
    public void testHistogramMetricNameIsAnInternalGroupingDependency() {
        createIndex();
        index("host", "a", "__name__", "m1", "le", "1", "tx", 2.0);
        index("host", "a", "__name__", "m1", "le", "+Inf", "tx", 4.0);
        index("host", "a", "__name__", "m2", "le", "1", "tx", 6.0);
        index("host", "a", "__name__", "m2", "le", "+Inf", "tx", 8.0);
        refresh(INDEX);
        // m1: target count 2 in a bucket of count 2 gives 1. m2: target 4 in a bucket of count 6 gives 2/3.
        // Combining their buckets first would incorrectly produce 6/8 = 0.75 as a single quantile.
        assertRows("histogram_quantile(0.5,sum by(le,__name__,cluster)(tx))", " | KEEP result", List.of(List.of(1.0), List.of(2.0 / 3.0)));
    }

    private void createIndex() {
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put("index.mode", "time_series")
                        .putList("index.routing_path", "cluster", "host")
                        .put("index.time_series.start_time", "2024-05-10T00:00:00Z")
                        .put("index.time_series.end_time", "2024-05-10T01:00:00Z")
                        .put("index.number_of_shards", 2)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.total_shards_per_node", 1)
                )
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "cluster",
                    "type=keyword,time_series_dimension=true",
                    "host",
                    "type=keyword,time_series_dimension=true",
                    "__name__",
                    "type=keyword,time_series_dimension=true",
                    "le",
                    "type=keyword,time_series_dimension=true",
                    "event",
                    "type=keyword",
                    "tx",
                    "type=double,time_series_metric=gauge",
                    "rx",
                    "type=double,time_series_metric=gauge"
                )
        );
        ensureGreen(INDEX);
    }

    private void index(Object... fields) {
        Object[] source = new Object[fields.length + 4];
        source[0] = "@timestamp";
        source[1] = TIME;
        source[2] = "cluster";
        source[3] = "prod";
        System.arraycopy(fields, 0, source, 4, fields.length);
        client().prepareIndex(INDEX).setSource(source).get();
    }

    private void assertRows(String expression, String suffix, List<List<Object>> expected) {
        String query = "PROMQL index="
            + INDEX
            + " start=\""
            + TIME
            + "\" end=\""
            + TIME
            + "\" step=1m result=("
            + expression
            + ")"
            + suffix;
        try (EsqlQueryResponse response = run(query)) {
            assertOk(response);
            assertThat(EsqlTestUtils.getValuesList(response), containsInAnyOrder(expected.toArray()));
        }
    }
}
