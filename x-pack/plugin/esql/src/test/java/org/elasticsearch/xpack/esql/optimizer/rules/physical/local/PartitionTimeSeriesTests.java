/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.optimizer.AbstractLocalPhysicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class PartitionTimeSeriesTests extends AbstractLocalPhysicalPlanOptimizerTests {
    final DateFormatter fmt = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;

    public PartitionTimeSeriesTests(String name, Configuration config) {
        super(name, config);
    }

    private void runTest(
        long totalDocs,
        String minTimestampFromData,
        String maxTimestampFromData,
        String queryStartTs,
        String queryEndTs,
        ByteSizeValue rateBufferSize,
        int taskConcurrency,
        List<QueryRange> expectedRanges
    ) {
        Map<String, Object> minValue = Map.of("@timestamp", fmt.parseMillis(minTimestampFromData));
        Map<String, Object> maxValue = Map.of("@timestamp", fmt.parseMillis(maxTimestampFromData));
        SearchStats searchStats = new EsqlTestUtils.TestSearchStatsWithMinMax(minValue, maxValue) {
            @Override
            public Map<ShardId, IndexMetadata> targetShards() {
                var indexMetadata = IndexMetadata.builder("k8s")
                    .settings(
                        ESTestCase.indexSettings(IndexVersion.current(), 1, 1)
                            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name())
                    )
                    .build();
                return Map.of(new ShardId(new Index("id", "n/a"), 1), indexMetadata);
            }

            @Override
            public long count() {
                return totalDocs;
            }
        };
        var plannerSettings = new PlannerSettings(
            DataPartitioning.SHARD,
            ByteSizeValue.ofMb(1),
            rateBufferSize,
            10_000,
            ByteSizeValue.ofMb(1)
        );
        Configuration config = configuration(
            new QueryPragmas(
                Settings.builder().put(QueryPragmas.TASK_CONCURRENCY.getKey().toLowerCase(Locale.ROOT), taskConcurrency).build()
            )
        );
        var planner = new TestPlannerOptimizer(
            config,
            timeSeriesAnalyzer,
            new LogicalPlanOptimizer(new LogicalOptimizerContext(config, FoldContext.small(), TransportVersion.current()))
        );
        String filter1 = "";
        if (queryStartTs != null || randomBoolean()) {
            filter1 = "| WHERE @timestamp >= \"" + (queryStartTs != null ? queryStartTs : minTimestampFromData) + "\"";
        }
        String filter2 = "";
        if (queryEndTs != null || randomBoolean()) {
            filter2 = "| WHERE @timestamp <= \"" + (queryEndTs != null ? queryEndTs : maxTimestampFromData) + "\"";
        }
        String q = String.format(Locale.ROOT, """
            TS k8s
            %s
            %s
            | STATS max(rate(network.total_bytes_in)) BY cluster, BUCKET(@timestamp, 1 hour)
            | LIMIT 10
            """, filter1, filter2);
        PhysicalPlan plan = planner.physicalPlan(q, timeSeriesAnalyzer);
        plan = PlannerUtils.integrateEsFilterIntoFragment(plan, null);
        plan = planner.optimizedPlan(plan, plannerSettings, searchStats, new EsqlFlags(true));
        EsQueryExec esQuery = (EsQueryExec) plan.collectFirstChildren(EsQueryExec.class::isInstance).getFirst();
        List<QueryRange> actualRanges = new ArrayList<>();
        for (EsQueryExec.QueryBuilderAndTags qt : esQuery.queryBuilderAndTags()) {
            BoolQueryBuilder boolQuery = (BoolQueryBuilder) qt.query();
            List<QueryBuilder> filters = boolQuery.must();
            assertThat(filters, hasSize(2));
            assertThat(filters.get(0), instanceOf(ExistsQueryBuilder.class));
            SingleValueQuery.Builder singleValueQuery = as(filters.get(1), SingleValueQuery.Builder.class);
            RangeQueryBuilder r = as(singleValueQuery.next(), RangeQueryBuilder.class);
            assertThat(r.fieldName(), equalTo("@timestamp"));
            assertTrue(r.includeLower());
            assertFalse(r.includeUpper());
            actualRanges.add(new QueryRange(r.from().toString(), r.to().toString()));
        }
        assertThat(actualRanges, equalTo(expectedRanges));
    }

    record QueryRange(String fromInclusive, String toInclusive) {

    }

    public void testPartition() {
        runTest(
            4000_000L,
            "2023-10-20T12:15:03.360Z",
            "2023-10-20T12:59:02.250Z",
            null,
            null,
            ByteSizeValue.ofMb(75),
            12, // 8 cpus
            List.of(
                new QueryRange("2023-10-20T12:15:03.360Z", "2023-10-20T12:20:00.000Z"),
                new QueryRange("2023-10-20T12:20:00.000Z", "2023-10-20T12:25:00.000Z"),
                new QueryRange("2023-10-20T12:25:00.000Z", "2023-10-20T12:30:00.000Z"),
                new QueryRange("2023-10-20T12:30:00.000Z", "2023-10-20T12:35:00.000Z"),
                new QueryRange("2023-10-20T12:35:00.000Z", "2023-10-20T12:40:00.000Z"),
                new QueryRange("2023-10-20T12:40:00.000Z", "2023-10-20T12:45:00.000Z"),
                new QueryRange("2023-10-20T12:45:00.000Z", "2023-10-20T12:50:00.000Z"),
                new QueryRange("2023-10-20T12:50:00.000Z", "2023-10-20T12:55:00.000Z"),
                new QueryRange("2023-10-20T12:55:00.000Z", "2023-10-20T12:59:02.251Z")
            )
        );
        runTest(
            4000_000L,
            "2023-10-20T12:15:03.360Z",
            "2023-10-20T12:59:02.250Z",
            null,
            null,
            ByteSizeValue.ofMb(75),
            6, // 4 cpus
            List.of(
                new QueryRange("2023-10-20T12:15:03.360Z", "2023-10-20T12:20:00.000Z"),
                new QueryRange("2023-10-20T12:20:00.000Z", "2023-10-20T12:30:00.000Z"),
                new QueryRange("2023-10-20T12:30:00.000Z", "2023-10-20T12:40:00.000Z"),
                new QueryRange("2023-10-20T12:40:00.000Z", "2023-10-20T12:50:00.000Z"),
                new QueryRange("2023-10-20T12:50:00.000Z", "2023-10-20T12:59:02.251Z")
            )
        );

        runTest(
            4000_000L,
            "2023-10-20T12:15:03.360Z",
            "2023-10-20T12:59:02.250Z",
            null,
            null,
            ByteSizeValue.ofMb(75),
            3, // 2 cpus
            List.of(
                new QueryRange("2023-10-20T12:15:03.360Z", "2023-10-20T12:30:00.000Z"),
                new QueryRange("2023-10-20T12:30:00.000Z", "2023-10-20T12:59:02.251Z")
            )
        );

        runTest(
            4000_000L,
            "2023-10-20T12:15:03.360Z",
            "2023-10-20T12:59:02.250Z",
            null,
            null,
            ByteSizeValue.ofMb(30), // smaller buffer to more smaller slices
            3, // 2 cpus
            List.of(
                new QueryRange("2023-10-20T12:15:03.360Z", "2023-10-20T12:20:00.000Z"),
                new QueryRange("2023-10-20T12:20:00.000Z", "2023-10-20T12:40:00.000Z"),
                new QueryRange("2023-10-20T12:40:00.000Z", "2023-10-20T12:59:02.251Z")
            )
        );
        runTest(
            4000_000L,
            "2023-10-20T12:15:03.360Z",
            "2023-10-20T12:59:02.250Z",
            null,
            null,
            ByteSizeValue.ofMb(10), // smaller buffer to more smaller slices
            3, // 2 cpus
            List.of(
                new QueryRange("2023-10-20T12:15:03.360Z", "2023-10-20T12:20:00.000Z"),
                new QueryRange("2023-10-20T12:20:00.000Z", "2023-10-20T12:25:00.000Z"),
                new QueryRange("2023-10-20T12:25:00.000Z", "2023-10-20T12:30:00.000Z"),
                new QueryRange("2023-10-20T12:30:00.000Z", "2023-10-20T12:35:00.000Z"),
                new QueryRange("2023-10-20T12:35:00.000Z", "2023-10-20T12:40:00.000Z"),
                new QueryRange("2023-10-20T12:40:00.000Z", "2023-10-20T12:45:00.000Z"),
                new QueryRange("2023-10-20T12:45:00.000Z", "2023-10-20T12:50:00.000Z"),
                new QueryRange("2023-10-20T12:50:00.000Z", "2023-10-20T12:55:00.000Z"),
                new QueryRange("2023-10-20T12:55:00.000Z", "2023-10-20T12:59:02.251Z")
            )
        );
        runTest(
            1000_000L, // less docs
            "2023-10-20T12:15:03.360Z",
            "2023-10-20T12:59:02.250Z",
            null,
            null,
            ByteSizeValue.ofMb(10), // smaller buffer to more smaller slices
            3, // 2 cpus
            List.of(
                new QueryRange("2023-10-20T12:15:03.360Z", "2023-10-20T12:30:00.000Z"),
                new QueryRange("2023-10-20T12:30:00.000Z", "2023-10-20T12:59:02.251Z")
            )
        );
    }

    public void testPartitionWithQuery() {
        runTest(
            4000_000L,
            "2023-11-20T12:15:03.360Z",
            "2023-11-20T12:59:02.250Z",
            null,
            null,
            ByteSizeValue.ofMb(75),
            12, // 8 cpus
            List.of(
                new QueryRange("2023-11-20T12:15:03.360Z", "2023-11-20T12:20:00.000Z"),
                new QueryRange("2023-11-20T12:20:00.000Z", "2023-11-20T12:25:00.000Z"),
                new QueryRange("2023-11-20T12:25:00.000Z", "2023-11-20T12:30:00.000Z"),
                new QueryRange("2023-11-20T12:30:00.000Z", "2023-11-20T12:35:00.000Z"),
                new QueryRange("2023-11-20T12:35:00.000Z", "2023-11-20T12:40:00.000Z"),
                new QueryRange("2023-11-20T12:40:00.000Z", "2023-11-20T12:45:00.000Z"),
                new QueryRange("2023-11-20T12:45:00.000Z", "2023-11-20T12:50:00.000Z"),
                new QueryRange("2023-11-20T12:50:00.000Z", "2023-11-20T12:55:00.000Z"),
                new QueryRange("2023-11-20T12:55:00.000Z", "2023-11-20T12:59:02.251Z")
            )
        );

        runTest(
            4000_000L,
            "2023-11-20T12:15:03.360Z",
            "2023-11-20T12:59:02.250Z",
            "2023-11-20T12:16:03.360Z",
            "2023-11-20T12:57:02.250Z",
            ByteSizeValue.ofMb(75),
            12, // 8 cpus
            List.of(
                new QueryRange("2023-11-20T12:16:03.360Z", "2023-11-20T12:20:00.000Z"),
                new QueryRange("2023-11-20T12:20:00.000Z", "2023-11-20T12:25:00.000Z"),
                new QueryRange("2023-11-20T12:25:00.000Z", "2023-11-20T12:30:00.000Z"),
                new QueryRange("2023-11-20T12:30:00.000Z", "2023-11-20T12:35:00.000Z"),
                new QueryRange("2023-11-20T12:35:00.000Z", "2023-11-20T12:40:00.000Z"),
                new QueryRange("2023-11-20T12:40:00.000Z", "2023-11-20T12:45:00.000Z"),
                new QueryRange("2023-11-20T12:45:00.000Z", "2023-11-20T12:50:00.000Z"),
                new QueryRange("2023-11-20T12:50:00.000Z", "2023-11-20T12:55:00.000Z"),
                new QueryRange("2023-11-20T12:55:00.000Z", "2023-11-20T12:57:02.251Z")
            )
        );
    }
}
