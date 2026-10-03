/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.operator.LimitOperator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.AsyncConnectorSourceOperatorFactory;
import org.elasticsearch.xpack.esql.datasources.CoalescedSplit;
import org.elasticsearch.xpack.esql.datasources.ExternalSliceQueue;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.SplitStats;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.MvExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.DEFAULT_EXTERNAL_SOURCE_PAGE_SIZE_ROWS;
import static org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.canObserveExternalLimit;
import static org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.capInstanceCountByCoveringSplits;
import static org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.coveringSplitCount;
import static org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.externalSourceBufferSize;
import static org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.observeExternalLimit;

/**
 * Planner helpers for the shared node LIMIT budget: covering split count, n/k buffer, and
 * which physical chains may observe a downstream limiter.
 */
public class LocalExecutionPlannerLimitBudgetTests extends ESTestCase {

    public void testCoveringSplitCountFiveThousandRowSplits() {
        List<ExternalSplit> splits = splitsWithRowCounts(5000, 5000, 5000, 5000);
        assertEquals(2, coveringSplitCount(splits, 10_000));
        assertEquals(1, coveringSplitCount(splits, 5_000));
        assertEquals(4, coveringSplitCount(splits, 20_000));
    }

    public void testCoveringSplitCountTreatsCoalescedAsOneQueueItem() {
        ExternalSplit coalesced = new CoalescedSplit("file", List.of(splitWithRowCount("a", 5000), splitWithRowCount("b", 5000)));
        List<ExternalSplit> splits = List.of(coalesced, splitWithRowCount("c", 5000));
        assertEquals(1, coveringSplitCount(splits, 10_000));
    }

    public void testCapInstanceCountFailClosedOnMissingStats() {
        List<ExternalSplit> splits = new ArrayList<>();
        splits.add(splitWithRowCount("a", 5000));
        splits.add(new FileSplit("file", StoragePath.of("s3://bucket/b.parquet"), 0, 100, "parquet", Map.of(), Map.of()));
        splits.add(splitWithRowCount("c", 5000));
        assertEquals(5, capInstanceCountByCoveringSplits(5, 10_000, splits, Map.of()));
    }

    public void testCapInstanceCountSkipRowUnchanged() {
        List<ExternalSplit> splits = splitsWithRowCounts(5000, 5000, 5000, 5000, 5000);
        Map<String, Object> skipRow = Map.of(ErrorPolicy.CONFIG_ERROR_MODE, "skip_row");
        assertEquals(5, capInstanceCountByCoveringSplits(5, 10_000, splits, skipRow));
        assertEquals(2, capInstanceCountByCoveringSplits(5, 10_000, splits, Map.of()));
    }

    public void testExternalSourceBufferSizeAfterFinalInstanceCount() {
        assertEquals(2, externalSourceBufferSize(10_000, 10, DEFAULT_EXTERNAL_SOURCE_PAGE_SIZE_ROWS));
        assertEquals(2, externalSourceBufferSize(1_000, 1, DEFAULT_EXTERNAL_SOURCE_PAGE_SIZE_ROWS));
        assertEquals(10, externalSourceBufferSize(FormatReader.NO_LIMIT, 10, DEFAULT_EXTERNAL_SOURCE_PAGE_SIZE_ROWS));
        assertEquals(10, externalSourceBufferSize(100_000, 1, DEFAULT_EXTERNAL_SOURCE_PAGE_SIZE_ROWS));
    }

    public void testCanObserveExternalLimitFilterEvalProjectOnly() {
        FieldAttribute value = attr("value");
        ExternalSourceExec source = new ExternalSourceExec(
            Source.EMPTY,
            "file:///test.parquet",
            "file",
            List.of(value),
            Map.of(),
            Map.of(),
            null
        );
        FilterExec filter = new FilterExec(Source.EMPTY, source, Literal.TRUE);
        EvalExec eval = new EvalExec(Source.EMPTY, filter, List.of());
        ProjectExec project = new ProjectExec(Source.EMPTY, eval, List.of(value));
        assertTrue(canObserveExternalLimit(source));
        assertTrue(canObserveExternalLimit(filter));
        assertTrue(canObserveExternalLimit(eval));
        assertTrue(canObserveExternalLimit(project));

        MvExpandExec mvExpand = new MvExpandExec(Source.EMPTY, source, value, value);
        assertFalse(canObserveExternalLimit(mvExpand));
        assertFalse(canObserveExternalLimit(new FilterExec(Source.EMPTY, mvExpand, Literal.TRUE)));

        AggregateExec stats = new AggregateExec(Source.EMPTY, source, List.of(), List.of(), AggregatorMode.SINGLE, List.of(), null);
        assertFalse(canObserveExternalLimit(stats));
        assertFalse(canObserveExternalLimit(new ProjectExec(Source.EMPTY, stats, List.of(value))));
    }

    public void testObserveExternalLimitWiresConnectorFactory() {
        FieldAttribute value = attr("value");
        ExternalSourceExec source = new ExternalSourceExec(
            Source.EMPTY,
            "file:///test.parquet",
            "file",
            List.of(value),
            Map.of(),
            Map.of(),
            null
        );
        FilterExec filter = new FilterExec(Source.EMPTY, source, Literal.TRUE);
        MvExpandExec mvExpand = new MvExpandExec(Source.EMPTY, source, value, value);
        LimitOperator.Factory limitFactory = new LimitOperator.Factory(7);

        AsyncConnectorSourceOperatorFactory wired = connectorFactory();
        observeExternalLimit(filter, wired, limitFactory);
        assertSame(limitFactory.limiter(), wired.observedLimiter());

        AsyncConnectorSourceOperatorFactory skipped = connectorFactory();
        observeExternalLimit(mvExpand, skipped, limitFactory);
        assertNull(skipped.observedLimiter());
    }

    public void testCapInstanceCountCoalescedChildMissingStatsFailClosed() {
        ExternalSplit coalesced = new CoalescedSplit(
            "file",
            List.of(
                splitWithRowCount("a", 5000),
                new FileSplit("file", StoragePath.of("s3://bucket/b.parquet"), 0, 100, "parquet", Map.of(), Map.of())
            )
        );
        List<ExternalSplit> splits = List.of(coalesced, splitWithRowCount("c", 5000));
        assertEquals(5, capInstanceCountByCoveringSplits(5, 10_000, splits, Map.of()));
    }

    private static List<ExternalSplit> splitsWithRowCounts(int... rowCounts) {
        List<ExternalSplit> splits = new ArrayList<>(rowCounts.length);
        for (int i = 0; i < rowCounts.length; i++) {
            splits.add(splitWithRowCount("f" + i, rowCounts[i]));
        }
        return splits;
    }

    private static FileSplit splitWithRowCount(String name, int rowCount) {
        SplitStats stats = new SplitStats.Builder().rowCount(rowCount).build();
        return FileSplit.withSplitStats(
            "file",
            StoragePath.of("s3://bucket/" + name + ".parquet"),
            0,
            100,
            "parquet",
            Map.of(),
            Map.of(),
            null,
            stats
        );
    }

    private static FieldAttribute attr(String name) {
        return new FieldAttribute(
            Source.EMPTY,
            name,
            new EsField(name, DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private static AsyncConnectorSourceOperatorFactory connectorFactory() {
        Connector connector = new Connector() {
            @Override
            public ResultCursor execute(QueryRequest request, Split split) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {}
        };
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
        QueryRequest request = new QueryRequest("target", List.of("value"), List.of(), Map.of(), 100, blockFactory);
        return new AsyncConnectorSourceOperatorFactory(connector, request, 10, Runnable::run, new ExternalSliceQueue(List.of()));
    }
}
