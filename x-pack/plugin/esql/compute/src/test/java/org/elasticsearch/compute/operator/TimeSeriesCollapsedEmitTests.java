/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.DimensionValuesByteRefGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.blockhash.TimeSeriesBlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.core.TimeValue;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests the tsid-contiguous emit with inline null-fill produced by
 * {@link TimeSeriesAggregationOperator} when {@code collapsed=true}.
 */
public class TimeSeriesCollapsedEmitTests extends ComputeTestCase {

    private static final Rounding.Prepared TIME_BUCKET = Rounding.builder(TimeValue.timeValueMinutes(1)).build().prepareForUnknown();

    private static final long BASE_TIME = 1_000_000L;
    private static final long ONE_MINUTE = 60_000L;

    private static long step(int i) {
        return TIME_BUCKET.round(BASE_TIME + i * ONE_MINUTE);
    }

    record InputRow(String tsid, long timestamp, long value) {}

    record Row(String tsid, long timestamp, Long sum) {}

    // ---- tests ----

    public void testDense() {
        List<InputRow> input = List.of(
            new InputRow("tsid1", step(0), 10),
            new InputRow("tsid1", step(1), 20),
            new InputRow("tsid1", step(2), 30),
            new InputRow("tsid2", step(0), 100),
            new InputRow("tsid2", step(1), 200),
            new InputRow("tsid2", step(2), 300)
        );

        List<Row> output = runCollapsed(input);

        assertThat(output.size(), equalTo(6));
        assertThat(output.get(0), equalTo(new Row("tsid1", step(0), 10L)));
        assertThat(output.get(1), equalTo(new Row("tsid1", step(1), 20L)));
        assertThat(output.get(2), equalTo(new Row("tsid1", step(2), 30L)));
        assertThat(output.get(3), equalTo(new Row("tsid2", step(0), 100L)));
        assertThat(output.get(4), equalTo(new Row("tsid2", step(1), 200L)));
        assertThat(output.get(5), equalTo(new Row("tsid2", step(2), 300L)));
    }

    public void testSparse() {
        // tsid1 has steps 0 and 2 (missing step 1); tsid2 has steps 1 and 2 (missing step 0)
        List<InputRow> input = List.of(
            new InputRow("tsid1", step(0), 10),
            new InputRow("tsid1", step(2), 30),
            new InputRow("tsid2", step(1), 200),
            new InputRow("tsid2", step(2), 300)
        );

        List<Row> output = runCollapsed(input);

        // Each tsid covers the full range [step(0), step(2)] = 3 steps -> 6 rows total
        assertThat(output.size(), equalTo(6));
        assertThat(output.get(0), equalTo(new Row("tsid1", step(0), 10L)));
        assertThat(output.get(1), equalTo(new Row("tsid1", step(1), null)));  // null-fill
        assertThat(output.get(2), equalTo(new Row("tsid1", step(2), 30L)));
        assertThat(output.get(3), equalTo(new Row("tsid2", step(0), null))); // null-fill
        assertThat(output.get(4), equalTo(new Row("tsid2", step(1), 200L)));
        assertThat(output.get(5), equalTo(new Row("tsid2", step(2), 300L)));
    }

    public void testSingleTsidSingleStep() {
        List<Row> output = runCollapsed(List.of(new InputRow("tsid1", step(0), 42)));

        assertThat(output.size(), equalTo(1));
        assertThat(output.get(0), equalTo(new Row("tsid1", step(0), 42L)));
    }

    public void testEmpty() {
        assertThat(runCollapsed(List.of()).size(), equalTo(0));
    }

    public void testStepsOrderedChronologically() {
        List<InputRow> input = List.of(
            new InputRow("tsid1", step(0), 10),
            new InputRow("tsid1", step(2), 30),
            new InputRow("tsid1", step(1), 20)
        );

        List<Row> output = runCollapsed(input);

        assertThat(output.size(), equalTo(3));
        assertThat(output.get(0), equalTo(new Row("tsid1", step(0), 10L)));
        assertThat(output.get(1), equalTo(new Row("tsid1", step(1), 20L)));
        assertThat(output.get(2), equalTo(new Row("tsid1", step(2), 30L)));
    }

    /**
     * Verifies that null-fill rows propagate dimension values (e.g. cluster) from a representative
     * group of the same tsid, rather than emitting null. Without this fix, null-dimension rows
     * break the label-contiguity assumption of downstream operators like TimeSeriesCollapseOperator.
     */
    public void testSparseDimensionValuesPropagatedOnNullFill() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null, "test");

        // 4-channel input: tsid, timestamp, value (long), cluster (bytes_ref)
        var sumFactory = new SumLongAggregatorFunctionSupplier(TestWarningsSource.INSTANCE).groupingAggregatorFactory(
            AggregatorMode.SINGLE,
            List.of(2)
        );
        var dimFactory = new DimensionValuesByteRefGroupingAggregatorFunction.FunctionSupplier().groupingAggregatorFactory(
            AggregatorMode.SINGLE,
            List.of(3)
        );
        TimeSeriesAggregationOperator op = createOperator(
            AggregatorMode.SINGLE,
            List.of(sumFactory, dimFactory),
            blockFactory,
            driverContext
        );

        // Sparse: tsid1 has step(0) only, tsid2 has step(1) only
        BytesRef clusterA = new BytesRef("clusterA");
        BytesRef clusterB = new BytesRef("clusterB");
        try (
            var tsidBuilder = blockFactory.newBytesRefBlockBuilder(2);
            var tsBuilder = blockFactory.newLongBlockBuilder(2);
            var valBuilder = blockFactory.newLongBlockBuilder(2);
            var clusterBuilder = blockFactory.newBytesRefBlockBuilder(2)
        ) {
            tsidBuilder.appendBytesRef(new BytesRef("tsid1"));
            tsBuilder.appendLong(step(0));
            valBuilder.appendLong(100);
            clusterBuilder.appendBytesRef(clusterA);

            tsidBuilder.appendBytesRef(new BytesRef("tsid2"));
            tsBuilder.appendLong(step(1));
            valBuilder.appendLong(200);
            clusterBuilder.appendBytesRef(clusterB);

            try (var page = new Page(tsidBuilder.build(), tsBuilder.build(), valBuilder.build(), clusterBuilder.build())) {
                op.addInput(page);
            }
        }
        op.finish();

        // Read output: channels 0=tsid, 1=step, 2=sum, 3=cluster
        record DimRow(String tsid, long timestamp, Long sum, String cluster) {}
        List<DimRow> result = new ArrayList<>();
        Page page;
        while ((page = op.getOutput()) != null) {
            BytesRefBlock tsids = page.getBlock(0);
            LongBlock timestamps = page.getBlock(1);
            LongBlock sums = page.getBlock(2);
            BytesRefBlock clusters = page.getBlock(3);
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < page.getPositionCount(); p++) {
                String tsid = tsids.getBytesRef(tsids.getFirstValueIndex(p), scratch).utf8ToString();
                long ts = timestamps.getLong(timestamps.getFirstValueIndex(p));
                Long sum = sums.isNull(p) ? null : sums.getLong(sums.getFirstValueIndex(p));
                String cluster = clusters.isNull(p) ? null : clusters.getBytesRef(clusters.getFirstValueIndex(p), scratch).utf8ToString();
                result.add(new DimRow(tsid, ts, sum, cluster));
            }
            page.releaseBlocks();
        }
        op.close();

        assertThat(result.size(), equalTo(4));
        // tsid1: step(0) has data, step(1) is null-fill — cluster must be propagated
        assertThat(result.get(0), equalTo(new DimRow("tsid1", step(0), 100L, "clusterA")));
        assertThat(result.get(1), equalTo(new DimRow("tsid1", step(1), null, "clusterA")));
        // tsid2: step(0) is null-fill, step(1) has data — cluster must be propagated
        assertThat(result.get(2), equalTo(new DimRow("tsid2", step(0), null, "clusterB")));
        assertThat(result.get(3), equalTo(new DimRow("tsid2", step(1), 200L, "clusterB")));
    }

    // ---- helpers ----

    private TimeSeriesAggregationOperator createOperator(
        AggregatorMode mode,
        List<GroupingAggregator.Factory> aggFactories,
        BlockFactory blockFactory,
        DriverContext driverContext
    ) {
        return new TimeSeriesAggregationOperator(
            TIME_BUCKET,
            org.elasticsearch.index.mapper.DateFieldMapper.Resolution.MILLISECONDS,
            mode,
            aggFactories,
            () -> new TimeSeriesBlockHash(0, 1, false, true, blockFactory),
            null,
            true,
            driverContext
        );
    }

    private TimeSeriesAggregationOperator createSingleModeOperator(BlockFactory blockFactory, DriverContext driverContext) {
        return createOperator(
            AggregatorMode.SINGLE,
            List.of(
                new SumLongAggregatorFunctionSupplier(TestWarningsSource.INSTANCE).groupingAggregatorFactory(
                    AggregatorMode.SINGLE,
                    List.of(2)
                )
            ),
            blockFactory,
            driverContext
        );
    }

    /** Build an input page from the given rows (channels: tsid, timestamp, value) and feed it to the operator. */
    private void feedInput(TimeSeriesAggregationOperator op, BlockFactory blockFactory, List<InputRow> inputRows) {
        try (
            var tsidBuilder = blockFactory.newBytesRefBlockBuilder(inputRows.size());
            var tsBuilder = blockFactory.newLongBlockBuilder(inputRows.size());
            var valBuilder = blockFactory.newLongBlockBuilder(inputRows.size())
        ) {
            for (InputRow row : inputRows) {
                tsidBuilder.appendBytesRef(new BytesRef(row.tsid()));
                tsBuilder.appendLong(row.timestamp());
                valBuilder.appendLong(row.value());
            }
            try (var page = new Page(tsidBuilder.build(), tsBuilder.build(), valBuilder.build())) {
                op.addInput(page);
            }
        }
    }

    /** Drain all expanded (flat) output rows from the operator. Channels: 0=tsid, 1=timestamp, 2=sum. */
    private List<Row> drainExpandedRows(TimeSeriesAggregationOperator op) {
        List<Row> result = new ArrayList<>();
        Page page;
        while ((page = op.getOutput()) != null) {
            BytesRefBlock tsids = page.getBlock(0);
            LongBlock timestamps = page.getBlock(1);
            LongBlock sums = page.getBlock(2);
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < page.getPositionCount(); p++) {
                String tsid = tsids.getBytesRef(tsids.getFirstValueIndex(p), scratch).utf8ToString();
                long ts = timestamps.getLong(timestamps.getFirstValueIndex(p));
                Long sum = sums.isNull(p) ? null : sums.getLong(sums.getFirstValueIndex(p));
                result.add(new Row(tsid, ts, sum));
            }
            page.releaseBlocks();
        }
        op.close();
        return result;
    }

    /** Run a collapsed operator against the given input rows and return all expanded output rows. */
    private List<Row> runCollapsed(List<InputRow> inputRows) {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null, "test");
        TimeSeriesAggregationOperator op = createSingleModeOperator(blockFactory, driverContext);
        feedInput(op, blockFactory, inputRows);
        op.finish();
        return drainExpandedRows(op);
    }
}
