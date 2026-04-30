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
import org.elasticsearch.index.mapper.DateFieldMapper;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests the tsid-contiguous emit with inline null-fill produced by
 * {@link TimeSeriesAggregationOperator} when {@code collapsed=true}.
 */
public class TimeSeriesCollapsedEmitTests extends ComputeTestCase {

    private static final Rounding.Prepared TIME_BUCKET = Rounding.builder(TimeValue.timeValueMinutes(1)).build().prepareForUnknown();
    private static final DateFieldMapper.Resolution MILLIS = DateFieldMapper.Resolution.MILLISECONDS;

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
     * When {@code outputTimeBucket} is coarser than {@code timeBucket}, only steps that are aligned
     * to the output bucket boundary must appear in the collapsed output. Non-aligned input-bucket
     * steps must be dropped, and output-bucket-aligned steps that have no data must be null-filled.
     */
    public void testOutputBucketFiltersNonAlignedSteps() {
        Rounding.Prepared fiveMinBucket = Rounding.builder(TimeValue.timeValueMinutes(5)).build().prepareForUnknown();
        // Use epoch-relative timestamps so that 0 and multiples of 5 minutes are trivially 5-min aligned.
        long min0 = TIME_BUCKET.round(0L);               // 0 ms — 5-min aligned
        long min1 = TIME_BUCKET.round(ONE_MINUTE);        // 60_000 ms — NOT 5-min aligned
        long min5 = TIME_BUCKET.round(5 * ONE_MINUTE);    // 300_000 ms — 5-min aligned
        long min10 = TIME_BUCKET.round(10 * ONE_MINUTE);  // 600_000 ms — 5-min aligned

        DriverContext driverContext = driverContext();
        TimeSeriesAggregationOperator op = createOperator(driverContext, List.of(sumLongFactory()), fiveMinBucket, false);

        // tsid1: data at min0, min1 (not 5-min aligned), and min10; min5 has no data -> null-fill
        feedInput(
            driverContext,
            op,
            List.of(new InputRow("tsid1", min0, 10), new InputRow("tsid1", min1, 20), new InputRow("tsid1", min10, 30))
        );
        op.finish();
        List<Row> result = drainRows(op, 0, 1, 2);

        // Range [min0, min10]. 5-min-aligned steps: min0, min5, min10.
        // min1 has data but is not 5-min aligned -> excluded.
        // min5 has no data -> null-fill.
        assertThat(result.size(), equalTo(3));
        assertThat(result.get(0), equalTo(new Row("tsid1", min0, 10L)));
        assertThat(result.get(1), equalTo(new Row("tsid1", min5, null)));
        assertThat(result.get(2), equalTo(new Row("tsid1", min10, 30L)));
    }

    /**
     * Verifies that null-fill rows propagate dimension values (e.g. cluster) from a representative
     * group of the same tsid, rather than emitting null. Without this fix, null-dimension rows
     * break the label-contiguity assumption of downstream operators like TimeSeriesCollapseOperator.
     */
    public void testSparseDimensionValuesPropagatedOnNullFill() {
        DriverContext driverContext = driverContext();
        var dimFactory = new DimensionValuesByteRefGroupingAggregatorFunction.FunctionSupplier().groupingAggregatorFactory(
            AggregatorMode.SINGLE,
            List.of(3)
        );
        TimeSeriesAggregationOperator op = createOperator(driverContext, List.of(sumLongFactory(), dimFactory), null, false);

        // Sparse: tsid1 has step(0) only, tsid2 has step(1) only — 4-channel input: tsid, timestamp, value, cluster
        BlockFactory blockFactory = driverContext.blockFactory();
        try (
            var tsidBuilder = blockFactory.newBytesRefBlockBuilder(2);
            var tsBuilder = blockFactory.newLongBlockBuilder(2);
            var valBuilder = blockFactory.newLongBlockBuilder(2);
            var clusterBuilder = blockFactory.newBytesRefBlockBuilder(2)
        ) {
            tsidBuilder.appendBytesRef(new BytesRef("tsid1"));
            tsBuilder.appendLong(step(0));
            valBuilder.appendLong(100);
            clusterBuilder.appendBytesRef(new BytesRef("clusterA"));

            tsidBuilder.appendBytesRef(new BytesRef("tsid2"));
            tsBuilder.appendLong(step(1));
            valBuilder.appendLong(200);
            clusterBuilder.appendBytesRef(new BytesRef("clusterB"));

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

    /**
     * Feeds input across multiple pages to exercise min/max timestamp tracking across addInput calls.
     */
    public void testMultiPageInput() {
        DriverContext driverContext = driverContext();
        TimeSeriesAggregationOperator op = createOperator(driverContext, List.of(sumLongFactory()), null, false);

        // Page 1: tsid1 at step(0)
        feedInput(driverContext, op, List.of(new InputRow("tsid1", step(0), 10)));
        // Page 2: tsid1 at step(2) — extends the time range
        feedInput(driverContext, op, List.of(new InputRow("tsid1", step(2), 30)));
        op.finish();
        List<Row> result = drainRows(op, 0, 1, 2);

        // Range [step(0), step(2)] = 3 steps; step(1) is null-fill
        assertThat(result.size(), equalTo(3));
        assertThat(result.get(0), equalTo(new Row("tsid1", step(0), 10L)));
        assertThat(result.get(1), equalTo(new Row("tsid1", step(1), null)));
        assertThat(result.get(2), equalTo(new Row("tsid1", step(2), 30L)));
    }

    /**
     * Tests that collapsed output works correctly when the block hash uses reverse output order
     * (timestamp first, tsid second in the key blocks).
     */
    public void testReverseOutput() {
        DriverContext driverContext = driverContext();
        TimeSeriesAggregationOperator op = createOperator(driverContext, List.of(sumLongFactory()), null, true);

        // Input channels: [timestamp, tsid, value] (reversed order to match reversed block hash)
        BlockFactory blockFactory = driverContext.blockFactory();
        try (
            var tsBuilder = blockFactory.newLongBlockBuilder(2);
            var tsidBuilder = blockFactory.newBytesRefBlockBuilder(2);
            var valBuilder = blockFactory.newLongBlockBuilder(2)
        ) {
            tsBuilder.appendLong(step(0));
            tsidBuilder.appendBytesRef(new BytesRef("tsid1"));
            valBuilder.appendLong(10);

            tsBuilder.appendLong(step(1));
            tsidBuilder.appendBytesRef(new BytesRef("tsid2"));
            valBuilder.appendLong(200);

            try (var page = new Page(tsBuilder.build(), tsidBuilder.build(), valBuilder.build())) {
                op.addInput(page);
            }
        }
        op.finish();

        // Output channels are [timestamp, tsid, sum] due to reverseOutput
        List<Row> result = drainRows(op, 1, 0, 2);

        // Both tsids cover [step(0), step(1)] = 2 steps each
        assertThat(result.size(), equalTo(4));
        assertThat(result.get(0), equalTo(new Row("tsid1", step(0), 10L)));
        assertThat(result.get(1), equalTo(new Row("tsid1", step(1), null)));
        assertThat(result.get(2), equalTo(new Row("tsid2", step(0), null)));
        assertThat(result.get(3), equalTo(new Row("tsid2", step(1), 200L)));
    }

    // ---- helpers ----

    private static GroupingAggregator.Factory sumLongFactory() {
        return new SumLongAggregatorFunctionSupplier(TestWarningsSource.INSTANCE).groupingAggregatorFactory(
            AggregatorMode.SINGLE,
            List.of(2)
        );
    }

    private DriverContext driverContext() {
        BlockFactory bf = blockFactory();
        return new DriverContext(bf.bigArrays(), bf, null, "test");
    }

    private static TimeSeriesAggregationOperator createOperator(
        DriverContext driverContext,
        List<GroupingAggregator.Factory> aggFactories,
        Rounding.Prepared outputTimeBucket,
        boolean reverseOutput
    ) {
        BlockFactory blockFactory = driverContext.blockFactory();
        int tsidChannel = reverseOutput ? 1 : 0;
        int timestampChannel = reverseOutput ? 0 : 1;
        return new TimeSeriesAggregationOperator(
            TIME_BUCKET,
            MILLIS,
            AggregatorMode.SINGLE,
            aggFactories,
            () -> new TimeSeriesBlockHash(tsidChannel, timestampChannel, reverseOutput, true, blockFactory),
            outputTimeBucket,
            true,
            driverContext
        );
    }

    /** Build an input page from the given rows (channels: tsid, timestamp, value) and feed it to the operator. */
    private static void feedInput(DriverContext driverContext, TimeSeriesAggregationOperator op, List<InputRow> inputRows) {
        BlockFactory blockFactory = driverContext.blockFactory();
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

    /** Drain output rows from the operator, reading tsid/timestamp/sum from the given channel indices. */
    private static List<Row> drainRows(TimeSeriesAggregationOperator op, int tsidChannel, int timestampChannel, int sumChannel) {
        List<Row> result = new ArrayList<>();
        Page page;
        while ((page = op.getOutput()) != null) {
            BytesRefBlock tsids = page.getBlock(tsidChannel);
            LongBlock timestamps = page.getBlock(timestampChannel);
            LongBlock sums = page.getBlock(sumChannel);
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
        DriverContext driverContext = driverContext();
        TimeSeriesAggregationOperator op = createOperator(driverContext, List.of(sumLongFactory()), null, false);
        feedInput(driverContext, op, inputRows);
        op.finish();
        return drainRows(op, 0, 1, 2);
    }
}
