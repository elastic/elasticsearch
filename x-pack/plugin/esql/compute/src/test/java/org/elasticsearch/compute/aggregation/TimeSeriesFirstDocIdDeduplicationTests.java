/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.TimeSeriesAggregationOperator;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.compute.test.operator.blocksource.AbstractBlockSourceOperator;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests the deduplication logic in {@link TimeSeriesAggregationOperator#selectedForDocIdsAggregator}.
 *
 * <p>When the same tsid appears in multiple time buckets, the {@code selectedForDocIdsAggregator} method
 * remaps group IDs so that all groups with the same tsid use the first group ID encountered for that tsid.
 * This ensures that when loading dimension fields, we only load the first doc ID for each tsid, even if
 * that tsid appears in multiple time buckets, preventing unnecessary document loading.
 *
 * <p>This test verifies that:
 * <ul>
 *   <li>When the same tsid appears in multiple time buckets with potentially different doc IDs,
 *       all groups with the same tsid use the first doc ID encountered for that tsid</li>
 *   <li>The deduplication logic correctly remaps group IDs based on tsid ordinals</li>
 * </ul>
 */
public class TimeSeriesFirstDocIdDeduplicationTests extends OperatorTestCase {
    static final Rounding.Prepared timeBucket = Rounding.builder(TimeValue.timeValueMinutes(1)).build().prepareForUnknown();
    static final int HASH_CHANNEL_COUNT = 2;

    private record Doc(int shard, int segment, int docId) {}

    private record RowData(BytesRef tsid, long timestamp, Doc doc) {}

    public void testDeduplicationWithMultipleTimeBuckets() {
        DriverContext driverContext = driverContext();
        int size = between(100, 1000);
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(driverContext.blockFactory(), size));
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());
        List<Page> results = new ArrayList<>();

        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(input.iterator()),
                List.of(simple().get(driverContext)),
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            new TestDriverRunner().run(d);
        }

        assertSimpleOutput(origInput, results);
        assertDriverContext(driverContext);

        for (Page page : results) {
            page.close();
        }
        for (Page page : origInput) {
            page.close();
        }
    }

    /**
     * The test creates a scenario with 2 different groups (different timestamps) but with
     * shard IDs that could trigger the bug. Position 0 has shard 0, and position 1 has shard 2.
     * When processing position 1, if shardRefCounted was called with shard ID 2 instead of
     * position 1, it would try to access position 2 in a vector with only 2 positions (0 and 1),
     * causing ArrayIndexOutOfBoundsException.
     */
    public void testOneGroupWithMultipleShards() {
        DriverContext driverContext = driverContext();
        final long START_TIME = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13");
        BytesRef tsid1 = new BytesRef("tsid1");
        BytesRef tsid2 = new BytesRef("tsid2");

        long timestamp1 = timeBucket.round(START_TIME);
        long timestamp2 = timeBucket.round(START_TIME + 2 * 60 * 1000);

        List<RowData> rows = new ArrayList<>();
        Map<Integer, RefCounted> shardRefs = new HashMap<>();

        // Position 0: shard 0, first tbucket
        rows.add(new RowData(tsid1, timestamp1, new Doc(0, 0, 0)));
        shardRefs.put(0, AbstractRefCounted.of(() -> {}));

        // Position 1: shard 2, second tbucket
        rows.add(new RowData(tsid2, timestamp2, new Doc(2, 0, 1)));
        shardRefs.put(2, AbstractRefCounted.of(() -> {}));

        SourceOperator source = new TimeSeriesDocIdSourceOperator(driverContext.blockFactory(), rows, shardRefs);
        List<Page> input = CannedSourceOperator.collectPages(source);
        List<Page> results = new ArrayList<>();

        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(input.iterator()),
                List.of(simple().get(driverContext)),
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            new TestDriverRunner().run(d);
        }

        assertThat("Should have at least one result page", results.size(), equalTo(1));
        Page resultPage = results.get(0);
        assertThat("Should have 2 groups (same tsid1, different timestamps)", resultPage.getPositionCount(), equalTo(2));

        DocBlock docBlock = resultPage.getBlock(2);
        DocVector docVector = docBlock.asVector();

        // First group should have shard 0
        assertThat("First group shard should be 0", docVector.shards().getInt(0), equalTo(0));
        assertThat("First group segment should be 0", docVector.segments().getInt(0), equalTo(0));
        assertThat("First group doc ID should be 0", docVector.docs().getInt(0), equalTo(0));

        // Second group should have shard 2
        assertThat("Second group shard should be 2", docVector.shards().getInt(1), equalTo(2));
        assertThat("Second group segment should be 0", docVector.segments().getInt(1), equalTo(0));
        assertThat("Second group doc ID should be 1", docVector.docs().getInt(1), equalTo(1));

        assertDriverContext(driverContext);

        for (Page page : results) {
            page.close();
        }
        for (Page page : input) {
            page.close();
        }
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        final long START_TIME = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13");
        List<BytesRef> tsids = List.of(new BytesRef("tsid1"), new BytesRef("tsid2"), new BytesRef("tsid3"));

        List<RowData> rows = new ArrayList<>();
        Map<Integer, RefCounted> shardRefs = new HashMap<>();

        for (int i = 0; i < size; i++) {
            BytesRef tsid = randomFrom(tsids);

            long tsOffset = randomLongBetween(0, 10 * 60 * 1000); // 0-10 minutes
            long ts = timeBucket.round(START_TIME + tsOffset);

            Doc doc = new Doc(between(0, 2), between(0, 5), randomNonNegativeInt());
            shardRefs.putIfAbsent(doc.shard, AbstractRefCounted.of(() -> {}));

            rows.add(new RowData(tsid, ts, doc));
        }

        return new TimeSeriesDocIdSourceOperator(blockFactory, rows, shardRefs);
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        Map<String, Doc> expectedFirstDocPerTsid = new LinkedHashMap<>();
        var scratch = new BytesRef();

        for (Page page : input) {
            BytesRefBlock tsids = page.getBlock(0);
            DocBlock docBlock = page.getBlock(2);
            DocVector docVector = docBlock.asVector();

            for (int p = 0; p < page.getPositionCount(); p++) {
                String tsid = tsids.getBytesRef(p, scratch).utf8ToString();
                Doc doc = new Doc(docVector.shards().getInt(p), docVector.segments().getInt(p), docVector.docs().getInt(p));
                expectedFirstDocPerTsid.putIfAbsent(tsid, doc);
            }
        }

        Map<String, Doc> actualDocPerTsid = new HashMap<>();
        for (Page page : results) {
            BytesRefBlock tsids = page.getBlock(0);
            LongBlock timestamps = page.getBlock(1);
            DocBlock docBlock = page.getBlock(2);
            DocVector docVector = docBlock.asVector();

            for (int p = 0; p < page.getPositionCount(); p++) {
                String tsid = tsids.getBytesRef(p, scratch).utf8ToString();
                long timestamp = timestamps.getLong(p);
                Doc doc = new Doc(docVector.shards().getInt(p), docVector.segments().getInt(p), docVector.docs().getInt(p));

                Doc existing = actualDocPerTsid.get(tsid);
                if (existing != null) {
                    assertThat(
                        "All groups with tsid "
                            + tsid
                            + " should use the same doc ID due to deduplication in selectedForDocIdsAggregator. "
                            + "Group at timestamp "
                            + timestamp
                            + " has doc "
                            + doc
                            + " but expected "
                            + existing,
                        doc,
                        equalTo(existing)
                    );
                } else {
                    actualDocPerTsid.put(tsid, doc);
                }

                Doc expectedFirstDoc = expectedFirstDocPerTsid.get(tsid);
                assertThat(
                    "Doc ID for tsid "
                        + tsid
                        + " at timestamp "
                        + timestamp
                        + " should match the first doc ID encountered for that tsid due to deduplication",
                    doc,
                    equalTo(expectedFirstDoc)
                );
            }
        }
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        AggregatorMode mode = AggregatorMode.SINGLE;
        return new TimeSeriesAggregationOperator.Factory(
            timeBucket,
            false,
            List.of(
                new BlockHash.GroupSpec(0, ElementType.BYTES_REF, null, null),
                new BlockHash.GroupSpec(1, ElementType.LONG, null, null)
            ),
            mode,
            List.of(aggregatorFunction().groupingAggregatorFactory(mode, channels())),
            randomPageSize()
        );

    }

    @Override
    protected final Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("TimeSeriesAggregationOperator[mode = <not-needed>, aggs = first_doc_id]");
    }

    @Override
    protected final Matcher<String> expectedToStringOfSimple() {
        String hash = "blockHash=BytesRefLongBlockHash{keys=[tsid[channel=0], timestamp[channel=1]], entries=0, size=%size%}".replace(
            "%size%",
            byteRefBlockHashSize()
        );
        return equalTo(
            "TimeSeriesAggregationOperator["
                + hash
                + ", aggregators=[GroupingAggregator[aggregatorFunction=FirstDocIdGroupingAggregatorFunction[channel=2], mode=SINGLE]]]"
        );
    }

    private AggregatorFunctionSupplier aggregatorFunction() {
        return new FirstDocIdGroupingAggregatorFunction.FunctionSupplier();
    }

    protected List<Integer> channels() {
        return List.of(HASH_CHANNEL_COUNT);
    }

    private static class TimeSeriesDocIdSourceOperator extends AbstractBlockSourceOperator {
        private final List<RowData> rows;
        private final Map<Integer, RefCounted> shardRefs;

        TimeSeriesDocIdSourceOperator(BlockFactory blockFactory, List<RowData> rows, Map<Integer, RefCounted> shardRefs) {
            super(blockFactory, 8 * 1024);
            this.rows = rows;
            this.shardRefs = shardRefs;
        }

        @Override
        protected Page createPage(int positionOffset, int length) {
            try (
                var tsidBuilder = blockFactory.newBytesRefBlockBuilder(length);
                var timestampBuilder = blockFactory.newLongBlockBuilder(length);
                var shardBuilder = blockFactory.newIntVectorFixedBuilder(length);
                var segmentBuilder = blockFactory.newIntVectorFixedBuilder(length);
                var docBuilder = blockFactory.newIntVectorFixedBuilder(length)
            ) {
                for (int i = 0; i < length; i++) {
                    RowData row = rows.get(positionOffset + i);
                    tsidBuilder.appendBytesRef(row.tsid);
                    timestampBuilder.appendLong(row.timestamp);
                    shardBuilder.appendInt(row.doc.shard);
                    segmentBuilder.appendInt(row.doc.segment);
                    docBuilder.appendInt(row.doc.docId);
                }

                var refs = new FirstDocIdGroupingAggregatorFunction.MappedShardRefs<>(shardRefs);
                DocVector docVector = new DocVector(
                    refs,
                    shardBuilder.build(),
                    segmentBuilder.build(),
                    docBuilder.build(),
                    DocVector.config()
                );

                currentPosition += length;
                return new Page(tsidBuilder.build(), timestampBuilder.build(), docVector.asBlock());
            }
        }

        @Override
        protected int remaining() {
            return rows.size() - currentPosition;
        }
    }
}
