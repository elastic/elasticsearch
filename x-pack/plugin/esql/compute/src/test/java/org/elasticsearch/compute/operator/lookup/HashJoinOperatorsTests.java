/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.MvExpandOperator;
import org.elasticsearch.compute.operator.ProjectOperator;
import org.elasticsearch.compute.operator.WarningSourceLocation;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class HashJoinOperatorsTests extends ComputeTestCase {
    private static final WarningSourceLocation WARNING_SOURCE = new WarningSourceLocation() {
        @Override
        public int lineNumber() {
            return 1;
        }

        @Override
        public int columnNumber() {
            return 1;
        }

        @Override
        public String viewName() {
            return null;
        }

        @Override
        public String text() {
            return "join_key";
        }
    };

    public void testDuplicateNullAndMultiValueKeys() {
        List<BuildRow> buildRows = List.of(
            new BuildRow(List.of(1L), 10),
            new BuildRow(List.of(1L), 11),
            new BuildRow(List.of(2L), 20),
            new BuildRow(null, 90),
            new BuildRow(List.of(2L, 3L), 91),
            new BuildRow(List.of(3L), 30)
        );
        List<ProbeRow> probeRows = List.of(
            new ProbeRow(List.of(1L), 100),
            new ProbeRow(List.of(2L), 101),
            new ProbeRow(List.of(1L), 102),
            new ProbeRow(null, 103),
            new ProbeRow(List.of(1L, 2L), 104),
            new ProbeRow(List.of(3L), 105),
            new ProbeRow(List.of(9L), 106)
        );

        JoinResult result = runJoin(buildRows, probeRows);

        assertThat(sorted(result.rows()), equalTo(sorted(nestedLoopJoin(buildRows, probeRows))));
        assertThat(result.buildWarnings(), hasItem(containsString("LOOKUP JOIN encountered multi-value")));
        assertThat(result.probeWarnings(), hasItem(containsString("LOOKUP JOIN encountered multi-value")));
    }

    public void testEmptyBuildAndProbeInputs() {
        List<ProbeRow> probeRows = List.of(new ProbeRow(List.of(1L), 1), new ProbeRow(null, 2), new ProbeRow(List.of(1L, 2L), 3));

        JoinResult emptyBuild = runJoin(List.of(), probeRows);
        assertThat(sorted(emptyBuild.rows()), equalTo(sorted(nestedLoopJoin(List.of(), probeRows))));
        assertThat(emptyBuild.buildWarnings(), equalTo(List.of()));

        JoinResult emptyProbe = runJoin(List.of(new BuildRow(List.of(1L), 10)), List.of());
        assertThat(emptyProbe.rows(), equalTo(List.of()));
        assertThat(emptyProbe.buildWarnings(), equalTo(List.of()));
        assertThat(emptyProbe.probeWarnings(), equalTo(List.of()));
    }

    public void testBuildSourceFailureFailsProbe() {
        Exception failure = expectThrows(
            Exception.class,
            () -> runJoin(List.of(), List.of(new ProbeRow(List.of(1L), 1)), new IllegalArgumentException("build source failed"))
        );

        assertThat(causeMessages(failure), hasItem("build source failed"));
    }

    private JoinResult runJoin(List<BuildRow> buildRows, List<ProbeRow> probeRows) {
        return runJoin(buildRows, probeRows, null);
    }

    private JoinResult runJoin(List<BuildRow> buildRows, List<ProbeRow> probeRows, RuntimeException buildSourceFailure) {
        BlockFactory buildBlockFactory = blockFactory();
        BlockFactory probeBlockFactory = blockFactory();
        DriverContext buildContext = new DriverContext(buildBlockFactory.bigArrays(), buildBlockFactory, null);
        DriverContext probeContext = new DriverContext(probeBlockFactory.bigArrays(), probeBlockFactory, null);
        HashJoinBridge bridge = new HashJoinBridge();
        List<Page> outputPages = new ArrayList<>();

        CannedSourceOperator buildSource;
        if (buildSourceFailure == null) {
            List<Page> buildPages = buildRows.isEmpty() ? List.of() : List.of(buildPage(buildBlockFactory, buildRows));
            buildSource = new CannedSourceOperator(buildPages.iterator());
        } else {
            buildSource = new CannedSourceOperator(List.<Page>of().iterator()) {
                @Override
                public boolean isFinished() {
                    return false;
                }

                @Override
                public Page getOutput() {
                    throw buildSourceFailure;
                }
            };
        }
        List<Page> probePages = probeRows.isEmpty() ? List.of() : List.of(probePage(probeBlockFactory, probeRows));
        Driver buildDriver = TestDriverFactory.create(
            buildContext,
            buildSource,
            List.of(),
            new HashJoinBuildOperator.Factory(
                bridge,
                new int[] { 0 },
                new ElementType[] { ElementType.LONG },
                new int[] { 1, 2 },
                new ElementType[] { ElementType.LONG, ElementType.NULL },
                WARNING_SOURCE
            ).get(buildContext)
        );
        Driver probeDriver = TestDriverFactory.create(
            probeContext,
            new CannedSourceOperator(probePages.iterator()),
            List.of(
                new HashJoinProbeOperator.Factory(bridge, new int[] { 0 }, WARNING_SOURCE).get(probeContext),
                new MvExpandOperator.Factory(2, 5000).get(probeContext),
                new HashJoinColumnLoadOperator.Factory(bridge, 0, 2).get(probeContext),
                new HashJoinColumnLoadOperator.Factory(bridge, 1, 2).get(probeContext),
                new ProjectOperator.ProjectOperatorFactory(List.of(0, 1, 3, 4)).get(probeContext)
            ),
            new TestResultPageSinkOperator(outputPages::add)
        );

        try (buildDriver; probeDriver) {
            new TestDriverRunner().numThreads(2).run(List.of(buildDriver, probeDriver));
            return new JoinResult(readRows(outputPages), buildContext.warnings(), probeContext.warnings());
        } finally {
            Releasables.closeExpectNoException(
                Releasables.wrap(outputPages.stream().<Releasable>map(page -> page::releaseBlocks).toList())
            );
        }
    }

    private static Page buildPage(BlockFactory blockFactory, List<BuildRow> rows) {
        List<List<Long>> keys = new ArrayList<>(rows.size());
        List<List<Long>> values = new ArrayList<>(rows.size());
        for (BuildRow row : rows) {
            keys.add(row.key());
            values.add(List.of(row.value()));
        }

        Block keyBlock = null;
        Block valueBlock = null;
        Block nullBlock = null;
        try {
            keyBlock = longBlock(blockFactory, keys);
            valueBlock = longBlock(blockFactory, values);
            nullBlock = blockFactory.newConstantNullBlock(rows.size());
            Page page = new Page(keyBlock, valueBlock, nullBlock);
            keyBlock = null;
            valueBlock = null;
            nullBlock = null;
            return page;
        } finally {
            Releasables.closeExpectNoException(keyBlock, valueBlock, nullBlock);
        }
    }

    private static Page probePage(BlockFactory blockFactory, List<ProbeRow> rows) {
        List<List<Long>> keys = new ArrayList<>(rows.size());
        for (ProbeRow row : rows) {
            keys.add(row.key());
        }

        Block keyBlock = null;
        IntBlock valueBlock = null;
        try {
            keyBlock = longBlock(blockFactory, keys);
            try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(rows.size())) {
                for (ProbeRow row : rows) {
                    builder.appendInt(row.value());
                }
                valueBlock = builder.build();
            }
            Page page = new Page(keyBlock, valueBlock);
            keyBlock = null;
            valueBlock = null;
            return page;
        } finally {
            Releasables.closeExpectNoException(keyBlock, valueBlock);
        }
    }

    private static LongBlock longBlock(BlockFactory blockFactory, List<List<Long>> positions) {
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(positions.size())) {
            for (List<Long> values : positions) {
                if (values == null || values.isEmpty()) {
                    builder.appendNull();
                } else if (values.size() == 1) {
                    builder.appendLong(values.get(0));
                } else {
                    builder.beginPositionEntry();
                    for (long value : values) {
                        builder.appendLong(value);
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    private static List<JoinRow> nestedLoopJoin(List<BuildRow> buildRows, List<ProbeRow> probeRows) {
        List<JoinRow> result = new ArrayList<>();
        for (ProbeRow probe : probeRows) {
            boolean matched = false;
            if (probe.key() != null && probe.key().size() == 1) {
                for (BuildRow build : buildRows) {
                    if (build.key() != null && build.key().size() == 1 && probe.key().get(0).equals(build.key().get(0))) {
                        result.add(new JoinRow(probe.key(), probe.value(), build.value(), true));
                        matched = true;
                    }
                }
            }
            if (matched == false) {
                result.add(new JoinRow(probe.key(), probe.value(), null, true));
            }
        }
        return result;
    }

    private static List<JoinRow> readRows(List<Page> pages) {
        List<JoinRow> result = new ArrayList<>();
        for (Page page : pages) {
            LongBlock keys = page.getBlock(0);
            IntBlock leftValues = page.getBlock(1);
            LongBlock rightValues = page.getBlock(2);
            Block nullValues = page.getBlock(3);
            for (int position = 0; position < page.getPositionCount(); position++) {
                int keyValueCount = keys.getValueCount(position);
                List<Long> key = null;
                if (keyValueCount > 0) {
                    key = new ArrayList<>(keyValueCount);
                    int firstKeyValue = keys.getFirstValueIndex(position);
                    for (int i = 0; i < keyValueCount; i++) {
                        key.add(keys.getLong(firstKeyValue + i));
                    }
                    key = List.copyOf(key);
                }
                int valueCount = rightValues.getValueCount(position);
                Long rightValue = valueCount == 0 ? null : rightValues.getLong(rightValues.getFirstValueIndex(position));
                result.add(
                    new JoinRow(key, leftValues.getInt(leftValues.getFirstValueIndex(position)), rightValue, nullValues.isNull(position))
                );
            }
        }
        return result;
    }

    private static List<JoinRow> sorted(List<JoinRow> rows) {
        return rows.stream().sorted(Comparator.comparing(JoinRow::toString)).toList();
    }

    private static List<String> causeMessages(Throwable failure) {
        List<String> messages = new ArrayList<>();
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            messages.add(cause.getMessage());
        }
        return messages;
    }

    private record BuildRow(List<Long> key, long value) {}

    private record ProbeRow(List<Long> key, int value) {}

    private record JoinRow(List<Long> key, int leftValue, Long rightValue, boolean nullPayloadIsNull) {}

    private record JoinResult(List<JoinRow> rows, List<String> buildWarnings, List<String> probeWarnings) {}
}
