/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.elasticsearch.codec.profiler.ProfileCollector;
import org.elasticsearch.codec.profiler.TestCollectors;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class ProfiledBlockDecoderTests extends ESTestCase {

    private record Call(DataInput in, int valueCount, long[] out) {}

    public void testTheDecoratorForwardsTheCallUnchangedAndCountsTheValues() throws IOException {
        final List<Call> calls = new ArrayList<>();
        final NumericBlockDecoder delegate = (in, valueCount, out) -> calls.add(new Call(in, valueCount, out));
        final ProfileCollector stats = TestCollectors.countersOnly();
        final ProfiledBlockDecoder profiled = new ProfiledBlockDecoder(delegate, stats);
        final DataInput in = new ByteArrayDataInput(new byte[0]);
        final long[] out = new long[128];
        profiled.decode(in, 128, out);
        profiled.decode(in, 7, out);
        assertThat(calls.size(), equalTo(2));
        assertThat(calls.get(0).in(), sameInstance(in));
        assertThat(calls.get(0).out(), sameInstance(out));
        assertThat(calls.get(1).valueCount(), equalTo(7));
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()), equalTo(Map.of("ColumNAR.blocks.values_decoded", 135L)));
    }

    public void testAReaderWithoutARecorderDecodesWithThePipelineItself() {
        final int blockSize = randomValidBlockSize();
        final NumericPipeline pipeline = randomFrom(
            NumericPipeline.defaultPipeline(blockSize),
            NumericPipeline.runsAndOutliersPipeline(blockSize),
            NumericPipeline.compressedOrdinalPipeline(blockSize)
        );
        assertThat(NumericBlockDecoder.forReading(pipeline, blockSize, null), instanceOf(NumericBlockEncoder.class));
        assertThat(
            NumericBlockDecoder.forReading(pipeline, blockSize, TestCollectors.countersOnly()),
            instanceOf(ProfiledBlockDecoder.class)
        );
    }

    public void testADecodeThatFailsIsTimedAndNotCounted() {
        final IOException failure = new IOException("corrupt block");
        final NumericBlockDecoder failing = (in, valueCount, out) -> { throw failure; };
        final long step = between(2, 50);
        final long[] now = { 0 };
        final ProfileCollector stats = TestCollectors.withClock(() -> now[0] += step);
        final ProfiledBlockDecoder profiled = new ProfiledBlockDecoder(failing, stats);
        final IOException thrown = expectThrows(
            IOException.class,
            () -> profiled.decode(new ByteArrayDataInput(new byte[0]), 128, new long[128])
        );
        assertThat(thrown, sameInstance(failure));
        assertThat(
            "the timer stops on a failure and the count stays at zero",
            TestCollectors.toKeyedValues(stats.snapshot()),
            equalTo(Map.of("ColumNAR.blocks.values_decoded", 0L, "ColumNAR.blocks.decode_time_nanos", step))
        );
    }

    public void testACountersOnlyCollectorRegistersTheCounterAndNoTimer() throws IOException {
        final ProfileCollector stats = TestCollectors.countersOnly();
        new ProfiledBlockDecoder((in, valueCount, out) -> {}, stats);
        assertThat(
            "the counter registers when the decoder is built",
            TestCollectors.toKeyedValues(stats.snapshot()),
            hasKey("ColumNAR.blocks.values_decoded")
        );
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.blocks.values_decoded"), equalTo(0L));
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()), not(hasKey("ColumNAR.blocks.decode_time_nanos")));
    }

    public void testTheCpuCompanionRegistersOnlyWhenTheCollectorMeasuresThreadCpuTime() {
        final ProfileCollector elapsedOnly = TestCollectors.elapsed();
        new ProfiledBlockDecoder((in, valueCount, out) -> {}, elapsedOnly);
        assertThat(TestCollectors.toKeyedValues(elapsedOnly.snapshot()), hasKey("ColumNAR.blocks.decode_time_nanos"));
        assertThat(TestCollectors.toKeyedValues(elapsedOnly.snapshot()), not(hasKey("ColumNAR.blocks.decode_cpu_time_nanos")));

        final ProfileCollector cpu = TestCollectors.withClocks(() -> 0, () -> 0);
        new ProfiledBlockDecoder((in, valueCount, out) -> {}, cpu);
        assertThat(
            "the companion registers when the decoder is built",
            TestCollectors.toKeyedValues(cpu.snapshot()),
            hasKey("ColumNAR.blocks.decode_cpu_time_nanos")
        );
        assertThat(TestCollectors.toKeyedValues(cpu.snapshot()).get("ColumNAR.blocks.decode_cpu_time_nanos"), equalTo(0L));
    }
}
