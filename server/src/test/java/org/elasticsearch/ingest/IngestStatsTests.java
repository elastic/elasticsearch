/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class IngestStatsTests extends ESTestCase {

    public void testSerialization() throws IOException {
        IngestStats.Stats totalStats = new IngestStats.Stats(50, 100, 200, 300);
        List<IngestStats.PipelineStat> pipelineStats = createPipelineStats();
        Map<String, List<IngestStats.ProcessorStat>> processorStats = createProcessorStats(pipelineStats);
        IngestStats ingestStats = new IngestStats(totalStats, pipelineStats, processorStats);
        IngestStats serializedStats = serialize(ingestStats);
        assertIngestStats(ingestStats, serializedStats);
    }

    public void testIdentitySerialization() throws IOException {
        IngestStats serializedStats = serialize(IngestStats.IDENTITY);
        assertThat(serializedStats, sameInstance(IngestStats.IDENTITY));
    }

    public void testProcessorNameAndTypeIdentitySerialization() throws IOException {
        IngestStats.Builder builder = new IngestStats.Builder();
        builder.addPipelineMetrics("pipeline_id", new IngestPipelineMetric());
        builder.addProcessorMetrics("pipeline_id", "set", "set", new IngestMetric());
        builder.addProcessorMetrics("pipeline_id", "set:foo", "set", new IngestMetric());
        builder.addProcessorMetrics("pipeline_id", "set:bar", "set", new IngestMetric());
        builder.addTotalMetrics(new IngestMetric());

        IngestStats serializedStats = serialize(builder.build());
        List<IngestStats.ProcessorStat> processorStats = serializedStats.processorStats().get("pipeline_id");

        // these are just table stakes
        assertThat(processorStats.get(0).name(), is("set"));
        assertThat(processorStats.get(0).type(), is("set"));
        assertThat(processorStats.get(1).name(), is("set:foo"));
        assertThat(processorStats.get(1).type(), is("set"));
        assertThat(processorStats.get(2).name(), is("set:bar"));
        assertThat(processorStats.get(2).type(), is("set"));

        // this is actually interesting, though -- we're canonical-izing these strings to keep our heap usage under control
        final String set = processorStats.get(0).name();
        assertThat(processorStats.get(0).name(), sameInstance(set));
        assertThat(processorStats.get(0).type(), sameInstance(set));
        assertThat(processorStats.get(1).type(), sameInstance(set));
        assertThat(processorStats.get(2).type(), sameInstance(set));
    }

    public void testBytesStatsSerialization() throws IOException {
        {
            IngestPipelineMetric metric = new IngestPipelineMetric();
            IngestStats.ByteStats byteStats = metric.createByteStats();
            assertThat(byteStats, sameInstance(IngestStats.ByteStats.IDENTITY));

            IngestStats.ByteStats serializedByteStats = serialize(byteStats);
            assertThat(serializedByteStats, sameInstance(IngestStats.ByteStats.IDENTITY));
            assertThat(IngestStats.ByteStats.merge(IngestStats.ByteStats.IDENTITY, byteStats), sameInstance(byteStats));
        }
        {
            long ingestBytes = randomLongBetween(0, Long.MAX_VALUE);
            long producedBytes = randomLongBetween(0, Long.MAX_VALUE);
            IngestPipelineMetric metric = new IngestPipelineMetric();
            metric.preIngestBytes(ingestBytes);
            metric.postIngestBytes(producedBytes);
            IngestStats.ByteStats byteStats = metric.createByteStats();
            assertThat(byteStats.bytesIngested(), equalTo(ingestBytes));
            assertThat(byteStats.bytesProduced(), equalTo(producedBytes));

            IngestStats.ByteStats serializedByteStats = serialize(byteStats);
            assertThat(serializedByteStats.bytesIngested(), equalTo(ingestBytes));
            assertThat(serializedByteStats.bytesProduced(), equalTo(producedBytes));

            assertThat(IngestStats.ByteStats.merge(byteStats, IngestStats.ByteStats.IDENTITY), sameInstance(byteStats));
            assertThat(IngestStats.ByteStats.merge(IngestStats.ByteStats.IDENTITY, byteStats), sameInstance(byteStats));
            assertThat(
                IngestStats.ByteStats.merge(IngestStats.ByteStats.IDENTITY, IngestStats.ByteStats.IDENTITY),
                sameInstance(IngestStats.ByteStats.IDENTITY)
            );
        }
    }

    public void testStatsMerge() {
        var first = randomStats();
        var second = randomStats();
        assertEquals(
            new IngestStats.Stats(
                first.ingestCount() + second.ingestCount(),
                first.ingestTimeInMillis() + second.ingestTimeInMillis(),
                first.ingestCurrent() + second.ingestCurrent(),
                first.ingestFailedCount() + second.ingestFailedCount()
            ),
            IngestStats.Stats.merge(first, second)
        );
    }

    public void testPipelineStatsMerge() {
        var first = List.of(
            randomPipelineStat("pipeline-1"),
            randomPipelineStat("pipeline-1"),
            randomPipelineStat("pipeline-2"),
            randomPipelineStat("pipeline-3"),
            randomPipelineStat("pipeline-5")
        );
        var second = List.of(
            randomPipelineStat("pipeline-2"),
            randomPipelineStat("pipeline-1"),
            randomPipelineStat("pipeline-4"),
            randomPipelineStat("pipeline-3")
        );

        assertThat(
            IngestStats.PipelineStat.merge(first, second),
            containsInAnyOrder(
                new IngestStats.PipelineStat(
                    "pipeline-1",
                    merge(first.get(0).stats(), first.get(1).stats(), second.get(1).stats()),
                    merge(first.get(0).byteStats(), first.get(1).byteStats(), second.get(1).byteStats())
                ),
                new IngestStats.PipelineStat(
                    "pipeline-2",
                    merge(first.get(2).stats(), second.get(0).stats()),
                    IngestStats.ByteStats.merge(first.get(2).byteStats(), second.get(0).byteStats())
                ),
                new IngestStats.PipelineStat(
                    "pipeline-3",
                    merge(first.get(3).stats(), second.get(3).stats()),
                    IngestStats.ByteStats.merge(first.get(3).byteStats(), second.get(3).byteStats())
                ),
                new IngestStats.PipelineStat("pipeline-4", second.get(2).stats(), second.get(2).byteStats()),
                new IngestStats.PipelineStat("pipeline-5", first.get(4).stats(), first.get(4).byteStats())
            )
        );
    }

    public void testProcessorStatsMergeZeroCounts() {
        {
            var expected = randomPipelineProcessorStats();
            var first = Map.of("pipeline-1", expected);

            // merging with an empty map yields the non-empty map
            assertEquals(IngestStats.merge(Map.of(), first), first);
            assertEquals(IngestStats.merge(first, Map.of()), first);

            // it's the same exact reference, in fact
            assertSame(expected, IngestStats.merge(Map.of(), first).get("pipeline-1"));
            assertSame(expected, IngestStats.merge(first, Map.of()).get("pipeline-1"));
        }
        {
            var expected = randomPipelineProcessorStats();
            var first = Map.of("pipeline-1", expected);
            var zero = List.of(
                new IngestStats.ProcessorStat("proc-1", "type-1", zeroStats()),
                new IngestStats.ProcessorStat("proc-1", "type-2", zeroStats()),
                new IngestStats.ProcessorStat("proc-2", "type-1", zeroStats()),
                new IngestStats.ProcessorStat("proc-3", "type-3", zeroStats())
            );
            var second = Map.of("pipeline-1", zero);

            // merging with a zero map yields the non-zero map
            assertEquals(IngestStats.merge(second, first), first);
            assertEquals(IngestStats.merge(first, second), first);

            // it's the same exact reference, in fact
            assertSame(expected, IngestStats.merge(second, first).get("pipeline-1"));
            assertSame(expected, IngestStats.merge(first, second).get("pipeline-1"));
        }
    }

    public void testProcessorStatsMerge() {
        var first = Map.of(
            "pipeline-1",
            randomPipelineProcessorStats(),
            "pipeline-2",
            randomPipelineProcessorStats(),
            "pipeline-3",
            randomPipelineProcessorStats()
        );
        var second = Map.of(
            "pipeline-2",
            randomPipelineProcessorStats(),
            "pipeline-3",
            randomPipelineProcessorStats(),
            "pipeline-1",
            randomPipelineProcessorStats()
        );

        assertEquals(
            IngestStats.merge(first, second),
            Map.of(
                "pipeline-1",
                expectedPipelineProcessorStats(first.get("pipeline-1"), second.get("pipeline-1")),
                "pipeline-2",
                expectedPipelineProcessorStats(first.get("pipeline-2"), second.get("pipeline-2")),
                "pipeline-3",
                expectedPipelineProcessorStats(first.get("pipeline-3"), second.get("pipeline-3"))
            )
        );
    }

    public void testProcessorStatsMergeHeterogeneous() {
        // if a pipeline has heterogeneous *non-zero* stats, then we defer to the one with a smaller total ingest count

        var first = Map.of(
            "pipeline-1",
            List.of(
                new IngestStats.ProcessorStat("name-1", "type-1", new IngestStats.Stats(randomLongBetween(1, 100), 0, 0, 0)),
                new IngestStats.ProcessorStat("name-2", "type-2", new IngestStats.Stats(randomLongBetween(1, 100), 0, 0, 0))
            )
        );
        var expected = List.of(new IngestStats.ProcessorStat("name-1", "type-1", new IngestStats.Stats(1, 0, 0, 0)));
        var second = Map.of("pipeline-1", expected);

        assertEquals(second, IngestStats.merge(first, second));
        assertSame(expected, IngestStats.merge(second, first).get("pipeline-1"));
    }

    private static List<IngestStats.ProcessorStat> expectedPipelineProcessorStats(
        List<IngestStats.ProcessorStat> first,
        List<IngestStats.ProcessorStat> second
    ) {
        return List.of(
            new IngestStats.ProcessorStat("proc-1", "type-1", merge(first.get(0).stats(), second.get(0).stats())),
            new IngestStats.ProcessorStat("proc-1", "type-2", merge(first.get(1).stats(), second.get(1).stats())),
            new IngestStats.ProcessorStat("proc-2", "type-1", merge(first.get(2).stats(), second.get(2).stats())),
            new IngestStats.ProcessorStat("proc-3", "type-3", merge(first.get(3).stats(), second.get(3).stats()))
        );
    }

    private static List<IngestStats.ProcessorStat> randomPipelineProcessorStats() {
        return List.of(
            randomProcessorStat("proc-1", "type-1"),
            randomProcessorStat("proc-1", "type-2"),
            randomProcessorStat("proc-2", "type-1"),
            randomProcessorStat("proc-3", "type-3")
        );
    }

    private static IngestStats.Stats merge(IngestStats.Stats... stats) {
        return Arrays.stream(stats).reduce(IngestStats.Stats.IDENTITY, IngestStats.Stats::merge);
    }

    private static IngestStats.ByteStats merge(IngestStats.ByteStats... stats) {
        return Arrays.stream(stats).reduce(new IngestStats.ByteStats(0, 0), IngestStats.ByteStats::merge);
    }

    private static List<IngestStats.PipelineStat> createPipelineStats() {
        IngestStats.PipelineStat pipeline1Stats = new IngestStats.PipelineStat(
            "pipeline1",
            new IngestStats.Stats(3, 3, 3, 3),
            new IngestStats.ByteStats(123, 456)
        );
        IngestStats.PipelineStat pipeline2Stats = new IngestStats.PipelineStat(
            "pipeline2",
            new IngestStats.Stats(47, 97, 197, 297),
            new IngestStats.ByteStats(1234567, 34567890)
        );
        IngestStats.PipelineStat pipeline3Stats = new IngestStats.PipelineStat(
            "pipeline3",
            new IngestStats.Stats(0, 0, 0, 0),
            new IngestStats.ByteStats(0, 0)
        );
        return List.of(pipeline1Stats, pipeline2Stats, pipeline3Stats);
    }

    private static Map<String, List<IngestStats.ProcessorStat>> createProcessorStats(List<IngestStats.PipelineStat> pipelineStats) {
        assert (pipelineStats.size() >= 2);
        IngestStats.ProcessorStat processor1Stat = new IngestStats.ProcessorStat("processor1", "type", new IngestStats.Stats(1, 1, 1, 1));
        IngestStats.ProcessorStat processor2Stat = new IngestStats.ProcessorStat("processor2", "type", new IngestStats.Stats(2, 2, 2, 2));
        IngestStats.ProcessorStat processor3Stat = new IngestStats.ProcessorStat(
            "processor3",
            "type",
            new IngestStats.Stats(47, 97, 197, 297)
        );
        // pipeline1 -> processor1,processor2; pipeline2 -> processor3
        return Map.of(
            pipelineStats.get(0).pipelineId(),
            List.of(processor1Stat, processor2Stat),
            pipelineStats.get(1).pipelineId(),
            List.of(processor3Stat)
        );
    }

    private static IngestStats serialize(IngestStats stats) throws IOException {
        var out = new BytesStreamOutput();
        stats.writeTo(out);
        var in = out.bytes().streamInput();
        return IngestStats.read(in);
    }

    private static IngestStats.ByteStats serialize(IngestStats.ByteStats stats) throws IOException {
        var out = new BytesStreamOutput();
        stats.writeTo(out);
        var in = out.bytes().streamInput();
        return IngestStats.readByteStats(in);
    }

    private static void assertIngestStats(IngestStats ingestStats, IngestStats serializedStats) {
        assertNotSame(ingestStats, serializedStats);
        assertNotSame(ingestStats.totalStats(), serializedStats.totalStats());
        assertNotSame(ingestStats.pipelineStats(), serializedStats.pipelineStats());
        assertNotSame(ingestStats.processorStats(), serializedStats.processorStats());

        assertEquals(ingestStats.totalStats(), serializedStats.totalStats());
        assertEquals(ingestStats.pipelineStats().size(), serializedStats.pipelineStats().size());

        for (IngestStats.PipelineStat serializedPipelineStat : serializedStats.pipelineStats()) {
            assertEquals(
                getPipelineStats(ingestStats.pipelineStats(), serializedPipelineStat.pipelineId()),
                serializedPipelineStat.stats()
            );
            assertEquals(
                getPipelineByteStats(ingestStats.pipelineStats(), serializedPipelineStat.pipelineId()),
                serializedPipelineStat.byteStats()
            );
            List<IngestStats.ProcessorStat> serializedProcessorStats = serializedStats.processorStats()
                .get(serializedPipelineStat.pipelineId());
            List<IngestStats.ProcessorStat> processorStat = ingestStats.processorStats().get(serializedPipelineStat.pipelineId());
            if (processorStat != null) {
                Iterator<IngestStats.ProcessorStat> it = processorStat.iterator();
                // intentionally enforcing the identical ordering
                for (IngestStats.ProcessorStat serializedProcessorStat : serializedProcessorStats) {
                    IngestStats.ProcessorStat ps = it.next();
                    assertEquals(ps.name(), serializedProcessorStat.name());
                    assertEquals(ps.type(), serializedProcessorStat.type());
                    assertEquals(ps.stats(), serializedProcessorStat.stats());
                }
                assertFalse(it.hasNext());
            }
        }
    }

    private static IngestStats.Stats getPipelineStats(List<IngestStats.PipelineStat> pipelineStats, String id) {
        return pipelineStats.stream()
            .filter(p1 -> p1.pipelineId().equals(id))
            .findFirst()
            .map(IngestStats.PipelineStat::stats)
            .orElse(null);
    }

    private static IngestStats.ByteStats getPipelineByteStats(List<IngestStats.PipelineStat> pipelineStats, String id) {
        return pipelineStats.stream()
            .filter(p1 -> p1.pipelineId().equals(id))
            .findFirst()
            .map(IngestStats.PipelineStat::byteStats)
            .orElse(null);
    }

    private static IngestStats.ProcessorStat randomProcessorStat(String name, String type) {
        return new IngestStats.ProcessorStat(name, type, randomStats());
    }

    private static IngestStats.PipelineStat randomPipelineStat(String id) {
        return new IngestStats.PipelineStat(id, randomStats(), randomByteStats());
    }

    private static IngestStats.Stats randomStats() {
        return new IngestStats.Stats(randomLong(), randomLong(), randomLong(), randomLong());
    }

    private static IngestStats.Stats zeroStats() {
        return new IngestStats.Stats(0, 0, 0, 0);
    }

    private static IngestStats.ByteStats randomByteStats() {
        return new IngestStats.ByteStats(randomLong(), randomLong());
    }
}
