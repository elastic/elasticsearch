/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class IngestStatsTests extends ESTestCase {

    public void testSerialization() throws IOException {
        IngestStats.Stats totalStats = new IngestStats.Stats(50, 100, 200, 300);
        List<IngestStats.PipelineStat> pipelineStats = createPipelineStats();
        Map<String, List<IngestStats.ProcessorStat>> processorStats = createProcessorStats(pipelineStats);
        IngestStats ingestStats = new IngestStats(totalStats, pipelineStats, processorStats);
        IngestStats serializedStats = serialize(ingestStats);
        assertIngestStats(ingestStats, serializedStats);
    }

    private static List<IngestStats.PipelineStat> createPipelineStats() {
        IngestStats.PipelineStat pipeline1Stats = new IngestStats.PipelineStat("pipeline1", new IngestStats.Stats(3, 3, 3, 3));
        IngestStats.PipelineStat pipeline2Stats = new IngestStats.PipelineStat("pipeline2", new IngestStats.Stats(47, 97, 197, 297));
        IngestStats.PipelineStat pipeline3Stats = new IngestStats.PipelineStat("pipeline3", new IngestStats.Stats(0, 0, 0, 0));
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
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        return new IngestStats(in);
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
}
