/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.Version;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IngestStatsTests extends ESTestCase {

    public void testSerialization() throws IOException {
        IngestStats.Stats totalStats = new IngestStats.Stats(50, 100, 200, 300);
        List<IngestStats.PipelineStat> pipelineStats = createPipelineStats();
        Map<String, List<IngestStats.ProcessorStat>> processorStats = createProcessorStats(pipelineStats);
        IngestStats ingestStats = new IngestStats(totalStats, pipelineStats, processorStats);
        IngestStats serializedStats = serialize(ingestStats);
        assertIngestStats(ingestStats, serializedStats, true, true);
    }

    public void testBWCIngestProcessorTypeStats() throws IOException {
        IngestStats.Stats totalStats = new IngestStats.Stats(50, 100, 200, 300);
        List<IngestStats.PipelineStat> pipelineStats = createPipelineStats();
        Map<String, List<IngestStats.ProcessorStat>> processorStats = createProcessorStats(pipelineStats);
        IngestStats expectedIngestStats = new IngestStats(totalStats, pipelineStats, processorStats);

        //legacy output logic
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(VersionUtils.getPreviousVersion(Version.V_7_6_0));
        expectedIngestStats.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(VersionUtils.getPreviousVersion(Version.V_7_6_0));
        IngestStats serializedStats = new IngestStats(in);
        assertIngestStats(expectedIngestStats, serializedStats, true, false);
    }

    private List<IngestStats.PipelineStat> createPipelineStats() {
        IngestStats.PipelineStat pipeline1Stats = new IngestStats.PipelineStat("pipeline1", new IngestStats.Stats(3, 3, 3, 3));
        IngestStats.PipelineStat pipeline2Stats = new IngestStats.PipelineStat("pipeline2", new IngestStats.Stats(47, 97, 197, 297));
        IngestStats.PipelineStat pipeline3Stats = new IngestStats.PipelineStat("pipeline3", new IngestStats.Stats(0, 0, 0, 0));
        return Stream.of(pipeline1Stats, pipeline2Stats, pipeline3Stats).collect(Collectors.toList());
    }

    private Map<String, List<IngestStats.ProcessorStat>> createProcessorStats(List<IngestStats.PipelineStat> pipelineStats){
        assert(pipelineStats.size() >= 2);
        IngestStats.ProcessorStat processor1Stat = new IngestStats.ProcessorStat("processor1", "type", new IngestStats.Stats(1, 1, 1, 1));
        IngestStats.ProcessorStat processor2Stat = new IngestStats.ProcessorStat("processor2", "type", new IngestStats.Stats(2, 2, 2, 2));
        IngestStats.ProcessorStat processor3Stat = new IngestStats.ProcessorStat("processor3", "type",
            new IngestStats.Stats(47, 97, 197, 297));
        //pipeline1 -> processor1,processor2; pipeline2 -> processor3
        return MapBuilder.<String, List<IngestStats.ProcessorStat>>newMapBuilder()
            .put(pipelineStats.get(0).getPipelineId(), Stream.of(processor1Stat, processor2Stat).collect(Collectors.toList()))
            .put(pipelineStats.get(1).getPipelineId(), Collections.singletonList(processor3Stat))
            .map();
    }

    private IngestStats serialize(IngestStats stats) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        return new IngestStats(in);
    }

    private void assertIngestStats(IngestStats ingestStats, IngestStats serializedStats, boolean expectProcessors,
                                   boolean expectProcessorTypes){
        assertNotSame(ingestStats, serializedStats);
        assertNotSame(ingestStats.getTotalStats(), serializedStats.getTotalStats());
        assertNotSame(ingestStats.getPipelineStats(), serializedStats.getPipelineStats());
        assertNotSame(ingestStats.getProcessorStats(), serializedStats.getProcessorStats());

        assertStats(ingestStats.getTotalStats(), serializedStats.getTotalStats());
        assertEquals(ingestStats.getPipelineStats().size(), serializedStats.getPipelineStats().size());

        for (IngestStats.PipelineStat serializedPipelineStat : serializedStats.getPipelineStats()) {
            assertStats(getPipelineStats(ingestStats.getPipelineStats(), serializedPipelineStat.getPipelineId()),
                serializedPipelineStat.getStats());
            List<IngestStats.ProcessorStat> serializedProcessorStats =
                serializedStats.getProcessorStats().get(serializedPipelineStat.getPipelineId());
            List<IngestStats.ProcessorStat> processorStat = ingestStats.getProcessorStats().get(serializedPipelineStat.getPipelineId());
            if(expectProcessors) {
                if (processorStat != null) {
                    Iterator<IngestStats.ProcessorStat> it = processorStat.iterator();
                    //intentionally enforcing the identical ordering
                    for (IngestStats.ProcessorStat serializedProcessorStat : serializedProcessorStats) {
                        IngestStats.ProcessorStat ps = it.next();
                        assertEquals(ps.getName(), serializedProcessorStat.getName());
                        if (expectProcessorTypes) {
                            assertEquals(ps.getType(), serializedProcessorStat.getType());
                        } else {
                            assertEquals("_NOT_AVAILABLE", serializedProcessorStat.getType());
                        }
                        assertStats(ps.getStats(), serializedProcessorStat.getStats());
                    }
                    assertFalse(it.hasNext());
                }
            }else{
                //pre 6.5 did not serialize any processor stats
                assertNull(serializedProcessorStats);
            }
        }

    }
    private void assertStats(IngestStats.Stats fromObject, IngestStats.Stats fromStream) {
        assertEquals(fromObject.getIngestCount(), fromStream.getIngestCount());
        assertEquals(fromObject.getIngestFailedCount(), fromStream.getIngestFailedCount());
        assertEquals(fromObject.getIngestTimeInMillis(), fromStream.getIngestTimeInMillis());
        assertEquals(fromObject.getIngestCurrent(), fromStream.getIngestCurrent());
    }

    private IngestStats.Stats getPipelineStats(List<IngestStats.PipelineStat> pipelineStats, String id) {
        return pipelineStats.stream().filter(p1 -> p1.getPipelineId().equals(id)).findFirst().map(p2 -> p2.getStats()).orElse(null);
    }
}
