/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class IngestStatsTests extends ESTestCase {

    public void testSerialization() throws IOException {
        IngestStats.Stats totalStats = new IngestStats.Stats(50, 100, 200, 300);
        List<IngestStats.PipelineStat> pipelineStats = createPipelineStats();
        Map<String, List<IngestStats.ProcessorStat>> processorStats = createProcessorStats(pipelineStats);
        IngestStats ingestStats = new IngestStats(totalStats, pipelineStats, processorStats);
        IngestStats serializedStats = serialize(ingestStats);
        assertIngestStats(ingestStats, serializedStats, true, true);
    }

    @SuppressWarnings("unchecked")
    public void testToXContent() throws Exception {
        IngestStats.Stats totalStats = new IngestStats.Stats(
            randomIntBetween(0, 100),
            randomIntBetween(0, 100),
            randomIntBetween(0, 100),
            randomIntBetween(0, 100)
        );
        IngestStats.PipelineStat pipeline1Stats = new IngestStats.PipelineStat(
            "outerPipeline",
            new IngestStats.Stats(randomIntBetween(0, 100), randomIntBetween(0, 100), randomIntBetween(0, 100), randomIntBetween(0, 100))
        );
        IngestStats.PipelineStat pipeline2Stats = new IngestStats.PipelineStat(
            "innerPipeline",
            new IngestStats.Stats(randomIntBetween(0, 100), randomIntBetween(0, 100), randomIntBetween(0, 100), randomIntBetween(0, 100))
        );
        IngestStats.Stats pipelineStats = new IngestStats.Stats(
            randomIntBetween(0, 100),
            randomIntBetween(0, 100),
            randomIntBetween(0, 100),
            randomIntBetween(0, 100)
        );
        IngestStats.ProcessorStat pipelineProcessorStat = new IngestStats.ProcessorStat(
            "pipeline:innerPipeline",
            "pipeline",
            pipelineStats
        );
        IngestStats.ProcessorStat sharedProcessor = getRandomNonPipelineProcessorStat();
        List<IngestStats.ProcessorStat> outerPipelineProcessors = List.of(
            sharedProcessor,
            getRandomNonPipelineProcessorStat(),
            pipelineProcessorStat,
            getRandomNonPipelineProcessorStat(),
            getRandomNonPipelineProcessorStat()
        );
        List<IngestStats.ProcessorStat> innerPipelineProcessors = List.of(
            getRandomNonPipelineProcessorStat(),
            getRandomNonPipelineProcessorStat(),
            sharedProcessor,
            getRandomNonPipelineProcessorStat()
        );
        Map<String, List<IngestStats.ProcessorStat>> processorStats = Map.of(
            "outerPipeline",
            outerPipelineProcessors,
            "outerPipeline:innerPipeline",
            innerPipelineProcessors
        );
        IngestStats ingestStats = new IngestStats(totalStats, List.of(pipeline1Stats, pipeline2Stats), processorStats);
        Map<String, Object> ingestStatsMap = xContentToMap(ingestStats, RestApiVersion.current());
        assertThat(ingestStatsMap.size(), equalTo(1));
        Map<String, Object> ingest = (Map<String, Object>) ingestStatsMap.get("ingest");
        assertThat(ingest.size(), equalTo(2));
        Map<String, Object> topLevelPipelines = (Map<String, Object>) ingest.get("pipelines");
        assertThat(topLevelPipelines.size(), equalTo(2));
        Map<String, Object> outerPipeline = (Map<String, Object>) topLevelPipelines.get("outerPipeline");
        assertThat(outerPipeline.size(), equalTo(5));
        List<Map<String, Object>> outerPipelineProcessorsList = (List<Map<String, Object>>) outerPipeline.get("processors");
        assertThat(outerPipelineProcessorsList.size(), equalTo(outerPipelineProcessors.size()));
        Map<String, Object> innerPipelineProcessor = outerPipelineProcessorsList.get(2);
        assertThat(innerPipelineProcessor.size(), equalTo(1));
        Map<String, Object> innerPipeline = (Map<String, Object>) innerPipelineProcessor.get("pipeline:innerPipeline");
        assertNotNull(innerPipeline);
        assertThat(innerPipeline.size(), equalTo(2));
        List<Map<String, Object>> innerPipelineProcessorsList = (List<Map<String, Object>>) ((Map<String, Object>) innerPipeline.get(
            "stats"
        )).get("processors");
        assertThat(innerPipelineProcessorsList.size(), equalTo(4));
    }

    private Map<String, Object> xContentToMap(ToXContent xcontent, RestApiVersion restApiVersion) throws IOException {
        XContentBuilder builder = new XContentBuilder(
            JsonXContent.jsonXContent,
            new ByteArrayOutputStream(),
            Collections.emptySet(),
            Collections.emptySet(),
            JsonXContent.jsonXContent.type().toParsedMediaType(),
            restApiVersion
        );
        // XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        xcontent.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
        return parser.map();
    }

    private IngestStats.ProcessorStat getRandomNonPipelineProcessorStat() {
        IngestStats.Stats processorStats = new IngestStats.Stats(47, 97, 197, 297);
        IngestStats.ProcessorStat processorStat = new IngestStats.ProcessorStat(
            randomAlphaOfLengthBetween(5, 10),
            randomAlphaOfLength(5),
            processorStats
        );
        return processorStat;
    }

    private List<IngestStats.PipelineStat> createPipelineStats() {
        IngestStats.PipelineStat pipeline1Stats = new IngestStats.PipelineStat("pipeline1", new IngestStats.Stats(3, 3, 3, 3));
        IngestStats.PipelineStat pipeline2Stats = new IngestStats.PipelineStat("pipeline2", new IngestStats.Stats(47, 97, 197, 297));
        IngestStats.PipelineStat pipeline3Stats = new IngestStats.PipelineStat("pipeline3", new IngestStats.Stats(0, 0, 0, 0));
        return List.of(pipeline1Stats, pipeline2Stats, pipeline3Stats);
    }

    private Map<String, List<IngestStats.ProcessorStat>> createProcessorStats(List<IngestStats.PipelineStat> pipelineStats) {
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
            pipelineStats.get(0).getPipelineId(),
            List.of(processor1Stat, processor2Stat),
            pipelineStats.get(1).getPipelineId(),
            List.of(processor3Stat)
        );
    }

    private IngestStats serialize(IngestStats stats) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        return new IngestStats(in);
    }

    private void assertIngestStats(
        IngestStats ingestStats,
        IngestStats serializedStats,
        boolean expectProcessors,
        boolean expectProcessorTypes
    ) {
        assertNotSame(ingestStats, serializedStats);
        assertNotSame(ingestStats.getTotalStats(), serializedStats.getTotalStats());
        assertNotSame(ingestStats.getPipelineStats(), serializedStats.getPipelineStats());
        assertNotSame(ingestStats.getProcessorStats(), serializedStats.getProcessorStats());

        assertStats(ingestStats.getTotalStats(), serializedStats.getTotalStats());
        assertEquals(ingestStats.getPipelineStats().size(), serializedStats.getPipelineStats().size());

        for (IngestStats.PipelineStat serializedPipelineStat : serializedStats.getPipelineStats()) {
            assertStats(
                getPipelineStats(ingestStats.getPipelineStats(), serializedPipelineStat.getPipelineId()),
                serializedPipelineStat.getStats()
            );
            List<IngestStats.ProcessorStat> serializedProcessorStats = serializedStats.getProcessorStats()
                .get(serializedPipelineStat.getPipelineId());
            List<IngestStats.ProcessorStat> processorStat = ingestStats.getProcessorStats().get(serializedPipelineStat.getPipelineId());
            if (expectProcessors) {
                if (processorStat != null) {
                    Iterator<IngestStats.ProcessorStat> it = processorStat.iterator();
                    // intentionally enforcing the identical ordering
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
            } else {
                // pre 6.5 did not serialize any processor stats
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
