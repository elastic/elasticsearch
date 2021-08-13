/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference;

import org.elasticsearch.client.ml.inference.trainedmodel.InferenceStatsTests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class TrainedModelStatsTests extends AbstractXContentTestCase<TrainedModelStats> {

    @Override
    protected TrainedModelStats doParseInstance(XContentParser parser) throws IOException {
        return TrainedModelStats.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }

    @Override
    protected TrainedModelStats createTestInstance() {
        return new TrainedModelStats(
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomIngestStats(),
            randomInt(),
            randomBoolean() ? null : InferenceStatsTests.randomInstance());
    }

    private Map<String, Object> randomIngestStats() {
        try {
            List<String> pipelineIds = Stream.generate(()-> randomAlphaOfLength(10))
                .limit(randomIntBetween(0, 10))
                .collect(Collectors.toList());
            IngestStats stats = new IngestStats(
                new IngestStats.Stats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()),
                pipelineIds.stream().map(id -> new IngestStats.PipelineStat(id, randomStats())).collect(Collectors.toList()),
                pipelineIds.stream().collect(Collectors.toMap(Function.identity(), (v) -> randomProcessorStats())));
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                builder.startObject();
                stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
                builder.endObject();
                return XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            }
        } catch (IOException ex) {
            fail(ex.getMessage());
            return null;
        }
    }

    private IngestStats.Stats randomStats(){
        return new IngestStats.Stats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }

    private List<IngestStats.ProcessorStat> randomProcessorStats() {
        return Stream.generate(() -> randomAlphaOfLength(10))
            .limit(randomIntBetween(0, 10))
            .map(name -> new IngestStats.ProcessorStat(name, "inference", randomStats()))
            .collect(Collectors.toList());
    }

}
