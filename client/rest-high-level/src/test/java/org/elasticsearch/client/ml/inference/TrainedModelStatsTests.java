/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.inference;

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
        return field -> !field.isEmpty();
    }

    @Override
    protected TrainedModelStats createTestInstance() {
        return new TrainedModelStats(
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomIngestStats(),
            randomInt());
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
