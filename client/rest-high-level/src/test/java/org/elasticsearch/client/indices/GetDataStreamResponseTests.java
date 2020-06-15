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

package org.elasticsearch.client.indices;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.cluster.metadata.DataStream.getDefaultBackingIndexName;
import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class GetDataStreamResponseTests extends ESTestCase {

    public void testFromXContent() throws Exception {
        xContentTester(
            this::createParser,
            GetDataStreamResponseTests::createTestInstance,
            GetDataStreamResponseTests::toXContent,
            GetDataStreamResponseTests::fromXContent)
            .supportsUnknownFields(true)
            .randomFieldsExcludeFilter(a -> true)
            .test();
    }

    private static GetDataStreamResponse createTestInstance() {
        ArrayList<DataStream> dataStreams = new ArrayList<>();
        int count = randomInt(10);
        for (int i = 0; i < count; i++) {
            dataStreams.add(randomInstance());
        }
        return new GetDataStreamResponse(dataStreams);
    }

    private static List<Index> randomIndexInstances() {
        int numIndices = randomIntBetween(0, 128);
        List<Index> indices = new ArrayList<>(numIndices);
        for (int i = 0; i < numIndices; i++) {
            indices.add(new Index(randomAlphaOfLength(10).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID(random())));
        }
        return indices;
    }

    private static DataStream randomInstance() {
        List<Index> indices = randomIndexInstances();
        long generation = indices.size() + randomLongBetween(1, 128);
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        indices.add(new Index(getDefaultBackingIndexName(dataStreamName, generation), UUIDs.randomBase64UUID(random())));
        return new DataStream(dataStreamName, randomAlphaOfLength(10), indices, generation);
    }

    private static GetDataStreamResponse fromXContent(XContentParser parser) throws IOException {
        parser.nextToken();
        return GetDataStreamResponse.fromXContent(parser);
    }

    private static void toXContent(GetDataStreamResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.startArray("arr");
        for (DataStream dataStream : response.getDataStreams()) {
            dataStream.toXContent(builder, null);
        }
        builder.endArray();
        builder.endObject();
    }
}
