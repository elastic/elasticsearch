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
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.datafeed.DatafeedConfig;
import org.elasticsearch.client.ml.datafeed.DatafeedConfigTests;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class PreviewDatafeedResponseTests extends ESTestCase {

    protected PreviewDatafeedResponse createTestInstance() throws IOException {
        //This is just to create a random object to stand in the place of random data
        DatafeedConfig datafeedConfig = DatafeedConfigTests.createRandom();
        BytesReference bytes = XContentHelper.toXContent(datafeedConfig, XContentType.JSON, false);
        return new PreviewDatafeedResponse(bytes);
    }

    public void testGetDataList() throws IOException {
        String rawData = "[\n" +
            "  {\n" +
            "    \"time\": 1454803200000,\n" +
            "    \"airline\": \"JZA\",\n" +
            "    \"doc_count\": 5,\n" +
            "    \"responsetime\": 990.4628295898438\n" +
            "  },\n" +
            "  {\n" +
            "    \"time\": 1454803200000,\n" +
            "    \"airline\": \"JBU\",\n" +
            "    \"doc_count\": 23,\n" +
            "    \"responsetime\": 877.5927124023438\n" +
            "  },\n" +
            "  {\n" +
            "    \"time\": 1454803200000,\n" +
            "    \"airline\": \"KLM\",\n" +
            "    \"doc_count\": 42,\n" +
            "    \"responsetime\": 1355.481201171875\n" +
            "  }\n" +
            "]";
        BytesReference bytes = new BytesArray(rawData);
        PreviewDatafeedResponse response = new PreviewDatafeedResponse(bytes);
        assertThat(response.getDataList()
            .stream()
            .map(map -> (String)map.get("airline"))
            .collect(Collectors.toList()), containsInAnyOrder("JZA", "JBU", "KLM"));

        rawData = "{\"key\":\"my_value\"}";
        bytes = new BytesArray(rawData);
        response = new PreviewDatafeedResponse(bytes);
        assertThat(response.getDataList()
            .stream()
            .map(map -> (String)map.get("key"))
            .collect(Collectors.toList()), containsInAnyOrder("my_value"));

    }

    //Because this is raw a BytesReference, the shuffling done via `AbstractXContentTestCase` is unacceptable and causes equality failures
    public void testSerializationDeserialization() throws IOException {
        for (int runs = 0; runs < 20; runs++) {
            XContentType xContentType = XContentType.JSON;
            PreviewDatafeedResponse testInstance = createTestInstance();
            BytesReference originalXContent = XContentHelper.toXContent(testInstance, xContentType, false);
            XContentParser parser = this.createParser(xContentType.xContent(), originalXContent);
            PreviewDatafeedResponse parsed = PreviewDatafeedResponse.fromXContent(parser);
            assertEquals(testInstance, parsed);
            assertToXContentEquivalent(
                XContentHelper.toXContent(testInstance, xContentType, false),
                XContentHelper.toXContent(parsed, xContentType, false),
                xContentType);
        }
    }

}
