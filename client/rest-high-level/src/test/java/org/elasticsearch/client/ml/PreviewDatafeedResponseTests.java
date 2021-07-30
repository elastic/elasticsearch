/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
