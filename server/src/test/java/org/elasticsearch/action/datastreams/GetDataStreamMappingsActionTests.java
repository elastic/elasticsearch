/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.equalTo;

public class GetDataStreamMappingsActionTests extends ESTestCase {

    public void testResponseToXContentEmpty() throws IOException {
        List<GetDataStreamMappingsAction.DataStreamMappingsResponse> responseList = new ArrayList<>();
        GetDataStreamMappingsAction.Response response = new GetDataStreamMappingsAction.Response(responseList);
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true);
            response.toXContentChunked(ToXContent.EMPTY_PARAMS).forEachRemaining(xcontent -> {
                try {
                    xcontent.toXContent(builder, EMPTY_PARAMS);
                } catch (IOException e) {
                    fail(e);
                }
            });
            Map<String, Object> xContentMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            assertThat(xContentMap, equalTo(Map.of("data_streams", List.of())));
        }
    }

    public void testResponseToXContent() throws IOException {
        Map<String, Object> dataStream1Mappings = Map.of(
            "properties",
            Map.of("field2", Map.of("type", "text"), "field3", Map.of("type", "keyword"))
        );
        Map<String, Object> dataStream1EffectiveMappings = Map.of(
            "properties",
            Map.of("field1", Map.of("type", "keyword"), "field2", Map.of("type", "text"), "field3", Map.of("type", "keyword"))
        );
        Map<String, Object> dataStream2Mappings = Map.of(
            "properties",
            Map.of("field4", Map.of("type", "text"), "field5", Map.of("type", "keyword"))
        );
        Map<String, Object> dataStream2EffectiveMappings = Map.of(
            "properties",
            Map.of("field4", Map.of("type", "text"), "field5", Map.of("type", "keyword"), "field6", Map.of("type", "keyword"))
        );
        GetDataStreamMappingsAction.DataStreamMappingsResponse DataStreamMappingsResponse1 =
            new GetDataStreamMappingsAction.DataStreamMappingsResponse(
                "dataStream1",
                new CompressedXContent(dataStream1Mappings),
                new CompressedXContent(dataStream1EffectiveMappings)
            );
        GetDataStreamMappingsAction.DataStreamMappingsResponse DataStreamMappingsResponse2 =
            new GetDataStreamMappingsAction.DataStreamMappingsResponse(
                "dataStream2",
                new CompressedXContent(dataStream2Mappings),
                new CompressedXContent(dataStream2EffectiveMappings)
            );
        List<GetDataStreamMappingsAction.DataStreamMappingsResponse> responseList = List.of(
            DataStreamMappingsResponse1,
            DataStreamMappingsResponse2
        );
        GetDataStreamMappingsAction.Response response = new GetDataStreamMappingsAction.Response(responseList);
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true);
            response.toXContentChunked(ToXContent.EMPTY_PARAMS).forEachRemaining(xcontent -> {
                try {
                    xcontent.toXContent(builder, EMPTY_PARAMS);
                } catch (IOException e) {
                    fail(e);
                }
            });
            Map<String, Object> xContentMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            assertThat(
                xContentMap,
                equalTo(
                    Map.of(
                        "data_streams",
                        List.of(
                            Map.of(
                                "name",
                                "dataStream1",
                                "mappings",
                                dataStream1Mappings,
                                "effective_mappings",
                                dataStream1EffectiveMappings
                            ),
                            Map.of(
                                "name",
                                "dataStream2",
                                "mappings",
                                dataStream2Mappings,
                                "effective_mappings",
                                dataStream2EffectiveMappings
                            )
                        )
                    )
                )
            );
        }
    }
}
