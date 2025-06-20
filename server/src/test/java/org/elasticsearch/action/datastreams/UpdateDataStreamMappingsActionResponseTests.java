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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.ComponentTemplateTests.randomMappings;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.equalTo;

public class UpdateDataStreamMappingsActionResponseTests extends AbstractWireSerializingTestCase<UpdateDataStreamMappingsAction.Response> {

    public void testToXContent() throws IOException {
        Map<String, Object> dataStream1Mappings = Map.of(
            "properties",
            Map.of("field1", Map.of("type", "keyword"), "field2", Map.of("type", "keyword"))
        );
        Map<String, Object> dataStream1EffectiveMappings = Map.of(
            "properties",
            Map.of("field1", Map.of("type", "keyword"), "field2", Map.of("type", "keyword"), "field3", Map.of("type", "keyword"))
        );
        Map<String, Object> dataStream2Mappings = Map.of(
            "properties",
            Map.of("field4", Map.of("type", "keyword"), "field5", Map.of("type", "keyword"))
        );
        Map<String, Object> dataStream2EffectiveMappings = Map.of(
            "properties",
            Map.of("field4", Map.of("type", "keyword"), "field5", Map.of("type", "keyword"), "field6", Map.of("type", "keyword"))
        );
        boolean dataStream1Succeeded = randomBoolean();
        String dataStream1Error = randomBoolean() ? null : randomAlphaOfLength(20);
        boolean dataStream2Succeeded = randomBoolean();
        String dataStream2Error = randomBoolean() ? null : randomAlphaOfLength(20);
        UpdateDataStreamMappingsAction.DataStreamMappingsResponse DataStreamMappingsResponse1 =
            new UpdateDataStreamMappingsAction.DataStreamMappingsResponse(
                "dataStream1",
                dataStream1Succeeded,
                dataStream1Error,
                new CompressedXContent(dataStream1Mappings),
                new CompressedXContent(dataStream1EffectiveMappings)
            );
        UpdateDataStreamMappingsAction.DataStreamMappingsResponse DataStreamMappingsResponse2 =
            new UpdateDataStreamMappingsAction.DataStreamMappingsResponse(
                "dataStream2",
                dataStream2Succeeded,
                dataStream2Error,
                new CompressedXContent(dataStream2Mappings),
                new CompressedXContent(dataStream2EffectiveMappings)
            );
        List<UpdateDataStreamMappingsAction.DataStreamMappingsResponse> responseList = List.of(
            DataStreamMappingsResponse1,
            DataStreamMappingsResponse2
        );
        UpdateDataStreamMappingsAction.Response response = new UpdateDataStreamMappingsAction.Response(responseList);
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
                            buildExpectedMap(
                                "dataStream1",
                                dataStream1Succeeded,
                                dataStream1Error,
                                dataStream1Mappings,
                                dataStream1EffectiveMappings
                            ),
                            buildExpectedMap(
                                "dataStream2",
                                dataStream2Succeeded,
                                dataStream2Error,
                                dataStream2Mappings,
                                dataStream2EffectiveMappings
                            )
                        )
                    )
                )
            );
        }
    }

    @Override
    protected Writeable.Reader<UpdateDataStreamMappingsAction.Response> instanceReader() {
        return UpdateDataStreamMappingsAction.Response::new;
    }

    @Override
    protected UpdateDataStreamMappingsAction.Response createTestInstance() {
        return new UpdateDataStreamMappingsAction.Response(randomList(10, this::randomDataStreamMappingsResponse));
    }

    @Override
    protected UpdateDataStreamMappingsAction.Response mutateInstance(UpdateDataStreamMappingsAction.Response instance) throws IOException {
        List<UpdateDataStreamMappingsAction.DataStreamMappingsResponse> responseList = instance.getDataStreamMappingsResponses();
        List<UpdateDataStreamMappingsAction.DataStreamMappingsResponse> mutatedResponseList = new ArrayList<>(responseList);
        switch (between(0, 1)) {
            case 0 -> {
                if (responseList.isEmpty()) {
                    mutatedResponseList.add(randomDataStreamMappingsResponse());
                } else {
                    mutatedResponseList.remove(randomInt(responseList.size() - 1));
                }
            }
            case 1 -> {
                mutatedResponseList.add(randomDataStreamMappingsResponse());
            }
            default -> throw new AssertionError("Should not be here");
        }
        return new UpdateDataStreamMappingsAction.Response(mutatedResponseList);
    }

    private Map<String, Object> buildExpectedMap(
        String name,
        boolean succeeded,
        String error,
        Map<String, Object> mappings,
        Map<String, Object> effectiveMappings
    ) {
        Map<String, Object> result = new HashMap<>();
        result.put("name", name);
        result.put("applied_to_data_stream", succeeded);
        if (error != null) {
            result.put("error", error);
        }
        result.put("mappings", mappings);
        result.put("effective_mappings", effectiveMappings);
        Map<String, Object> indexSettingsResults = new HashMap<>();
        return result;
    }

    private UpdateDataStreamMappingsAction.DataStreamMappingsResponse randomDataStreamMappingsResponse() {
        return new UpdateDataStreamMappingsAction.DataStreamMappingsResponse(
            "dataStream1",
            randomBoolean(),
            randomBoolean() ? null : randomAlphaOfLength(20),
            randomMappings(),
            randomMappings()
        );
    }
}
