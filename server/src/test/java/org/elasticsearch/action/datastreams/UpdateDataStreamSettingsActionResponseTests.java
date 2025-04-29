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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
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

import static org.elasticsearch.action.datastreams.UpdateDataStreamSettingsAction.DataStreamSettingsResponse.IndexSettingError;
import static org.elasticsearch.cluster.metadata.ComponentTemplateTests.randomSettings;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.equalTo;

public class UpdateDataStreamSettingsActionResponseTests extends AbstractWireSerializingTestCase<UpdateDataStreamSettingsAction.Response> {
    @Override
    protected Writeable.Reader<UpdateDataStreamSettingsAction.Response> instanceReader() {
        return UpdateDataStreamSettingsAction.Response::new;
    }

    public void testToXContent() throws IOException {
        Map<String, String> dataStream1Settings = Map.of("setting1", "value1", "setting2", "value2");
        Map<String, String> dataStream1EffectiveSettings = Map.of("setting1", "value1", "setting2", "value2", "setting3", "value3");
        List<String> dataStream1AppliedToDataStreamOnly = randomList(10, () -> randomAlphanumericOfLength(10));
        List<String> dataStream1AppliedToBackingIndices = randomList(10, () -> randomAlphanumericOfLength(10));
        List<IndexSettingError> dataStream1IndexErrors = randomList(
            10,
            () -> new IndexSettingError(randomAlphanumericOfLength(10), randomAlphaOfLength(10))
        );
        Map<String, String> dataStream2Settings = Map.of("setting4", "value4", "setting5", "value5");
        Map<String, String> dataStream2EffectiveSettings = Map.of("setting4", "value4", "setting5", "value5", "settings6", "value6");
        List<String> dataStream2AppliedToDataStreamOnly = randomList(10, () -> randomAlphanumericOfLength(10));
        List<String> dataStream2AppliedToBackingIndices = randomList(10, () -> randomAlphanumericOfLength(10));
        List<IndexSettingError> dataStream2IndexErrors = randomList(
            10,
            () -> new IndexSettingError(randomAlphanumericOfLength(10), randomAlphaOfLength(10))
        );
        boolean dataStream1Succeeded = randomBoolean();
        String dataStream1Error = randomBoolean() ? null : randomAlphaOfLength(20);
        boolean dataStream2Succeeded = randomBoolean();
        String dataStream2Error = randomBoolean() ? null : randomAlphaOfLength(20);
        UpdateDataStreamSettingsAction.DataStreamSettingsResponse dataStreamSettingsResponse1 =
            new UpdateDataStreamSettingsAction.DataStreamSettingsResponse(
                "dataStream1",
                dataStream1Succeeded,
                dataStream1Error,
                Settings.builder().loadFromMap(dataStream1Settings).build(),
                Settings.builder().loadFromMap(dataStream1EffectiveSettings).build(),
                new UpdateDataStreamSettingsAction.DataStreamSettingsResponse.IndicesSettingsResult(
                    dataStream1AppliedToDataStreamOnly,
                    dataStream1AppliedToBackingIndices,
                    dataStream1IndexErrors
                )
            );
        UpdateDataStreamSettingsAction.DataStreamSettingsResponse dataStreamSettingsResponse2 =
            new UpdateDataStreamSettingsAction.DataStreamSettingsResponse(
                "dataStream2",
                dataStream2Succeeded,
                dataStream2Error,
                Settings.builder().loadFromMap(dataStream2Settings).build(),
                Settings.builder().loadFromMap(dataStream2EffectiveSettings).build(),
                new UpdateDataStreamSettingsAction.DataStreamSettingsResponse.IndicesSettingsResult(
                    dataStream2AppliedToDataStreamOnly,
                    dataStream2AppliedToBackingIndices,
                    dataStream2IndexErrors
                )
            );
        List<UpdateDataStreamSettingsAction.DataStreamSettingsResponse> responseList = List.of(
            dataStreamSettingsResponse1,
            dataStreamSettingsResponse2
        );
        UpdateDataStreamSettingsAction.Response response = new UpdateDataStreamSettingsAction.Response(responseList);
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
                                dataStream1Settings,
                                dataStream1EffectiveSettings,
                                dataStream1AppliedToDataStreamOnly,
                                dataStream1AppliedToBackingIndices,
                                dataStream1IndexErrors
                            ),
                            buildExpectedMap(
                                "dataStream2",
                                dataStream2Succeeded,
                                dataStream2Error,
                                dataStream2Settings,
                                dataStream2EffectiveSettings,
                                dataStream2AppliedToDataStreamOnly,
                                dataStream2AppliedToBackingIndices,
                                dataStream2IndexErrors
                            )
                        )
                    )
                )
            );
        }
    }

    private Map<String, Object> buildExpectedMap(
        String name,
        boolean succeeded,
        String error,
        Map<String, String> settings,
        Map<String, String> effectiveSettings,
        List<String> appliedToDataStreamOnly,
        List<String> appliedToIndices,
        List<IndexSettingError> indexErrors
    ) {
        Map<String, Object> result = new HashMap<>();
        result.put("name", name);
        result.put("applied_to_data_stream", succeeded);
        if (error != null) {
            result.put("error", error);
        }
        result.put("settings", settings);
        result.put("effective_settings", effectiveSettings);
        Map<String, Object> indexSettingsResults = new HashMap<>();
        indexSettingsResults.put("applied_to_data_stream_only", appliedToDataStreamOnly);
        indexSettingsResults.put("applied_to_data_stream_and_backing_indices", appliedToIndices);
        if (indexErrors.isEmpty() == false) {
            indexSettingsResults.put(
                "errors",
                indexErrors.stream()
                    .map(indexSettingError -> Map.of("index", indexSettingError.indexName(), "error", indexSettingError.errorMessage()))
                    .toList()
            );
        }
        result.put("index_settings_results", indexSettingsResults);
        return result;
    }

    @Override
    protected UpdateDataStreamSettingsAction.Response createTestInstance() {
        return new UpdateDataStreamSettingsAction.Response(randomList(10, this::randomDataStreamSettingsResponse));
    }

    private UpdateDataStreamSettingsAction.DataStreamSettingsResponse randomDataStreamSettingsResponse() {
        return new UpdateDataStreamSettingsAction.DataStreamSettingsResponse(
            "dataStream1",
            randomBoolean(),
            randomBoolean() ? null : randomAlphaOfLength(20),
            randomSettings(),
            randomSettings(),
            randomIndicesSettingsResult()
        );
    }

    private UpdateDataStreamSettingsAction.DataStreamSettingsResponse.IndicesSettingsResult randomIndicesSettingsResult() {
        return new UpdateDataStreamSettingsAction.DataStreamSettingsResponse.IndicesSettingsResult(
            randomList(10, () -> randomAlphanumericOfLength(20)),
            randomList(10, () -> randomAlphanumericOfLength(20)),
            randomList(10, this::randomIndexSettingError)
        );
    }

    private IndexSettingError randomIndexSettingError() {
        return new IndexSettingError(randomAlphanumericOfLength(20), randomAlphanumericOfLength(20));
    }

    @Override
    protected UpdateDataStreamSettingsAction.Response mutateInstance(UpdateDataStreamSettingsAction.Response instance) throws IOException {
        List<UpdateDataStreamSettingsAction.DataStreamSettingsResponse> responseList = instance.getDataStreamSettingsResponses();
        List<UpdateDataStreamSettingsAction.DataStreamSettingsResponse> mutatedResponseList = new ArrayList<>(responseList);
        switch (between(0, 1)) {
            case 0 -> {
                if (responseList.isEmpty()) {
                    mutatedResponseList.add(randomDataStreamSettingsResponse());
                } else {
                    mutatedResponseList.remove(randomInt(responseList.size() - 1));
                }
            }
            case 1 -> {
                mutatedResponseList.add(randomDataStreamSettingsResponse());
            }
            default -> throw new AssertionError("Should not be here");
        }
        return new UpdateDataStreamSettingsAction.Response(mutatedResponseList);
    }
}
