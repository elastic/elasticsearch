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
import org.elasticsearch.common.settings.Settings;
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

public class GetDataStreamSettingsActionTests extends ESTestCase {

    public void testResponseToXContentEmpty() throws IOException {
        List<GetDataStreamSettingsAction.DataStreamSettingsResponse> responseList = new ArrayList<>();
        GetDataStreamSettingsAction.Response response = new GetDataStreamSettingsAction.Response(responseList);
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
        Map<String, String> dataStream1Settings = Map.of("setting1", "value1", "setting2", "value2");
        Map<String, String> dataStream1EffectiveSettings = Map.of("setting1", "value1", "setting2", "value2", "setting3", "value3");
        Map<String, String> dataStream2Settings = Map.of("setting4", "value4", "setting5", "value5");
        Map<String, String> dataStream2EffectiveSettings = Map.of("setting4", "value4", "setting5", "value5", "settings6", "value6");
        GetDataStreamSettingsAction.DataStreamSettingsResponse dataStreamSettingsResponse1 =
            new GetDataStreamSettingsAction.DataStreamSettingsResponse(
                "dataStream1",
                Settings.builder().loadFromMap(dataStream1Settings).build(),
                Settings.builder().loadFromMap(dataStream1EffectiveSettings).build()
            );
        GetDataStreamSettingsAction.DataStreamSettingsResponse dataStreamSettingsResponse2 =
            new GetDataStreamSettingsAction.DataStreamSettingsResponse(
                "dataStream2",
                Settings.builder().loadFromMap(dataStream2Settings).build(),
                Settings.builder().loadFromMap(dataStream2EffectiveSettings).build()
            );
        List<GetDataStreamSettingsAction.DataStreamSettingsResponse> responseList = List.of(
            dataStreamSettingsResponse1,
            dataStreamSettingsResponse2
        );
        GetDataStreamSettingsAction.Response response = new GetDataStreamSettingsAction.Response(responseList);
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
                                "settings",
                                dataStream1Settings,
                                "effective_settings",
                                dataStream1EffectiveSettings
                            ),
                            Map.of(
                                "name",
                                "dataStream2",
                                "settings",
                                dataStream2Settings,
                                "effective_settings",
                                dataStream2EffectiveSettings
                            )
                        )
                    )
                )
            );
        }
    }
}
