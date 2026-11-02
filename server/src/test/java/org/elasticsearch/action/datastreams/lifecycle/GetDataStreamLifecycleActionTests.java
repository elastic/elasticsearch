/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams.lifecycle;

import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class GetDataStreamLifecycleActionTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testDefaultLifecycleResponseToXContent() throws Exception {
        boolean isInternalDataStream = randomBoolean();
        GetDataStreamLifecycleAction.Response.DataStreamLifecycle dataStreamLifecycle = createDataStreamLifecycle(
            DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE,
            isInternalDataStream
        );
        GetDataStreamLifecycleAction.Response response = new GetDataStreamLifecycleAction.Response(List.of(dataStreamLifecycle));
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true);
            response.toXContentChunked(ToXContent.EMPTY_PARAMS).forEachRemaining(xcontent -> {
                try {
                    xcontent.toXContent(builder, EMPTY_PARAMS);
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    fail(e.getMessage());
                }
            });
            Map<String, Object> resultMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            assertThat(resultMap.get("global_retention"), equalTo(Map.of()));
            assertThat(resultMap.containsKey("data_streams"), equalTo(true));
            List<Map<String, Object>> dataStreams = (List<Map<String, Object>>) resultMap.get("data_streams");
            Map<String, Object> firstDataStream = dataStreams.get(0);
            assertThat(firstDataStream.containsKey("lifecycle"), equalTo(true));
            Map<String, Object> lifecycleResult = (Map<String, Object>) firstDataStream.get("lifecycle");
            assertThat(lifecycleResult.get("enabled"), equalTo(true));
            assertThat(lifecycleResult.get("data_retention"), nullValue());
            assertThat(lifecycleResult.get("effective_retention"), nullValue());
            assertThat(lifecycleResult.get("retention_determined_by"), nullValue());
        }
    }

    @SuppressWarnings("unchecked")
    public void testGlobalRetentionToXContent() {
        TimeValue globalDefaultRetention = TimeValue.timeValueDays(10);
        TimeValue globalMaxRetention = TimeValue.timeValueDays(50);
        DataStreamGlobalRetention globalRetention = new DataStreamGlobalRetention(globalDefaultRetention, globalMaxRetention);
        GetDataStreamLifecycleAction.Response response = new GetDataStreamLifecycleAction.Response(List.of(), null, globalRetention);
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true);
            response.toXContentChunked(ToXContent.EMPTY_PARAMS).forEachRemaining(xcontent -> {
                try {
                    xcontent.toXContent(builder, EMPTY_PARAMS);
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    fail(e.getMessage());
                }
            });
            Map<String, Object> resultMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            assertThat(resultMap.containsKey("global_retention"), equalTo(true));
            Map<String, String> globalRetentionMap = (Map<String, String>) resultMap.get("global_retention");
            assertThat(globalRetentionMap.get("max_retention"), equalTo(globalMaxRetention.getStringRep()));
            assertThat(globalRetentionMap.get("default_retention"), equalTo(globalDefaultRetention.getStringRep()));
            assertThat(resultMap.containsKey("data_streams"), equalTo(true));
        } catch (Exception e) {
            fail(e);
        }
    }

    @SuppressWarnings("unchecked")
    public void testDataStreamLifecycleToXContent() throws Exception {
        TimeValue configuredRetention = TimeValue.timeValueDays(100);
        TimeValue globalDefaultRetention = TimeValue.timeValueDays(10);
        TimeValue globalMaxRetention = TimeValue.timeValueDays(50);
        DataStreamGlobalRetention globalRetention = new DataStreamGlobalRetention(globalDefaultRetention, globalMaxRetention);
        DataStreamLifecycle lifecycle = DataStreamLifecycle.dataLifecycleBuilder().enabled(true).dataRetention(configuredRetention).build();
        {
            boolean isInternalDataStream = true;
            GetDataStreamLifecycleAction.Response.DataStreamLifecycle explainIndexDataStreamLifecycle = createDataStreamLifecycle(
                lifecycle,
                isInternalDataStream
            );
            Map<String, Object> resultMap = getXContentMap(explainIndexDataStreamLifecycle, globalRetention);
            Map<String, Object> lifecycleResult = (Map<String, Object>) resultMap.get("lifecycle");
            assertThat(lifecycleResult.get("data_retention"), equalTo(configuredRetention.getStringRep()));
            assertThat(lifecycleResult.get("effective_retention"), equalTo(configuredRetention.getStringRep()));
            assertThat(lifecycleResult.get("retention_determined_by"), equalTo("data_stream_configuration"));
        }
        {
            boolean isInternalDataStream = false;
            GetDataStreamLifecycleAction.Response.DataStreamLifecycle explainIndexDataStreamLifecycle = createDataStreamLifecycle(
                lifecycle,
                isInternalDataStream
            );
            Map<String, Object> resultMap = getXContentMap(explainIndexDataStreamLifecycle, globalRetention);
            Map<String, Object> lifecycleResult = (Map<String, Object>) resultMap.get("lifecycle");
            assertThat(lifecycleResult.get("data_retention"), equalTo(configuredRetention.getStringRep()));
            assertThat(lifecycleResult.get("effective_retention"), equalTo(globalMaxRetention.getStringRep()));
            assertThat(lifecycleResult.get("retention_determined_by"), equalTo("max_global_retention"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testLifecycleEnabledByDefaultToXContent() throws Exception {
        DataStreamGlobalRetention globalRetention = randomBoolean()
            ? null
            : new DataStreamGlobalRetention(TimeValue.timeValueDays(10), TimeValue.timeValueDays(50));
        {
            // a data stream managed by the default lifecycle reports it, but has no configured lifecycle to display, so neither is the
            // global retention displayed as effective retention
            String dataStreamName = randomAlphaOfLength(50);
            Map<String, Object> resultMap = getXContentMap(
                new GetDataStreamLifecycleAction.Response.DataStreamLifecycle(dataStreamName, null, randomBoolean(), true),
                globalRetention
            );
            assertThat(resultMap, equalTo(Map.of("name", dataStreamName, "minimum_lifecycle_enabled", true)));
        }
        {
            // a data stream without any lifecycle displays only its name
            String dataStreamName = randomAlphaOfLength(50);
            Map<String, Object> resultMap = getXContentMap(
                new GetDataStreamLifecycleAction.Response.DataStreamLifecycle(dataStreamName, null, randomBoolean(), false),
                globalRetention
            );
            assertThat(resultMap, equalTo(Map.of("name", dataStreamName)));
        }
        {
            // a data stream with a configured lifecycle does not report the lifecycle as enabled by default
            Map<String, Object> resultMap = getXContentMap(
                createDataStreamLifecycle(DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE, randomBoolean()),
                globalRetention
            );
            assertThat(resultMap.containsKey("minimum_lifecycle_enabled"), equalTo(false));
            assertThat(((Map<String, Object>) resultMap.get("lifecycle")).get("enabled"), equalTo(true));
        }
    }

    public void testLifecycleEnabledByDefaultEquality() {
        GetDataStreamLifecycleAction.Response.DataStreamLifecycle enabledByDefault =
            new GetDataStreamLifecycleAction.Response.DataStreamLifecycle("my-data-stream", null, false, true);
        GetDataStreamLifecycleAction.Response.DataStreamLifecycle withoutLifecycle =
            new GetDataStreamLifecycleAction.Response.DataStreamLifecycle("my-data-stream", null, false, false);
        assertThat(enabledByDefault.equals(withoutLifecycle), equalTo(false));
        assertThat(
            new GetDataStreamLifecycleAction.Response(List.of(enabledByDefault)),
            not(equalTo(new GetDataStreamLifecycleAction.Response(List.of(withoutLifecycle))))
        );
    }

    private GetDataStreamLifecycleAction.Response.DataStreamLifecycle createDataStreamLifecycle(
        DataStreamLifecycle lifecycle,
        boolean isInternalDataStream
    ) {
        return new GetDataStreamLifecycleAction.Response.DataStreamLifecycle(
            randomAlphaOfLength(50),
            lifecycle,
            isInternalDataStream,
            false
        );
    }

    /*
     * Calls toXContent on the given dataStreamLifecycle, and converts the response to a Map
     */
    private Map<String, Object> getXContentMap(
        GetDataStreamLifecycleAction.Response.DataStreamLifecycle dataStreamLifecycle,
        DataStreamGlobalRetention globalRetention
    ) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            RolloverConfiguration rolloverConfiguration = null;
            dataStreamLifecycle.toXContent(builder, ToXContent.EMPTY_PARAMS, rolloverConfiguration, globalRetention);
            String serialized = Strings.toString(builder);
            return XContentHelper.convertToMap(XContentType.JSON.xContent(), serialized, randomBoolean());
        }
    }
}
