/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.datastreams;

import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class GetDataStreamActionTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testDataStreamInfoToXContent() throws IOException {
        TimeValue configuredRetention = TimeValue.timeValueDays(100);
        TimeValue globalDefaultRetention = TimeValue.timeValueDays(10);
        TimeValue globalMaxRetention = TimeValue.timeValueDays(50);

        {
            // Since this is a system data stream, we expect the global retention to be ignored
            boolean isSystem = true;
            GetDataStreamAction.Response.DataStreamInfo dataStreamInfo = newDataStreamInfo(isSystem, configuredRetention);
            Map<String, Object> resultMap = getXContentMap(dataStreamInfo, globalDefaultRetention, globalMaxRetention);
            assertThat(resultMap.get("hidden"), equalTo(true));
            assertThat(resultMap.get("system"), equalTo(true));
            Map<String, Object> lifecycleResult = (Map<String, Object>) resultMap.get("lifecycle");
            assertThat(lifecycleResult.get("data_retention"), equalTo(configuredRetention.getStringRep()));
            assertThat(lifecycleResult.get("effective_retention"), equalTo(configuredRetention.getStringRep()));
            assertThat(lifecycleResult.get("retention_determined_by"), equalTo("data_stream_configuration"));
        }
        {
            // Since this is not a system data stream, we expect the global retention to override the configured retention
            boolean isSystem = false;
            GetDataStreamAction.Response.DataStreamInfo dataStreamInfo = newDataStreamInfo(isSystem, configuredRetention);
            Map<String, Object> resultMap = getXContentMap(dataStreamInfo, globalDefaultRetention, globalMaxRetention);
            assertThat(resultMap.get("hidden"), equalTo(false));
            assertThat(resultMap.get("system"), equalTo(false));
            Map<String, Object> lifecycleResult = (Map<String, Object>) resultMap.get("lifecycle");
            assertThat(lifecycleResult.get("data_retention"), equalTo(configuredRetention.getStringRep()));
            assertThat(lifecycleResult.get("effective_retention"), equalTo(globalMaxRetention.getStringRep()));
            assertThat(lifecycleResult.get("retention_determined_by"), equalTo("max_global_retention"));
        }
    }

    /*
     * Calls toXContent on the given dataStreamInfo, and converts the response to a Map
     */
    private Map<String, Object> getXContentMap(
        GetDataStreamAction.Response.DataStreamInfo dataStreamInfo,
        TimeValue globalDefaultRetention,
        TimeValue globalMaxRetention
    ) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            ToXContent.Params params = new ToXContent.MapParams(DataStreamLifecycle.INCLUDE_EFFECTIVE_RETENTION_PARAMS);
            RolloverConfiguration rolloverConfiguration = null;
            DataStreamGlobalRetention globalRetention = new DataStreamGlobalRetention(globalDefaultRetention, globalMaxRetention);
            dataStreamInfo.toXContent(builder, params, rolloverConfiguration, globalRetention);
            String serialized = Strings.toString(builder);
            return XContentHelper.convertToMap(XContentType.JSON.xContent(), serialized, randomBoolean());
        }
    }

    private static GetDataStreamAction.Response.DataStreamInfo newDataStreamInfo(boolean isSystem, TimeValue retention) {
        DataStream dataStream = newDataStreamInstance(isSystem, retention);
        return new GetDataStreamAction.Response.DataStreamInfo(
            dataStream,
            randomFrom(ClusterHealthStatus.values()),
            null,
            null,
            null,
            Map.of(),
            randomBoolean()
        );
    }

    private static DataStream newDataStreamInstance(boolean isSystem, TimeValue retention) {
        List<Index> indices = List.of(new Index(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        DataStreamLifecycle lifecycle = new DataStreamLifecycle(new DataStreamLifecycle.Retention(retention), null, null);
        return DataStream.builder(randomAlphaOfLength(50), indices)
            .setGeneration(randomLongBetween(1, 1000))
            .setMetadata(Map.of())
            .setSystem(isSystem)
            .setHidden(isSystem)
            .setReplicated(randomBoolean())
            .setLifecycle(lifecycle)
            .build();
    }
}
