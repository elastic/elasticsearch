/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.datastreams.lifecycle;

import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.PATH_RESTRICTED;
import static org.hamcrest.Matchers.equalTo;

public class GetDataStreamLifecycleActionTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testDataStreamLifecycleToXContent() throws Exception {
        TimeValue configuredRetention = TimeValue.timeValueDays(100);
        TimeValue globalDefaultRetention = TimeValue.timeValueDays(10);
        TimeValue globalMaxRetention = TimeValue.timeValueDays(50);
        DataStreamLifecycle lifecycle = new DataStreamLifecycle(new DataStreamLifecycle.Retention(configuredRetention), null, null);
        {
            boolean isInternalDataStream = true;
            GetDataStreamLifecycleAction.Response.DataStreamLifecycle explainIndexDataStreamLifecycle = createDataStreamLifecycle(
                lifecycle,
                isInternalDataStream
            );
            Map<String, Object> resultMap = getXContentMap(explainIndexDataStreamLifecycle, globalDefaultRetention, globalMaxRetention);
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
            Map<String, Object> resultMap = getXContentMap(explainIndexDataStreamLifecycle, globalDefaultRetention, globalMaxRetention);
            Map<String, Object> lifecycleResult = (Map<String, Object>) resultMap.get("lifecycle");
            assertThat(lifecycleResult.get("data_retention"), equalTo(configuredRetention.getStringRep()));
            assertThat(lifecycleResult.get("effective_retention"), equalTo(globalMaxRetention.getStringRep()));
            assertThat(lifecycleResult.get("retention_determined_by"), equalTo("max_global_retention"));
        }
    }

    private GetDataStreamLifecycleAction.Response.DataStreamLifecycle createDataStreamLifecycle(
        DataStreamLifecycle lifecycle,
        boolean isInternalDataStream
    ) {
        return new GetDataStreamLifecycleAction.Response.DataStreamLifecycle(randomAlphaOfLength(50), lifecycle, isInternalDataStream);
    }

    /*
     * Calls toXContent on the given dataStreamLifecycle, and converts the response to a Map
     */
    private Map<String, Object> getXContentMap(
        GetDataStreamLifecycleAction.Response.DataStreamLifecycle dataStreamLifecycle,
        TimeValue globalDefaultRetention,
        TimeValue globalMaxRetention
    ) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            ToXContent.Params params = new ToXContent.MapParams(Map.of(PATH_RESTRICTED, "serverless"));
            RolloverConfiguration rolloverConfiguration = null;
            DataStreamGlobalRetention globalRetention = new DataStreamGlobalRetention(globalDefaultRetention, globalMaxRetention);
            dataStreamLifecycle.toXContent(builder, params, rolloverConfiguration, globalRetention);
            String serialized = Strings.toString(builder);
            return XContentHelper.convertToMap(XContentType.JSON.xContent(), serialized, randomBoolean());
        }
    }
}
