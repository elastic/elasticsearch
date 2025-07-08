/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.FakeRestRequest.Builder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchRequest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class RestExecuteWatchActionTests extends ESTestCase {

    private NodeClient client = mock(NodeClient.class);

    public void testThatFlagsCanBeSpecifiedViaParameters() throws Exception {
        String randomId = randomAlphaOfLength(10);
        for (String recordExecution : Arrays.asList("true", "false", null)) {
            for (String ignoreCondition : Arrays.asList("true", "false", null)) {
                for (String debugCondition : Arrays.asList("true", "false", null)) {

                    ExecuteWatchRequest request = RestExecuteWatchAction.parseRequest(
                        createFakeRestRequest(randomId, recordExecution, ignoreCondition, debugCondition),
                        client
                    );

                    assertThat(request.getId(), is(randomId));
                    assertThat(request.isRecordExecution(), is(Booleans.parseBoolean(recordExecution, false)));
                    assertThat(request.isIgnoreCondition(), is(Booleans.parseBoolean(ignoreCondition, false)));
                    assertThat(request.isDebug(), is(Booleans.parseBoolean(debugCondition, false)));
                }
            }
        }
    }

    private FakeRestRequest createFakeRestRequest(String randomId, String recordExecution, String ignoreCondition, String debugCondition) {
        FakeRestRequest.Builder builder = new Builder(NamedXContentRegistry.EMPTY);
        builder.withContent(new BytesArray("{}"), XContentType.JSON);
        Map<String, String> params = new HashMap<>();
        params.put("id", randomId);
        // make sure we test true/false/no params
        if (recordExecution != null) params.put("record_execution", recordExecution);
        if (ignoreCondition != null) params.put("ignore_condition", ignoreCondition);
        if (debugCondition != null) params.put("debug", debugCondition);

        builder.withParams(params);
        return builder.build();
    }
}
