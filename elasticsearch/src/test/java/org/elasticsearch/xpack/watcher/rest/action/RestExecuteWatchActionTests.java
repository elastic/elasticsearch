/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.watcher.client.WatcherClient;
import org.elasticsearch.xpack.watcher.transport.actions.execute.ExecuteWatchRequestBuilder;

import java.util.Arrays;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestExecuteWatchActionTests extends ESTestCase {

    private RestController restController = mock(RestController.class);
    private Client client = mock(Client.class);
    private WatcherClient watcherClient = mock(WatcherClient.class);

    public void testThatFlagsCanBeSpecifiedViaParameters() throws Exception {
        String randomId = randomAsciiOfLength(10);
        for (String recordExecution : Arrays.asList("true", "false", null)) {
            for (String ignoreCondition : Arrays.asList("true", "false", null)) {
                for (String debugCondition : Arrays.asList("true", "false", null)) {
                    ExecuteWatchRequestBuilder builder = new ExecuteWatchRequestBuilder(client);
                    when(watcherClient.prepareExecuteWatch()).thenReturn(builder);

                    RestExecuteWatchAction restExecuteWatchAction = new RestExecuteWatchAction(Settings.EMPTY, restController);
                    restExecuteWatchAction.doPrepareRequest(createFakeRestRequest(randomId, recordExecution, ignoreCondition,
                            debugCondition), watcherClient);

                    assertThat(builder.request().getId(), is(randomId));
                    assertThat(builder.request().isRecordExecution(), is(Boolean.parseBoolean(recordExecution)));
                    assertThat(builder.request().isIgnoreCondition(), is(Boolean.parseBoolean(ignoreCondition)));
                    assertThat(builder.request().isDebug(), is(Boolean.parseBoolean(debugCondition)));
                }
            }
        }
    }

    private FakeRestRequest createFakeRestRequest(String randomId, String recordExecution, String ignoreCondition, String debugCondition) {
        FakeRestRequest restRequest = new FakeRestRequest() {
            @Override
            public boolean hasContent() {
                return true;
            }

            @Override
            public BytesReference content() {
                return new BytesArray("{}");
            }
        };

        restRequest.params().put("id", randomId);
        // make sure we test true/false/no params
        if (recordExecution != null) restRequest.params().put("record_execution", recordExecution);
        if (ignoreCondition != null) restRequest.params().put("ignore_condition", ignoreCondition);
        if (debugCondition != null) restRequest.params().put("debug", debugCondition);

        return restRequest;
    }
}
