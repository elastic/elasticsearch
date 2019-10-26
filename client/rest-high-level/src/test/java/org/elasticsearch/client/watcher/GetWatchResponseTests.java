/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.watcher;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.core.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class GetWatchResponseTests extends AbstractResponseTestCase<GetWatchResponse, org.elasticsearch.client.watcher.GetWatchResponse> {

    @Override
    protected GetWatchResponse createServerTestInstance(XContentType xContentType) {
        String id = randomAlphaOfLength(10);
        if (LuceneTestCase.rarely()) {
            return new GetWatchResponse(id);
        }
        long version = randomLongBetween(0, 10);
        long seqNo = randomNonNegativeLong();
        long primaryTerm = randomLongBetween(1, 2000);
        WatchStatus status = randomWatchStatus();
        BytesReference source = simpleWatch();
        return new GetWatchResponse(id, version, seqNo, primaryTerm, status, new XContentSource(source, XContentType.JSON));
    }

    @Override
    protected org.elasticsearch.client.watcher.GetWatchResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.client.watcher.GetWatchResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(GetWatchResponse serverTestInstance, org.elasticsearch.client.watcher.GetWatchResponse clientInstance) {
        assertThat(clientInstance.getId(), equalTo(serverTestInstance.getId()));
        assertThat(clientInstance.getSeqNo(), equalTo(serverTestInstance.getSeqNo()));
        assertThat(clientInstance.getPrimaryTerm(), equalTo(serverTestInstance.getPrimaryTerm()));
        assertThat(clientInstance.getVersion(), equalTo(serverTestInstance.getVersion()));
        if (serverTestInstance.getStatus() != null) {
            assertThat(convertWatchStatus(clientInstance.getStatus()), equalTo(serverTestInstance.getStatus()));
        } else {
            assertThat(clientInstance.getStatus(), nullValue());
        }
        if (serverTestInstance.getSource() != null) {
            assertThat(clientInstance.getSourceAsMap(), equalTo(serverTestInstance.getSource().getAsMap()));
        } else {
            assertThat(clientInstance.getSource(), nullValue());
        }
    }

    @Override
    protected ToXContent.Params getParams() {
        return new ToXContent.MapParams(Map.of("hide_headers", "false"));
    }

    private static BytesReference simpleWatch() {
        try {
            XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
            builder.startObject()
                .startObject("trigger")
                    .startObject("schedule")
                        .field("interval", "10h")
                    .endObject()
                .endObject()
                .startObject("input")
                    .startObject("none").endObject()
                .endObject()
                .startObject("actions")
                    .startObject("logme")
                        .field("text", "{{ctx.payload}}")
                    .endObject()
                .endObject().endObject();
            return BytesReference.bytes(builder);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static WatchStatus randomWatchStatus() {
        long version = randomLongBetween(-1, Long.MAX_VALUE);
        WatchStatus.State state = new WatchStatus.State(randomBoolean(), DateUtils.nowWithMillisResolution());
        ExecutionState executionState = randomFrom(ExecutionState.values());
        ZonedDateTime lastChecked = LuceneTestCase.rarely() ? null : DateUtils.nowWithMillisResolution();
        ZonedDateTime lastMetCondition = LuceneTestCase.rarely() ? null : DateUtils.nowWithMillisResolution();
        int size = randomIntBetween(0, 5);
        Map<String, ActionStatus> actionMap = new HashMap<>();
        for (int i = 0; i < size; i++) {
            ActionStatus.AckStatus ack = new ActionStatus.AckStatus(
                DateUtils.nowWithMillisResolution(),
                randomFrom(ActionStatus.AckStatus.State.values())
            );
            ActionStatus actionStatus = new ActionStatus(
                ack,
                randomBoolean() ? null : randomExecution(),
                randomBoolean() ? null : randomExecution(),
                randomBoolean() ? null : randomThrottle()
            );
            actionMap.put(randomAlphaOfLength(10), actionStatus);
        }
        Map<String, String> headers = new HashMap<>();
        int headerSize = randomIntBetween(0, 5);
        for (int i = 0; i < headerSize; i++) {
            headers.put(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(1, 10));
        }
        return new WatchStatus(version, state, executionState, lastChecked, lastMetCondition, actionMap, headers);
    }

    private static ActionStatus.Throttle randomThrottle() {
        return new ActionStatus.Throttle(DateUtils.nowWithMillisResolution(), randomAlphaOfLengthBetween(10, 20));
    }

    private static ActionStatus.Execution randomExecution() {
        if (randomBoolean()) {
            return null;
        } else if (randomBoolean()) {
            return ActionStatus.Execution.failure(DateUtils.nowWithMillisResolution(), randomAlphaOfLengthBetween(10, 20));
        } else {
            return ActionStatus.Execution.successful(DateUtils.nowWithMillisResolution());
        }
    }

    private static WatchStatus convertWatchStatus(org.elasticsearch.client.watcher.WatchStatus status) {
        final Map<String, ActionStatus> actions = new HashMap<>();
        for (Map.Entry<String, org.elasticsearch.client.watcher.ActionStatus> entry : status.getActions().entrySet()) {
            actions.put(entry.getKey(), convertActionStatus(entry.getValue()));
        }
        return new WatchStatus(status.version(),
            convertWatchStatusState(status.state()),
            status.getExecutionState() == null ? null : convertWatchStatus(status.getExecutionState()),
            status.lastChecked(), status.lastMetCondition(), actions, status.getHeaders()
        );
    }

    private static ActionStatus convertActionStatus(org.elasticsearch.client.watcher.ActionStatus actionStatus) {
        return new ActionStatus(convertAckStatus(actionStatus.ackStatus()),
            actionStatus.lastExecution() == null ? null : convertActionStatusExecution(actionStatus.lastExecution()),
            actionStatus.lastSuccessfulExecution() == null ? null : convertActionStatusExecution(actionStatus.lastSuccessfulExecution()),
            actionStatus.lastThrottle() == null ? null : convertActionStatusThrottle(actionStatus.lastThrottle())
        );
    }

    private static ActionStatus.AckStatus convertAckStatus(org.elasticsearch.client.watcher.ActionStatus.AckStatus ackStatus) {
        return new ActionStatus.AckStatus(ackStatus.timestamp(), convertAckStatusState(ackStatus.state()));
    }

    private static ActionStatus.AckStatus.State convertAckStatusState(
        org.elasticsearch.client.watcher.ActionStatus.AckStatus.State state) {
        return ActionStatus.AckStatus.State.valueOf(state.name());
    }

    private static WatchStatus.State convertWatchStatusState(org.elasticsearch.client.watcher.WatchStatus.State state) {
        return new WatchStatus.State(state.isActive(), state.getTimestamp());
    }

    private static ExecutionState convertWatchStatus(org.elasticsearch.client.watcher.ExecutionState executionState) {
        return ExecutionState.valueOf(executionState.name());
    }

    private static ActionStatus.Execution convertActionStatusExecution(
        org.elasticsearch.client.watcher.ActionStatus.Execution execution) {
        if (execution.successful()) {
            return ActionStatus.Execution.successful(execution.timestamp());
        } else {
            return ActionStatus.Execution.failure(execution.timestamp(), execution.reason());
        }
    }

    private static ActionStatus.Throttle convertActionStatusThrottle(org.elasticsearch.client.watcher.ActionStatus.Throttle throttle) {
        return new ActionStatus.Throttle(throttle.timestamp(), throttle.reason());
    }
}
