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
package org.elasticsearch.protocol.xpack.watcher;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.xpack.watcher.status.ActionAckStatus;
import org.elasticsearch.protocol.xpack.watcher.status.ActionStatus;
import org.elasticsearch.protocol.xpack.watcher.status.ActionStatusExecution;
import org.elasticsearch.protocol.xpack.watcher.status.ActionStatusThrottle;
import org.elasticsearch.protocol.xpack.watcher.status.ExecutionState;
import org.elasticsearch.protocol.xpack.watcher.status.WatchStatus;
import org.elasticsearch.protocol.xpack.watcher.status.WatchStatusParams;
import org.elasticsearch.protocol.xpack.watcher.status.WatchStatusState;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ActivateWatchResponseTests extends AbstractXContentTestCase<ActivateWatchResponse> {

    @Override
    protected ActivateWatchResponse createTestInstance() {
        long version = randomNonNegativeLong();
        WatchStatusState state = generateStatusState();
        ExecutionState executionState = randomFrom(ExecutionState.values());
        DateTime lastChecked = randomDateTime();
        DateTime lastMetCondition = randomDateTime();
        int actionsCount = randomIntBetween(0,10);
        Map<String, ActionStatus> actions = new HashMap<>();
        for (int i=0; i<actionsCount; i++) {
            actions.put(randomAlphaOfLength(10), generateActionStatus());
        }
        int headersCount = randomIntBetween(0,10);
        Map<String, String> headers = new HashMap<>();
        for (int i=0; i<headersCount; i++) {
            headers.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
        }

        WatchStatus status = new WatchStatus(
                version,
                state,
                nullable(executionState),
                nullable(lastChecked),
                nullable(lastMetCondition),
                actions,
                headers
        );
        return new ActivateWatchResponse(status);
    }

    private static DateTime randomDateTime() {
        DateTime futureDate = DateTime.parse("2040-01-01T00:00");
        return new DateTime(randomLongBetween(0, futureDate.getMillis()), DateTimeZone.UTC);
    }

    private static <T> T nullable(T t) {
        return randomBoolean() ? null : t;
    }

    private WatchStatusState generateStatusState() {
        return new WatchStatusState(randomBoolean(), randomDateTime());
    }

    private ActionStatus generateActionStatus() {
        ActionAckStatus ackStatus = new ActionAckStatus(randomDateTime(), randomFrom(ActionAckStatus.State.values()));
        ActionStatusExecution lastExecution = randomFrom(
                ActionStatusExecution.successful(randomDateTime()),
                ActionStatusExecution.failure(randomDateTime(), randomAlphaOfLength(10))
        );
        ActionStatusExecution lastSuccessfulExecution = ActionStatusExecution.successful(randomDateTime());
        ActionStatusThrottle lastThrottle = new ActionStatusThrottle(randomDateTime(), randomAlphaOfLength(10));
        return new ActionStatus(
                ackStatus,
                nullable(lastExecution),
                nullable(lastSuccessfulExecution),
                nullable(lastThrottle)
        );
    }

    @Override
    protected ActivateWatchResponse doParseInstance(XContentParser parser) throws IOException {
        return ActivateWatchResponse.fromXContent(randomAlphaOfLength(10), parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return WatchStatusParams.builder().includeState(true).hideHeaders(false).build();
    }

}
