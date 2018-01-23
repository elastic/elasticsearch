/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.watch;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus.AckStatus.State;
import org.elasticsearch.xpack.core.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingAction;
import org.hamcrest.Matcher;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.joda.time.DateTime.now;

public class WatchStatusTests extends ESTestCase {

    public void testAckStatusIsResetOnUnmetCondition() {
        HashMap<String, ActionStatus> myMap = new HashMap<>();
        ActionStatus actionStatus = new ActionStatus(now());
        myMap.put("foo", actionStatus);

        actionStatus.update(now(), new LoggingAction.Result.Success("foo"));
        actionStatus.onAck(now());
        assertThat(actionStatus.ackStatus().state(), is(State.ACKED));

        WatchStatus status = new WatchStatus(now(), myMap);
        status.onCheck(false, now());

        assertThat(status.actionStatus("foo").ackStatus().state(), is(State.AWAITS_SUCCESSFUL_EXECUTION));
    }

    public void testExecutionStateIsOnlySerializedInNewVersions() throws IOException {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        WatchStatus watchStatus = new WatchStatus(now, Collections.emptyMap());
        ExecutionState executionState = randomFrom(ExecutionState.values());
        watchStatus.setExecutionState(executionState);

        assertExecutionState(watchStatus, Version.CURRENT, Version.CURRENT, is(executionState));
        assertExecutionState(watchStatus, null, null, is(executionState));
        assertExecutionState(watchStatus, Version.CURRENT, null, is(executionState));
        assertExecutionState(watchStatus, null, Version.CURRENT, is(executionState));

        // write new, read old
        assertExecutionState(watchStatus,
                VersionUtils.randomVersionBetween(random(), Version.V_6_1_0, Version.CURRENT),
                VersionUtils.randomVersionBetween(random(), Version.V_5_0_0, Version.V_6_0_0_rc2),
                is(nullValue()));

        // write old, read old
        assertExecutionState(watchStatus,
                VersionUtils.randomVersionBetween(random(), Version.V_5_0_0, Version.V_6_0_0_rc2),
                VersionUtils.randomVersionBetween(random(), Version.V_5_0_0, Version.V_6_0_0_rc2),
                is(nullValue()));

        // write new, read new
        assertExecutionState(watchStatus,
                VersionUtils.randomVersionBetween(random(), Version.V_6_1_0, Version.CURRENT),
                VersionUtils.randomVersionBetween(random(), Version.V_6_1_0, Version.CURRENT),
                is(executionState));
    }

    private void assertExecutionState(WatchStatus watchStatus, Version writeVersion, Version readVersion, Matcher matcher) throws
            IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        if (writeVersion != null) {
            out.setVersion(writeVersion);
        }
        watchStatus.writeTo(out);

        StreamInput in = StreamInput.wrap(BytesReference.toBytes(out.bytes()));
        if (readVersion != null) {
            in.setVersion(readVersion);
        }

        WatchStatus status = WatchStatus.read(in);
        assertThat(status.getExecutionState(), matcher);
    }

    public void testHeadersToXContent() throws Exception {
        WatchStatus status = new WatchStatus(now(), Collections.emptyMap());
        String key = randomAlphaOfLength(10);
        String value = randomAlphaOfLength(10);
        Map<String, String> headers = Collections.singletonMap(key, value);
        status.setHeaders(headers);

        // by default headers are hidden
        try (XContentBuilder builder = jsonBuilder()) {
            status.toXContent(builder, ToXContent.EMPTY_PARAMS);
            try (XContentParser parser = createParser(builder)) {
                Map<String, Object> fields = parser.map();
                assertThat(fields, not(hasKey(WatchStatus.Field.HEADERS.getPreferredName())));
            }
        }

        // but they are required when storing a watch
        try (XContentBuilder builder = jsonBuilder()) {
            status.toXContent(builder, WatcherParams.builder().hideHeaders(false).build());
            try (XContentParser parser = createParser(builder)) {
                parser.nextToken();
                Map<String, Object> fields = parser.map();
                assertThat(fields, hasKey(WatchStatus.Field.HEADERS.getPreferredName()));
                assertThat(fields.get(WatchStatus.Field.HEADERS.getPreferredName()), instanceOf(Map.class));
                Map<String, Object> extractedHeaders = (Map<String, Object>) fields.get(WatchStatus.Field.HEADERS.getPreferredName());
                assertThat(extractedHeaders, is(headers));
            }
        }
    }

    public void testHeadersSerialization() throws IOException {
        WatchStatus status = new WatchStatus(now(), Collections.emptyMap());
        String key = randomAlphaOfLength(10);
        String value = randomAlphaOfLength(10);
        Map<String, String> headers = Collections.singletonMap(key, value);
        status.setHeaders(headers);

        // current version
        {
            BytesStreamOutput out = new BytesStreamOutput();
            status.writeTo(out);
            BytesReference bytesReference = out.bytes();
            WatchStatus readStatus = WatchStatus.read(bytesReference.streamInput());
            assertThat(readStatus, is(status));
        }
        // earlier version
        {
            BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(Version.V_6_0_0);
            status.writeTo(out);
            BytesReference bytesReference = out.bytes();
            StreamInput in = bytesReference.streamInput();
            in.setVersion(Version.V_6_0_0);
            WatchStatus readStatus = WatchStatus.read(in);
            assertThat(readStatus, is(not(status)));
            assertThat(readStatus.getHeaders(), is(Collections.emptyMap()));
        }
    }
}