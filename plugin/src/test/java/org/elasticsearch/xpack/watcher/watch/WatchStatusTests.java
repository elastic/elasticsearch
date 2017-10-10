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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.watcher.actions.ActionStatus.AckStatus.State;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingAction;
import org.elasticsearch.xpack.watcher.execution.ExecutionState;
import org.hamcrest.Matcher;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;

import static org.hamcrest.Matchers.is;
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
}