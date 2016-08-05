/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.watch;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.watcher.actions.ActionStatus.AckStatus.State;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.joda.time.DateTime.now;

public class WatchStatusTests extends ESTestCase {

    public void testThatWatchStatusDirtyOnConditionCheck() throws Exception {
        // no actions, met condition
        WatchStatus status = new WatchStatus(now(), new HashMap<>());
        status.onCheck(true, now());
        assertThat(status.dirty(), is(true));

        // no actions, unmet condition
        status = new WatchStatus(now(), new HashMap<>());
        status.onCheck(false, now());
        assertThat(status.dirty(), is(true));

        // actions, no action with reset ack status, unmet condition
        Map<String, ActionStatus > actions = new HashMap<>();
        actions.put(randomAsciiOfLength(10), new ActionStatus(now()));
        status = new WatchStatus(now(), actions);
        status.onCheck(false, now());
        assertThat(status.dirty(), is(true));

        // actions, one action with state other than AWAITS_SUCCESSFUL_EXECUTION, unmet condition
        actions.clear();
        ActionStatus.AckStatus ackStatus = new ActionStatus.AckStatus(now(), randomFrom(State.ACKED, State.ACKABLE));
        actions.put(randomAsciiOfLength(10), new ActionStatus(ackStatus, null, null, null));
        actions.put(randomAsciiOfLength(11), new ActionStatus(now()));
        status = new WatchStatus(now(), actions);
        status.onCheck(false, now());
        assertThat(status.dirty(), is(true));

        status.resetDirty();
        assertThat(status.dirty(), is(false));
    }
}