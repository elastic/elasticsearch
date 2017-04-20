/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.watch;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.watcher.actions.ActionStatus.AckStatus.State;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingAction;

import java.util.HashMap;

import static org.hamcrest.Matchers.is;
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
}