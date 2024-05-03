/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination.stateless;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.coordination.AbstractCoordinatorTestCase;

public class DisruptibleHeartbeatStore implements HeartbeatStore {

    private static final Logger logger = LogManager.getLogger(DisruptibleHeartbeatStore.class);

    private final HeartbeatStore delegate;
    private final AbstractCoordinatorTestCase.DisruptibleRegisterConnection disruptibleRegisterConnection;

    public DisruptibleHeartbeatStore(
        HeartbeatStore delegate,
        AbstractCoordinatorTestCase.DisruptibleRegisterConnection disruptibleRegisterConnection
    ) {
        this.delegate = delegate;
        this.disruptibleRegisterConnection = disruptibleRegisterConnection;
    }

    @Override
    public final void writeHeartbeat(Heartbeat newHeartbeat, ActionListener<Void> listener) {
        disruptibleRegisterConnection.runDisrupted(listener, l -> delegate.writeHeartbeat(newHeartbeat, l));
    }

    @Override
    public final void readLatestHeartbeat(ActionListener<Heartbeat> listener) {
        // only used when triggering a new election, so can just drop requests if disrupted
        disruptibleRegisterConnection.runDisruptedOrDrop(listener, delegate::readLatestHeartbeat);
    }
}
