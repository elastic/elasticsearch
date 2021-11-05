/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client;

import org.elasticsearch.core.TimeValue;

import static org.elasticsearch.core.TimeValue.timeValueSeconds;

/**
 * A base request for any requests that supply timeouts.
 *
 * Please note, any requests that use a ackTimeout should set timeout as they
 * represent the same backing field on the server.
 */
public abstract class TimedRequest implements Validatable {

    public static final TimeValue DEFAULT_ACK_TIMEOUT = timeValueSeconds(30);
    public static final TimeValue DEFAULT_MASTER_NODE_TIMEOUT = TimeValue.timeValueSeconds(30);

    private TimeValue timeout = DEFAULT_ACK_TIMEOUT;
    private TimeValue masterTimeout = DEFAULT_MASTER_NODE_TIMEOUT;

    /**
     * Sets the timeout to wait for the all the nodes to acknowledge
     * @param timeout timeout as a {@link TimeValue}
     */
    public void setTimeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    /**
     * Sets the timeout to connect to the master node
     * @param masterTimeout timeout as a {@link TimeValue}
     */
    public void setMasterTimeout(TimeValue masterTimeout) {
        this.masterTimeout = masterTimeout;
    }

    /**
     * Returns the request timeout
     */
    public TimeValue timeout() {
        return timeout;
    }

    /**
     * Returns the timeout for the request to be completed on the master node
     */
    public TimeValue masterNodeTimeout() {
        return masterTimeout;
    }
}
