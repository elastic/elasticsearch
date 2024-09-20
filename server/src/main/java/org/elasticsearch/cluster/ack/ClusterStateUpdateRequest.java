/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.ack;

import org.elasticsearch.core.TimeValue;

/**
 * Base class to be used when needing to update the cluster state
 * Contains the basic fields that are always needed
 */
public abstract class ClusterStateUpdateRequest<T extends ClusterStateUpdateRequest<T>> {

    private TimeValue ackTimeout;
    private TimeValue masterNodeTimeout;

    /**
     * Returns the maximum time interval to wait for acknowledgements
     */
    public TimeValue ackTimeout() {
        return ackTimeout;
    }

    /**
     * Sets the acknowledgement timeout
     */
    @SuppressWarnings("unchecked")
    public T ackTimeout(TimeValue ackTimeout) {
        this.ackTimeout = ackTimeout;
        return (T) this;
    }

    /**
     * Returns the maximum time interval to wait for the request to
     * be completed on the master node
     */
    public TimeValue masterNodeTimeout() {
        return masterNodeTimeout;
    }

    /**
     * Sets the master node timeout
     */
    @SuppressWarnings("unchecked")
    public T masterNodeTimeout(TimeValue masterNodeTimeout) {
        this.masterNodeTimeout = masterNodeTimeout;
        return (T) this;
    }
}
