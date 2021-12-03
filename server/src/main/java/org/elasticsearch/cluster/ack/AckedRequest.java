/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.ack;

import org.elasticsearch.core.TimeValue;

/**
 * Identifies a cluster state update request with acknowledgement support
 */
public interface AckedRequest {

    /**
     * Returns the acknowledgement timeout
     */
    TimeValue ackTimeout();

    /**
     * Returns the timeout for the request to be completed on the master node
     */
    TimeValue masterNodeTimeout();
}
