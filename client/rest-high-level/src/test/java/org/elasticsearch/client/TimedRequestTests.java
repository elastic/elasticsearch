/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

public class TimedRequestTests extends ESTestCase {

    public void testDefaults() {
        TimedRequest timedRequest = new TimedRequest(){};
        assertEquals(timedRequest.timeout(), TimedRequest.DEFAULT_ACK_TIMEOUT);
        assertEquals(timedRequest.masterNodeTimeout(), TimedRequest.DEFAULT_MASTER_NODE_TIMEOUT);
    }

    public void testNonDefaults() {
        TimedRequest timedRequest = new TimedRequest(){};
        TimeValue timeout = TimeValue.timeValueSeconds(randomIntBetween(0, 1000));
        TimeValue masterTimeout = TimeValue.timeValueSeconds(randomIntBetween(0,1000));
        timedRequest.setTimeout(timeout);
        timedRequest.setMasterTimeout(masterTimeout);
        assertEquals(timedRequest.timeout(), timeout);
        assertEquals(timedRequest.masterNodeTimeout(), masterTimeout);
    }
}
