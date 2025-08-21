/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

public class CcrStatsActionTests extends AbstractWireSerializingTestCase<CcrStatsAction.Request> {

    @Override
    protected Writeable.Reader<CcrStatsAction.Request> instanceReader() {
        return CcrStatsAction.Request::new;
    }

    @Override
    protected CcrStatsAction.Request createTestInstance() {
        var request = new CcrStatsAction.Request(TEST_REQUEST_TIMEOUT);
        request.setTimeout(TimeValue.timeValueSeconds(randomFrom(1, 5, 10, 15)));
        request.masterNodeTimeout(TimeValue.timeValueSeconds(randomFrom(1, 5, 10, 15)));
        return request;
    }

    @Override
    protected CcrStatsAction.Request mutateInstance(CcrStatsAction.Request instance) throws IOException {
        return switch (randomInt(1)) {
            case 0 -> {
                var mutatedInstance = new CcrStatsAction.Request(TEST_REQUEST_TIMEOUT);
                mutatedInstance.setTimeout(instance.getTimeout());
                mutatedInstance.masterNodeTimeout(TimeValue.timeValueSeconds(randomFrom(20, 25, 30)));
                yield mutatedInstance;
            }
            case 1 -> {
                var mutatedInstance = new CcrStatsAction.Request(TEST_REQUEST_TIMEOUT);
                mutatedInstance.setTimeout(TimeValue.timeValueSeconds(randomFrom(20, 25, 30)));
                mutatedInstance.masterNodeTimeout(instance.masterNodeTimeout());
                yield mutatedInstance;
            }
            default -> throw new RuntimeException("Cannot happen");
        };
    }

    public void testSerializationBwc() throws IOException {
        // In previous version `timeout` is not set
        var request = new CcrStatsAction.Request(TEST_REQUEST_TIMEOUT);
        if (randomBoolean()) {
            request.masterNodeTimeout(TimeValue.timeValueSeconds(randomFrom(20, 25, 30)));
        }
        assertSerialization(request, TransportVersionUtils.getPreviousVersion(TransportVersions.V_8_14_0));
        assertSerialization(request, TransportVersions.MINIMUM_CCS_VERSION);
    }
}
